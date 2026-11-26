package dev.viren.extapi.outbox

import dev.viren.extapi.config.AsyncProperties
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Clock
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Polls the outbox and hands due rows to the transport. Runs on every instance; the
 * PENDING -> QUEUED compare-and-set makes sure each row is dispatched by exactly one of them.
 *
 * Each row is marked QUEUED (with a lease) *before* it is sent, so a worker can never see a
 * message for a row that is still PENDING. If sending fails the row goes back to PENDING; if
 * the instance dies in between, the lease expires and [OutboxRepository.recoverExpiredLeases]
 * puts it back.
 */
class OutboxRelay(
    private val repository: OutboxRepository,
    private val transport: AsyncTransport,
    private val props: AsyncProperties,
    private val clock: Clock = Clock.systemUTC(),
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private var scheduler: ScheduledExecutorService? = null
    private val ticks = AtomicLong()

    /** One polling round; public so tests and admin endpoints can trigger it. @return rows dispatched. */
    fun pollOnce(): Int {
        val n = ticks.incrementAndGet()
        if (n % 20 == 1L) {
            repository.recoverExpiredLeases().takeIf { it > 0 }?.let { log.warn("Recovered {} outbox rows with expired leases", it) }
        }
        if (n % 7200 == 0L) {
            repository.deleteTerminalBefore(clock.instant().minus(props.retention)).takeIf { it > 0 }?.let { log.info("Purged {} old outbox rows", it) }
        }

        val due = repository.findDue(props.batchSize)
        var dispatched = 0
        val inflight = due.mapNotNull { row ->
            val lease = clock.instant().plus(props.queuedLease)
            if (!repository.transition(row.id, OutboxStatus.PENDING, OutboxStatus.QUEUED, lease)) return@mapNotNull null
            row to transport.dispatch(OutboxEnvelope(row.id, row.kind, row.client, row.dedupeKey ?: row.id))
        }
        for ((row, future) in inflight) {
            try {
                future.get(props.publishTimeout.toMillis(), TimeUnit.MILLISECONDS)
                dispatched++
            } catch (e: Exception) {
                log.warn("Dispatch of outbox {} failed ({}); returning it to PENDING", row.id, e.message)
                repository.transition(row.id, OutboxStatus.QUEUED, OutboxStatus.PENDING, null)
            }
        }
        return dispatched
    }

    override fun start() {
        val s = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "ext-api-outbox-relay").apply { isDaemon = true } }
        val interval = props.relayInterval.toMillis().coerceAtLeast(50)
        s.scheduleWithFixedDelay({
            try {
                // Keep draining while full batches come back, then wait for the next tick.
                while (pollOnce() >= props.batchSize) Unit
            } catch (e: Exception) {
                log.error("Outbox relay round failed", e)
            }
        }, interval, interval, TimeUnit.MILLISECONDS)
        scheduler = s
        log.info("Outbox relay started (interval={}ms, batch={})", interval, props.batchSize)
    }

    override fun stop() {
        scheduler?.let {
            it.shutdown()
            it.awaitTermination(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS)
        }
        scheduler = null
    }

    override fun isRunning() = scheduler != null
    override fun getPhase() = Int.MAX_VALUE - 100 // start late, stop early
}
