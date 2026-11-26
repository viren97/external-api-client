package dev.viren.extapi.outbox

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import dev.viren.extapi.config.AsyncProperties
import dev.viren.extapi.error.ExternalApiException
import dev.viren.extapi.error.FailureKind
import dev.viren.extapi.http.ExternalApiClientRegistry
import dev.viren.extapi.observability.ApiMetrics
import dev.viren.extapi.resilience.RetryPolicy
import dev.viren.extapi.webhook.WebhookPublisher
import dev.viren.extapi.webhook.WebhookSigner
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration

/**
 * Executes one outbox row. Safe to call any number of times for the same id: only the call
 * that wins the QUEUED -> PROCESSING transition does any work, so duplicate Kafka deliveries
 * and relay re-dispatches are harmless.
 */
class AsyncCallWorker(
    private val repository: OutboxRepository,
    private val clients: ExternalApiClientRegistry,
    private val mapper: ObjectMapper,
    private val props: AsyncProperties,
    private val signer: WebhookSigner?,
    private val webhooks: () -> WebhookPublisher?,
    private val listeners: List<AsyncResultListener>,
    private val outcomes: OutcomePublisher,
    private val tx: TransactionTemplate?,
    private val metrics: ApiMetrics = ApiMetrics.Noop,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun process(id: String) {
        if (!repository.transition(id, OutboxStatus.QUEUED, OutboxStatus.PROCESSING, clock.instant().plus(props.lease))) {
            log.debug("Outbox {} already taken or finished; skipping duplicate delivery", id)
            return
        }
        val record = repository.findById(id) ?: return
        val attempts = record.attempts + 1
        record.correlationId?.let { MDC.put("correlationId", it) }
        try {
            val request = try {
                mapper.readValue(record.payload, StoredRequest::class.java).request
            } catch (e: Exception) {
                finish(record, OutboxStatus.FAILED, attempts, "SERIALIZATION", "unreadable payload: ${e.message}", null, null)
                return
            }
            val client = clients.clientOrNull(record.clientName) ?: run {
                finish(record, OutboxStatus.FAILED, attempts, "UNKNOWN_CLIENT", "no client '${record.clientName}'", null, null)
                return
            }
            val toSend = if (record.kind == OutboxKind.WEBHOOK && signer != null) signer.sign(request, record.id) else request

            try {
                val response = client.execute<JsonNode>(toSend, JsonNode::class.java)
                finish(record, OutboxStatus.SUCCEEDED, attempts, null, null, response.status, response.rawBody)
            } catch (e: ExternalApiException) {
                handleFailure(record, attempts, e)
            }
        } catch (e: Exception) {
            // Infrastructure problem (DB down mid-way...). The lease expiry will requeue the row.
            log.error("Outbox {} processing crashed; it will be retried after the lease expires", id, e)
        } finally {
            MDC.remove("correlationId")
        }
    }

    private fun handleFailure(record: OutboxRecord, attempts: Int, e: ExternalApiException) {
        // Asynchronous work has time on its side: local protections (open circuit, rate limit,
        // bulkhead, deadline) mean "not now", not "never".
        val transient = e.retryable || e.kind in DEFERRABLE
        val error = e.message?.take(2000)
        if (transient && attempts < record.maxAttempts) {
            val backoff = RetryPolicy.exponential(attempts, props.initialBackoff, props.maxBackoff)
            val delay = maxOf(backoff, e.retryAfter ?: Duration.ZERO)
            repository.reschedule(record.id, attempts, clock.instant().plus(delay), e.kind.name, error, e.status, truncate(e.responseBody))
            metrics.asyncOutcome(record.clientName, "retry_scheduled")
            log.info("Outbox {} ({}/{}) attempt {}/{} failed with {}; next try in {}s", record.id, record.clientName, record.operation, attempts, record.maxAttempts, e.kind, delay.seconds)
        } else {
            val status = if (transient) OutboxStatus.DEAD else OutboxStatus.FAILED
            finish(record, status, attempts, e.kind.name, error, e.status, e.responseBody)
        }
    }

    private fun finish(record: OutboxRecord, status: OutboxStatus, attempts: Int, errorKind: String?, error: String?, responseStatus: Int?, body: String?) {
        val stored = truncate(body)
        val result = AsyncCallResult(
            record.id, record.kind, record.clientName, record.operation, status, attempts, responseStatus, stored,
            errorKind, error, record.dedupeKey, record.correlationId, clock.instant(),
        )
        val work = {
            if (repository.complete(record.id, status, attempts, errorKind, error, responseStatus, stored)) {
                // Same transaction as the status update: the callback is scheduled exactly once.
                if (record.callbackUrl != null && record.kind == OutboxKind.API_CALL) {
                    webhooks()?.publish(record.callbackUrl, record.callbackEvent ?: "async_call.${status.name.lowercase()}", result, dedupeKey = "cb-${record.id}")
                }
                true
            } else false
        }
        val done = tx?.execute { work() } ?: work()
        if (done != true) return

        metrics.asyncOutcome(record.clientName, status.name.lowercase())
        if (status == OutboxStatus.SUCCEEDED) log.info("Outbox {} ({}/{}) succeeded after {} attempt(s)", record.id, record.clientName, record.operation, attempts)
        else log.warn("Outbox {} ({}/{}) ended {} after {} attempt(s): {} {}", record.id, record.clientName, record.operation, status, attempts, errorKind, error)

        runCatching { outcomes.publish(result) }.onFailure { log.warn("Outcome publish failed for {}", record.id, it) }
        listeners.forEach { l -> runCatching { l.onResult(result) }.onFailure { log.warn("AsyncResultListener failed for {}", record.id, it) } }
    }

    private fun truncate(s: String?) = s?.let { if (it.length > props.maxStoredResponseChars) it.take(props.maxStoredResponseChars) else it }

    companion object {
        val DEFERRABLE = setOf(
            FailureKind.CIRCUIT_OPEN, FailureKind.RATE_LIMITED, FailureKind.BULKHEAD_FULL,
            FailureKind.DEADLINE_EXCEEDED, FailureKind.POOL_TIMEOUT, FailureKind.UPSTREAM_RATE_LIMITED,
        )
    }
}
