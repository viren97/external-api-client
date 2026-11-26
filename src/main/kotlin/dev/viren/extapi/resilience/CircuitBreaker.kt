package dev.viren.extapi.resilience

import dev.viren.extapi.config.CircuitBreakerProperties
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

enum class CircuitState { CLOSED, OPEN, HALF_OPEN, FORCED_OPEN, DISABLED }

fun interface CircuitStateListener {
    fun onTransition(breaker: String, from: CircuitState, to: CircuitState)
}

/**
 * Count-based circuit breaker.
 *
 * - **CLOSED**: calls flow; the last [CircuitBreakerProperties.slidingWindowSize] outcomes are
 *   kept in a ring buffer. Once at least `minimumCalls` are recorded and the failure rate or the
 *   slow-call rate crosses its threshold, the breaker opens.
 * - **OPEN**: calls are rejected immediately for `openDuration` so the upstream can recover and
 *   our threads are not tied up waiting on timeouts.
 * - **HALF_OPEN**: up to `halfOpenPermittedCalls` probe calls go through. When all of them have
 *   finished, the breaker closes if their failure rate is below the threshold, otherwise reopens.
 *
 * Every successful [tryAcquirePermission] must be followed by exactly one of [onSuccess],
 * [onError] or [onIgnored], otherwise a HALF_OPEN probe slot leaks.
 */
class CircuitBreaker(
    val name: String,
    private val config: CircuitBreakerProperties,
    private val nanoClock: () -> Long = System::nanoTime,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val listeners = CopyOnWriteArrayList<CircuitStateListener>()

    private val windowSize = config.slidingWindowSize.coerceAtLeast(1)
    private val failures = BooleanArray(windowSize)
    private val slow = BooleanArray(windowSize)
    private var cursor = 0
    private var recorded = 0
    private var failureCount = 0
    private var slowCount = 0

    private var state = if (config.enabled) CircuitState.CLOSED else CircuitState.DISABLED
    private var openedAtNanos = 0L
    private var halfOpenIssued = 0
    private var halfOpenDone = 0
    private var halfOpenFailures = 0

    fun addListener(listener: CircuitStateListener) = apply { listeners += listener }

    val currentState: CircuitState @Synchronized get() = state

    @Synchronized
    fun metrics(): Snapshot = Snapshot(state, recorded, failureRate(), slowRate())

    data class Snapshot(val state: CircuitState, val bufferedCalls: Int, val failureRate: Double, val slowCallRate: Double)

    /** @return false when the call must be rejected. */
    @Synchronized
    fun tryAcquirePermission(): Boolean = when (state) {
        CircuitState.DISABLED, CircuitState.CLOSED -> true
        CircuitState.FORCED_OPEN -> false
        CircuitState.OPEN ->
            if (nanoClock() - openedAtNanos >= config.openDuration.toNanos()) {
                transition(CircuitState.HALF_OPEN)
                halfOpenIssued = 1
                true
            } else false
        CircuitState.HALF_OPEN ->
            if (halfOpenIssued < config.halfOpenPermittedCalls) { halfOpenIssued++; true } else false
    }

    /** Time left until the breaker lets a probe through; null when it is not open. */
    @Synchronized
    fun remainingOpen(): Duration? = when (state) {
        CircuitState.OPEN -> Duration.ofNanos((config.openDuration.toNanos() - (nanoClock() - openedAtNanos)).coerceAtLeast(0))
        CircuitState.HALF_OPEN -> Duration.ofMillis(100)
        else -> null
    }

    fun onSuccess(duration: Duration) = record(failed = false, duration)
    fun onError(duration: Duration) = record(failed = true, duration)

    /** The permitted call ended without a verdict on upstream health (e.g. a 404). */
    @Synchronized
    fun onIgnored() {
        if (state == CircuitState.HALF_OPEN && halfOpenIssued > halfOpenDone) halfOpenIssued--
    }

    @Synchronized
    fun forceOpen() = transition(CircuitState.FORCED_OPEN)

    @Synchronized
    fun reset() { clearWindow(); transition(if (config.enabled) CircuitState.CLOSED else CircuitState.DISABLED) }

    @Synchronized
    private fun record(failed: Boolean, duration: Duration) {
        val isSlow = duration >= config.slowCallThreshold
        when (state) {
            CircuitState.CLOSED -> {
                push(failed, isSlow)
                if (recorded >= config.minimumCalls && (failureRate() >= config.failureRateThreshold || slowRate() >= config.slowCallRateThreshold)) {
                    log.warn("Circuit '{}' opening: failureRate={}% slowRate={}% over {} calls", name, "%.1f".format(failureRate()), "%.1f".format(slowRate()), recorded)
                    open()
                }
            }
            CircuitState.HALF_OPEN -> {
                halfOpenDone++
                if (failed || isSlow) halfOpenFailures++
                if (halfOpenDone >= config.halfOpenPermittedCalls) {
                    val rate = halfOpenFailures * 100.0 / halfOpenDone
                    if (rate >= config.failureRateThreshold) open() else { clearWindow(); transition(CircuitState.CLOSED) }
                }
            }
            else -> Unit // results of calls that started before the breaker opened are dropped
        }
    }

    private fun open() {
        openedAtNanos = nanoClock()
        transition(CircuitState.OPEN)
    }

    private fun push(failed: Boolean, isSlow: Boolean) {
        if (recorded == windowSize) {
            if (failures[cursor]) failureCount--
            if (slow[cursor]) slowCount--
        } else recorded++
        failures[cursor] = failed
        slow[cursor] = isSlow
        if (failed) failureCount++
        if (isSlow) slowCount++
        cursor = (cursor + 1) % windowSize
    }

    private fun clearWindow() {
        failures.fill(false); slow.fill(false)
        cursor = 0; recorded = 0; failureCount = 0; slowCount = 0
    }

    private fun failureRate() = if (recorded == 0) 0.0 else failureCount * 100.0 / recorded
    private fun slowRate() = if (recorded == 0) 0.0 else slowCount * 100.0 / recorded

    private fun transition(to: CircuitState) {
        val from = state
        if (from == to) return
        state = to
        halfOpenIssued = 0; halfOpenDone = 0; halfOpenFailures = 0
        log.info("Circuit '{}' {} -> {}", name, from, to)
        listeners.forEach { runCatching { it.onTransition(name, from, to) } }
    }
}

/** Creates breakers lazily, one per client or per client+operation. */
class CircuitBreakerRegistry {
    private val breakers = ConcurrentHashMap<String, CircuitBreaker>()
    private val listeners = CopyOnWriteArrayList<CircuitStateListener>()

    fun addListener(listener: CircuitStateListener) {
        listeners += listener
        breakers.values.forEach { it.addListener(listener) }
    }

    fun get(name: String, config: CircuitBreakerProperties): CircuitBreaker =
        breakers.computeIfAbsent(name) { n -> CircuitBreaker(n, config).also { b -> listeners.forEach(b::addListener) } }

    fun find(name: String): CircuitBreaker? = breakers[name]
    fun all(): Map<String, CircuitBreaker> = breakers.toMap()
}
