package dev.viren.extapi.ratelimit

import dev.viren.extapi.config.RateLimitProperties
import dev.viren.extapi.error.DeadlineExceededException
import dev.viren.extapi.error.RateLimitExceededException
import java.time.Duration

/**
 * All outbound throttling for one client: the client-wide limit, per-operation limits and the
 * adaptive upstream cooldown. Callers wait up to `maxWait` (and never past the call deadline)
 * for a permit, otherwise they get a [RateLimitExceededException] carrying a retry-after hint.
 */
class RateLimitGate(
    private val client: String,
    private val props: RateLimitProperties,
    factory: RateLimiterFactory?,
    private val cooldown: UpstreamCooldown,
    private val sleeper: (Duration) -> Unit = { Thread.sleep(it.toMillis().coerceAtLeast(1)) },
    private val nanoClock: () -> Long = System::nanoTime,
) {
    private val clientLimiter: RateLimiter? =
        if (props.enabled && factory != null) factory.create(RateLimitSpec(client, props.algorithm, props.limit, props.window, props.burst ?: props.limit, props.failMode)) else null

    private val operationLimiters: Map<String, RateLimiter> =
        if (props.enabled && factory != null) props.operations.mapValues { (op, l) ->
            factory.create(RateLimitSpec("$client:$op", props.algorithm, l.limit, l.window, l.burst ?: l.limit, props.failMode))
        } else emptyMap()

    /** Blocks until permitted, or throws. */
    fun acquire(operation: String, deadlineNanos: Long) {
        awaitCooldown(operation, deadlineNanos)
        // Most specific limit first so a rejected op-level call does not burn a client-level permit.
        operationLimiters[operation]?.let { await(it, operation, deadlineNanos) }
        clientLimiter?.let { await(it, operation, deadlineNanos) }
    }

    fun tripCooldown(retryAfter: Duration) {
        if (props.adaptiveCooldown && !retryAfter.isZero) cooldown.trip(client, retryAfter)
    }

    private fun awaitCooldown(operation: String, deadlineNanos: Long) {
        if (!props.adaptiveCooldown) return
        val remaining = cooldown.remaining(client) ?: return
        if (!canWait(remaining, deadlineNanos)) throw RateLimitExceededException(client, operation, "upstream-cooldown", remaining)
        sleeper(remaining)
    }

    private fun await(limiter: RateLimiter, operation: String, deadlineNanos: Long) {
        val waitLimit = nanoClock() + props.maxWait.toNanos()
        while (true) {
            val decision = limiter.tryAcquire()
            if (decision.allowed) return
            val now = nanoClock()
            if (now >= deadlineNanos) throw DeadlineExceededException(client, operation, "deadline reached while waiting for rate limit")
            if (now + decision.retryAfter.toNanos() > minOf(waitLimit, deadlineNanos)) {
                throw RateLimitExceededException(client, operation, limiter.spec.key, decision.retryAfter)
            }
            sleeper(decision.retryAfter)
        }
    }

    private fun canWait(d: Duration, deadlineNanos: Long): Boolean {
        val now = nanoClock()
        return d <= props.maxWait && now + d.toNanos() < deadlineNanos
    }

    val enabled: Boolean get() = clientLimiter != null || operationLimiters.isNotEmpty()
}
