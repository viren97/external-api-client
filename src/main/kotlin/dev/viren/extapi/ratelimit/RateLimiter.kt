package dev.viren.extapi.ratelimit

import dev.viren.extapi.config.FailMode
import dev.viren.extapi.config.RateLimitAlgorithm
import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentHashMap

data class RateLimitDecision(val allowed: Boolean, val retryAfter: Duration = Duration.ZERO) {
    companion object {
        val ALLOW = RateLimitDecision(true)
    }
}

/** Specification of one limit: [limit] permits per [window], with a bucket of size [burst]. */
data class RateLimitSpec(
    val key: String,
    val algorithm: RateLimitAlgorithm,
    val limit: Long,
    val window: Duration,
    val burst: Long = limit,
    /** Only meaningful for distributed backends. */
    val failMode: FailMode = FailMode.OPEN,
) {
    init {
        require(limit > 0) { "rate limit for $key must be > 0" }
        require(!window.isZero && !window.isNegative) { "rate limit window for $key must be > 0" }
    }
}

interface RateLimiter {
    val spec: RateLimitSpec
    fun tryAcquire(permits: Long = 1): RateLimitDecision
}

/** Builds limiters for a backend. Provide your own bean to plug in another store. */
interface RateLimiterFactory {
    fun create(spec: RateLimitSpec): RateLimiter
}

/** In-process token bucket. Limits each JVM separately. */
class LocalTokenBucketRateLimiter(
    override val spec: RateLimitSpec,
    private val nanoClock: () -> Long = System::nanoTime,
) : RateLimiter {
    private val capacity = spec.burst.toDouble()
    private val refillPerNano = spec.limit.toDouble() / spec.window.toNanos()
    private var tokens = capacity
    private var last = nanoClock()

    @Synchronized
    override fun tryAcquire(permits: Long): RateLimitDecision {
        val now = nanoClock()
        tokens = minOf(capacity, tokens + (now - last) * refillPerNano)
        last = now
        if (tokens >= permits) {
            tokens -= permits
            return RateLimitDecision.ALLOW
        }
        val waitNanos = ((permits - tokens) / refillPerNano).toLong()
        return RateLimitDecision(false, Duration.ofNanos(waitNanos.coerceAtLeast(1_000_000)))
    }
}

/** In-process sliding window log: never more than `limit` calls in any `window`-long span. */
class LocalSlidingWindowRateLimiter(
    override val spec: RateLimitSpec,
    private val nanoClock: () -> Long = System::nanoTime,
) : RateLimiter {
    private val events = ArrayDeque<Long>()

    @Synchronized
    override fun tryAcquire(permits: Long): RateLimitDecision {
        val now = nanoClock()
        val windowNanos = spec.window.toNanos()
        while (events.isNotEmpty() && events.peekFirst() <= now - windowNanos) events.pollFirst()
        if (events.size + permits <= spec.limit) {
            repeat(permits.toInt()) { events.addLast(now) }
            return RateLimitDecision.ALLOW
        }
        val oldest = events.peekFirst() ?: now
        return RateLimitDecision(false, Duration.ofNanos((oldest + windowNanos - now).coerceAtLeast(1_000_000)))
    }
}

class LocalRateLimiterFactory : RateLimiterFactory {
    override fun create(spec: RateLimitSpec): RateLimiter = when (spec.algorithm) {
        RateLimitAlgorithm.TOKEN_BUCKET -> LocalTokenBucketRateLimiter(spec)
        RateLimitAlgorithm.SLIDING_WINDOW -> LocalSlidingWindowRateLimiter(spec)
    }
}

/**
 * Shared "upstream told us to back off" state. When a provider answers 429 with Retry-After,
 * tripping the cooldown stops every caller (and with Redis, every instance) from sending until
 * the window passes, instead of each one hitting the 429 on its own.
 */
interface UpstreamCooldown {
    fun trip(key: String, duration: Duration)
    fun remaining(key: String): Duration?
}

class LocalUpstreamCooldown(private val nanoClock: () -> Long = System::nanoTime) : UpstreamCooldown {
    private val until = ConcurrentHashMap<String, Long>()

    override fun trip(key: String, duration: Duration) {
        val deadline = nanoClock() + duration.toNanos()
        until.merge(key, deadline) { a, b -> maxOf(a, b) }
    }

    override fun remaining(key: String): Duration? {
        val deadline = until[key] ?: return null
        val left = deadline - nanoClock()
        if (left <= 0) { until.remove(key, deadline); return null }
        return Duration.ofNanos(left)
    }
}
