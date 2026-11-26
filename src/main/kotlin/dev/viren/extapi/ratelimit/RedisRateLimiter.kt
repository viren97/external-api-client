package dev.viren.extapi.ratelimit

import dev.viren.extapi.config.FailMode
import dev.viren.extapi.config.RateLimitAlgorithm
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * Distributed limiters backed by Redis. Each check is one Lua script, so the read-modify-write
 * is atomic across every instance. The scripts read time with `redis.call('TIME')`, so clock
 * skew between application nodes does not matter. Keys use a `{hash tag}` so they work on
 * Redis Cluster.
 */
class RedisRateLimiterFactory(
    private val redis: StringRedisTemplate,
    private val keyPrefix: String,
    private val localFallback: RateLimiterFactory = LocalRateLimiterFactory(),
) : RateLimiterFactory {
    override fun create(spec: RateLimitSpec): RateLimiter {
        val key = "$keyPrefix:rl:{${spec.key}}"
        val primary = when (spec.algorithm) {
            RateLimitAlgorithm.TOKEN_BUCKET -> RedisTokenBucketRateLimiter(spec, redis, key)
            RateLimitAlgorithm.SLIDING_WINDOW -> RedisSlidingWindowRateLimiter(spec, redis, key)
        }
        return ResilientRateLimiter(primary, spec.failMode, lazy { localFallback.create(spec) })
    }
}

class RedisTokenBucketRateLimiter(
    override val spec: RateLimitSpec,
    private val redis: StringRedisTemplate,
    private val key: String,
) : RateLimiter {
    private val refillPerMs = spec.limit.toDouble() / spec.window.toMillis()
    // Keep the key a little longer than a full refill so idle buckets expire on their own.
    private val ttlMs = ((spec.burst / refillPerMs) * 2).toLong().coerceAtLeast(1000)

    override fun tryAcquire(permits: Long): RateLimitDecision {
        val result = redis.execute(SCRIPT, listOf(key), spec.burst.toString(), refillPerMs.toString(), permits.toString(), ttlMs.toString())
        return toDecision(result)
    }

    companion object {
        val SCRIPT = DefaultRedisScript(
            """
            local t = redis.call('TIME')
            local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
            local capacity = tonumber(ARGV[1])
            local rate = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local ttl = tonumber(ARGV[4])
            local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
            local tokens = tonumber(data[1])
            local ts = tonumber(data[2])
            if tokens == nil then tokens = capacity; ts = now end
            tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
            local allowed = 0
            local wait = 0
            if tokens >= requested then
              tokens = tokens - requested
              allowed = 1
            else
              wait = math.ceil((requested - tokens) / rate)
            end
            redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
            redis.call('PEXPIRE', KEYS[1], ttl)
            return {allowed, wait}
            """.trimIndent(),
            List::class.java,
        )
    }
}

class RedisSlidingWindowRateLimiter(
    override val spec: RateLimitSpec,
    private val redis: StringRedisTemplate,
    private val key: String,
) : RateLimiter {
    override fun tryAcquire(permits: Long): RateLimitDecision {
        val result = redis.execute(
            SCRIPT, listOf(key),
            spec.limit.toString(), spec.window.toMillis().toString(), permits.toString(), UUID.randomUUID().toString(),
        )
        return toDecision(result)
    }

    companion object {
        val SCRIPT = DefaultRedisScript(
            """
            local t = redis.call('TIME')
            local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
            local limit = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
            local count = redis.call('ZCARD', KEYS[1])
            if count + requested <= limit then
              for i = 1, requested do redis.call('ZADD', KEYS[1], now, ARGV[4] .. ':' .. i) end
              redis.call('PEXPIRE', KEYS[1], window)
              return {1, 0}
            end
            local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            local wait = window
            if oldest[2] then wait = math.max(1, tonumber(oldest[2]) + window - now) end
            return {0, wait}
            """.trimIndent(),
            List::class.java,
        )
    }
}

private fun toDecision(result: List<*>?): RateLimitDecision {
    val allowed = (result?.getOrNull(0) as? Number)?.toLong() == 1L
    val waitMs = (result?.getOrNull(1) as? Number)?.toLong() ?: 0L
    return if (allowed) RateLimitDecision.ALLOW else RateLimitDecision(false, Duration.ofMillis(waitMs.coerceAtLeast(1)))
}

/**
 * Applies [FailMode] when Redis is unreachable:
 * OPEN lets calls through, CLOSED rejects them, LOCAL_FALLBACK switches to a per-instance limiter.
 */
class ResilientRateLimiter(
    private val delegate: RateLimiter,
    private val failMode: FailMode,
    private val fallback: Lazy<RateLimiter>,
) : RateLimiter {
    private val log = LoggerFactory.getLogger(javaClass)
    private val lastWarn = AtomicLong(0)
    override val spec: RateLimitSpec get() = delegate.spec

    override fun tryAcquire(permits: Long): RateLimitDecision = try {
        delegate.tryAcquire(permits)
    } catch (e: RuntimeException) {
        val now = System.nanoTime()
        val prev = lastWarn.get()
        if (now - prev > TimeUnit.SECONDS.toNanos(10) && lastWarn.compareAndSet(prev, now)) {
            log.warn("Rate limiter '{}' backend unavailable ({}); applying fail mode {}", spec.key, e.message, failMode)
        }
        when (failMode) {
            FailMode.OPEN -> RateLimitDecision.ALLOW
            FailMode.CLOSED -> RateLimitDecision(false, Duration.ofSeconds(1))
            FailMode.LOCAL_FALLBACK -> fallback.value.tryAcquire(permits)
        }
    }
}

class RedisUpstreamCooldown(
    private val redis: StringRedisTemplate,
    private val keyPrefix: String,
    private val local: UpstreamCooldown = LocalUpstreamCooldown(),
) : UpstreamCooldown {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun trip(key: String, duration: Duration) {
        local.trip(key, duration) // always remember locally, even if Redis is down
        runCatching {
            redis.opsForValue().set(redisKey(key), "1", duration.toMillis().coerceAtLeast(1), TimeUnit.MILLISECONDS)
        }.onFailure { log.debug("Could not share cooldown for {}: {}", key, it.message) }
    }

    override fun remaining(key: String): Duration? {
        local.remaining(key)?.let { return it }
        val ttl = runCatching { redis.getExpire(redisKey(key), TimeUnit.MILLISECONDS) }.getOrNull() ?: return null
        return if (ttl > 0) Duration.ofMillis(ttl) else null
    }

    private fun redisKey(key: String) = "$keyPrefix:cooldown:{$key}"
}
