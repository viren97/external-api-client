package dev.viren.extapi.ratelimit

import dev.viren.extapi.config.FailMode
import dev.viren.extapi.config.RateLimitAlgorithm
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Runs against a real Redis: `EXT_API_REDIS_HOST=localhost ./gradlew test` (CI starts one). */
@EnabledIfEnvironmentVariable(named = "EXT_API_REDIS_HOST", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisRateLimiterTest {
    private lateinit var cf: LettuceConnectionFactory
    private lateinit var redis: StringRedisTemplate

    @BeforeAll
    fun connect() {
        cf = LettuceConnectionFactory(RedisStandaloneConfiguration(System.getenv("EXT_API_REDIS_HOST"), 6379)).apply { afterPropertiesSet(); start() }
        redis = StringRedisTemplate(cf).apply { afterPropertiesSet() }
    }

    @AfterAll fun close() = cf.destroy()

    private fun spec(alg: RateLimitAlgorithm, limit: Long, window: Duration, burst: Long = limit) =
        RateLimitSpec("t-${UUID.randomUUID()}", alg, limit, window, burst, FailMode.CLOSED)

    @Test
    fun `token bucket is shared and atomic across concurrent callers`() {
        val factory = RedisRateLimiterFactory(redis, "test")
        val s = spec(RateLimitAlgorithm.TOKEN_BUCKET, 20, Duration.ofSeconds(60))
        // Two limiter instances = two application nodes sharing one bucket.
        val nodes = listOf(factory.create(s), factory.create(s))
        val allowed = AtomicInteger()
        val pool = Executors.newFixedThreadPool(8)
        repeat(100) { i -> pool.submit { if (nodes[i % 2].tryAcquire().allowed) allowed.incrementAndGet() } }
        pool.shutdown(); pool.awaitTermination(10, TimeUnit.SECONDS)
        assertThat(allowed.get()).isEqualTo(20)
    }

    @Test
    fun `token bucket refills`() {
        val rl = RedisRateLimiterFactory(redis, "test").create(spec(RateLimitAlgorithm.TOKEN_BUCKET, 10, Duration.ofSeconds(1), burst = 1))
        assertThat(rl.tryAcquire().allowed).isTrue()
        val denied = rl.tryAcquire()
        assertThat(denied.allowed).isFalse()
        assertThat(denied.retryAfter).isLessThanOrEqualTo(Duration.ofMillis(100))
        Thread.sleep(150)
        assertThat(rl.tryAcquire().allowed).isTrue()
    }

    @Test
    fun `sliding window enforces limit and reports wait`() {
        val rl = RedisRateLimiterFactory(redis, "test").create(spec(RateLimitAlgorithm.SLIDING_WINDOW, 3, Duration.ofMillis(500)))
        repeat(3) { assertThat(rl.tryAcquire().allowed).isTrue() }
        val denied = rl.tryAcquire()
        assertThat(denied.allowed).isFalse()
        assertThat(denied.retryAfter).isBetween(Duration.ofMillis(1), Duration.ofMillis(500))
        Thread.sleep(550)
        assertThat(rl.tryAcquire().allowed).isTrue()
    }

    @Test
    fun `cooldown is visible to other instances`() {
        val a = RedisUpstreamCooldown(redis, "test")
        val b = RedisUpstreamCooldown(redis, "test")
        val key = "psp-${UUID.randomUUID()}"
        a.trip(key, Duration.ofSeconds(5))
        assertThat(b.remaining(key)).isBetween(Duration.ofSeconds(3), Duration.ofSeconds(5))
    }
}
