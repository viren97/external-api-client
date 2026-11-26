package dev.viren.extapi.ratelimit

import dev.viren.extapi.config.FailMode
import dev.viren.extapi.config.RateLimitAlgorithm
import dev.viren.extapi.config.RateLimitProperties
import dev.viren.extapi.error.RateLimitExceededException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration

class RateLimiterTest {
    private var now = 0L
    private val clock = { now }
    private fun advance(d: Duration) { now += d.toNanos() }

    @Test
    fun `token bucket allows burst then refills at rate`() {
        val rl = LocalTokenBucketRateLimiter(RateLimitSpec("k", RateLimitAlgorithm.TOKEN_BUCKET, 10, Duration.ofSeconds(1), burst = 3), clock)
        repeat(3) { assertThat(rl.tryAcquire().allowed).isTrue() }
        val denied = rl.tryAcquire()
        assertThat(denied.allowed).isFalse()
        assertThat(denied.retryAfter).isEqualTo(Duration.ofMillis(100))
        advance(Duration.ofMillis(100))
        assertThat(rl.tryAcquire().allowed).isTrue()
    }

    @Test
    fun `sliding window never exceeds limit within any window`() {
        val rl = LocalSlidingWindowRateLimiter(RateLimitSpec("k", RateLimitAlgorithm.SLIDING_WINDOW, 2, Duration.ofSeconds(1)), clock)
        assertThat(rl.tryAcquire().allowed).isTrue()
        advance(Duration.ofMillis(600))
        assertThat(rl.tryAcquire().allowed).isTrue()
        val denied = rl.tryAcquire()
        assertThat(denied.allowed).isFalse()
        assertThat(denied.retryAfter).isEqualTo(Duration.ofMillis(400))
        advance(Duration.ofMillis(400))
        assertThat(rl.tryAcquire().allowed).isTrue()
    }

    @Test
    fun `gate fails fast with retry hint when maxWait is zero`() {
        val gate = RateLimitGate("c", RateLimitProperties().apply { enabled = true; limit = 1; window = Duration.ofSeconds(1) },
            LocalRateLimiterFactory(), LocalUpstreamCooldown())
        val deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos()
        gate.acquire("op", deadline)
        assertThatThrownBy { gate.acquire("op", deadline) }
            .isInstanceOf(RateLimitExceededException::class.java)
            .satisfies({ assertThat((it as RateLimitExceededException).retryAfter).isPositive() })
    }

    @Test
    fun `gate waits for a permit within maxWait`() {
        val slept = mutableListOf<Duration>()
        val props = RateLimitProperties().apply { enabled = true; limit = 10; window = Duration.ofSeconds(1); burst = 1; maxWait = Duration.ofSeconds(1) }
        val limiterClock = { now }
        val factory = object : RateLimiterFactory {
            override fun create(spec: RateLimitSpec) = LocalTokenBucketRateLimiter(spec, limiterClock)
        }
        val gate = RateLimitGate("c", props, factory, LocalUpstreamCooldown(limiterClock), { slept += it; advance(it) }, limiterClock)
        gate.acquire("op", Long.MAX_VALUE)
        gate.acquire("op", Long.MAX_VALUE)
        assertThat(slept).containsExactly(Duration.ofMillis(100))
    }

    @Test
    fun `per operation limit is enforced separately`() {
        val props = RateLimitProperties().apply {
            enabled = true; limit = 100
            operations["payout"] = RateLimitProperties.OperationLimit().apply { limit = 1 }
        }
        val gate = RateLimitGate("c", props, LocalRateLimiterFactory(), LocalUpstreamCooldown())
        gate.acquire("payout", Long.MAX_VALUE)
        gate.acquire("balance", Long.MAX_VALUE)
        assertThatThrownBy { gate.acquire("payout", Long.MAX_VALUE) }.isInstanceOf(RateLimitExceededException::class.java)
    }

    @Test
    fun `upstream cooldown blocks every caller until it expires`() {
        val cooldown = LocalUpstreamCooldown(clock)
        val gate = RateLimitGate("c", RateLimitProperties(), null, cooldown, { advance(it) }, clock)
        gate.tripCooldown(Duration.ofSeconds(2))
        assertThatThrownBy { gate.acquire("op", Long.MAX_VALUE) }
            .isInstanceOf(RateLimitExceededException::class.java).hasMessageContaining("upstream-cooldown")
        advance(Duration.ofSeconds(2))
        gate.acquire("op", Long.MAX_VALUE)
    }

    @Test
    fun `resilient limiter applies fail mode when backend is down`() {
        val broken = object : RateLimiter {
            override val spec = RateLimitSpec("k", RateLimitAlgorithm.TOKEN_BUCKET, 1, Duration.ofSeconds(1))
            override fun tryAcquire(permits: Long): RateLimitDecision = throw IllegalStateException("redis down")
        }
        val local = lazy { LocalTokenBucketRateLimiter(broken.spec, clock) }
        assertThat(ResilientRateLimiter(broken, FailMode.OPEN, local).tryAcquire().allowed).isTrue()
        assertThat(ResilientRateLimiter(broken, FailMode.CLOSED, local).tryAcquire().allowed).isFalse()
        val fallback = ResilientRateLimiter(broken, FailMode.LOCAL_FALLBACK, local)
        assertThat(fallback.tryAcquire().allowed).isTrue()
        assertThat(fallback.tryAcquire().allowed).isFalse()
    }
}
