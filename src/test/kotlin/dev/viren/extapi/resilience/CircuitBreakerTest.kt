package dev.viren.extapi.resilience

import dev.viren.extapi.config.CircuitBreakerProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class CircuitBreakerTest {
    private var now = 0L
    private val fast = Duration.ofMillis(10)

    private fun breaker(customize: CircuitBreakerProperties.() -> Unit = {}) = CircuitBreaker(
        "test",
        CircuitBreakerProperties().apply {
            slidingWindowSize = 10; minimumCalls = 4; failureRateThreshold = 50.0
            openDuration = Duration.ofSeconds(30); halfOpenPermittedCalls = 2
            customize()
        },
    ) { now }

    private fun CircuitBreaker.call(failed: Boolean, took: Duration = fast) {
        check(tryAcquirePermission())
        if (failed) onError(took) else onSuccess(took)
    }

    @Test
    fun `stays closed below minimum calls even if all fail`() {
        val cb = breaker()
        repeat(3) { cb.call(failed = true) }
        assertThat(cb.currentState).isEqualTo(CircuitState.CLOSED)
    }

    @Test
    fun `opens at failure threshold and rejects calls`() {
        val cb = breaker()
        cb.call(false); cb.call(false); cb.call(true); cb.call(true)
        assertThat(cb.currentState).isEqualTo(CircuitState.OPEN)
        assertThat(cb.tryAcquirePermission()).isFalse()
        assertThat(cb.remainingOpen()).isEqualTo(Duration.ofSeconds(30))
    }

    @Test
    fun `half open lets limited probes through then closes on success`() {
        val cb = breaker()
        repeat(4) { cb.call(true) }
        now += Duration.ofSeconds(30).toNanos()

        assertThat(cb.tryAcquirePermission()).isTrue()
        assertThat(cb.currentState).isEqualTo(CircuitState.HALF_OPEN)
        assertThat(cb.tryAcquirePermission()).isTrue()
        assertThat(cb.tryAcquirePermission()).describedAs("only 2 probes").isFalse()

        cb.onSuccess(fast); cb.onSuccess(fast)
        assertThat(cb.currentState).isEqualTo(CircuitState.CLOSED)
        assertThat(cb.metrics().bufferedCalls).isZero()
    }

    @Test
    fun `half open reopens when probes fail`() {
        val cb = breaker()
        repeat(4) { cb.call(true) }
        now += Duration.ofSeconds(31).toNanos()
        cb.call(true); cb.call(false)
        assertThat(cb.currentState).isEqualTo(CircuitState.OPEN)
    }

    @Test
    fun `ignored probe frees its slot`() {
        val cb = breaker { halfOpenPermittedCalls = 1 }
        repeat(4) { cb.call(true) }
        now += Duration.ofSeconds(31).toNanos()
        assertThat(cb.tryAcquirePermission()).isTrue()
        cb.onIgnored()
        assertThat(cb.tryAcquirePermission()).isTrue()
    }

    @Test
    fun `slow calls open the breaker`() {
        val cb = breaker { slowCallThreshold = Duration.ofSeconds(1); slowCallRateThreshold = 75.0 }
        repeat(4) { cb.call(false, took = Duration.ofSeconds(2)) }
        assertThat(cb.currentState).isEqualTo(CircuitState.OPEN)
    }

    @Test
    fun `sliding window forgets old failures`() {
        val cb = breaker { slidingWindowSize = 4; minimumCalls = 4 }
        cb.call(true); cb.call(false); cb.call(false); cb.call(false) // 25%
        cb.call(true) // window now F? -> drops first F: [F(new), S, S, S] = 25%
        assertThat(cb.currentState).isEqualTo(CircuitState.CLOSED)
        cb.call(true) // [S,S,F,F] = 50%
        assertThat(cb.currentState).isEqualTo(CircuitState.OPEN)
    }

    @Test
    fun `listeners see transitions and forceOpen and reset work`() {
        val seen = mutableListOf<String>()
        val cb = breaker().addListener { _, from, to -> seen += "$from>$to" }
        cb.forceOpen()
        assertThat(cb.tryAcquirePermission()).isFalse()
        cb.reset()
        assertThat(seen).containsExactly("CLOSED>FORCED_OPEN", "FORCED_OPEN>CLOSED")
    }

    @Test
    fun `disabled breaker always permits`() {
        val cb = breaker { enabled = false }
        repeat(10) { cb.call(true) }
        assertThat(cb.currentState).isEqualTo(CircuitState.DISABLED)
        assertThat(cb.tryAcquirePermission()).isTrue()
    }
}
