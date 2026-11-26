package dev.viren.extapi.observability

import dev.viren.extapi.resilience.CircuitBreakerRegistry
import dev.viren.extapi.resilience.CircuitState
import org.springframework.boot.actuate.health.AbstractHealthIndicator
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.Status

/**
 * `/actuator/health/extApi`. An open breaker reports UNKNOWN rather than DOWN on purpose: a
 * third-party outage should not make Kubernetes restart our pods, which would not help.
 */
class CircuitBreakerHealthIndicator(private val breakers: CircuitBreakerRegistry) : AbstractHealthIndicator() {
    override fun doHealthCheck(builder: Health.Builder) {
        val all = breakers.all()
        val open = all.values.filter { it.currentState == CircuitState.OPEN || it.currentState == CircuitState.FORCED_OPEN }
        builder.status(if (open.isEmpty()) Status.UP else Status.UNKNOWN)
        all.forEach { (name, b) ->
            val m = b.metrics()
            builder.withDetail(name, mapOf(
                "state" to m.state.name,
                "failureRate" to "%.1f%%".format(m.failureRate),
                "slowCallRate" to "%.1f%%".format(m.slowCallRate),
                "bufferedCalls" to m.bufferedCalls,
            ))
        }
    }
}
