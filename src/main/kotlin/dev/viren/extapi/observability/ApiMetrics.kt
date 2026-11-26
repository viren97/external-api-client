package dev.viren.extapi.observability

import dev.viren.extapi.resilience.CircuitBreakerRegistry
import dev.viren.extapi.resilience.CircuitState
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration

interface ApiMetrics {
    /** One logical call (after all retries). [outcome] is `success` or a FailureKind name. */
    fun call(client: String, operation: String, method: String, outcome: String, status: Int?, attempts: Int, duration: Duration) {}
    fun retry(client: String, operation: String, reason: String) {}
    fun rejected(client: String, operation: String, reason: String) {}
    fun asyncOutcome(client: String, outcome: String) {}

    object Noop : ApiMetrics
}

class MicrometerApiMetrics(private val registry: MeterRegistry) : ApiMetrics {

    override fun call(client: String, operation: String, method: String, outcome: String, status: Int?, attempts: Int, duration: Duration) {
        Timer.builder("ext_api.client.calls")
            .description("Logical external API calls including retries")
            .tags("client", client, "operation", operation, "method", method, "outcome", outcome, "status", status?.toString() ?: "none")
            .publishPercentileHistogram()
            .register(registry)
            .record(duration)
        registry.summary("ext_api.client.attempts", "client", client, "operation", operation).record(attempts.toDouble())
    }

    override fun retry(client: String, operation: String, reason: String) {
        registry.counter("ext_api.client.retries", "client", client, "operation", operation, "reason", reason).increment()
    }

    override fun rejected(client: String, operation: String, reason: String) {
        registry.counter("ext_api.client.rejected", "client", client, "operation", operation, "reason", reason).increment()
    }

    override fun asyncOutcome(client: String, outcome: String) {
        registry.counter("ext_api.async.outcomes", "client", client, "outcome", outcome).increment()
    }

    /** Gauge per breaker: 0 closed, 1 open, 2 half-open, 3 forced open, -1 disabled. */
    fun bindCircuitBreakers(breakers: CircuitBreakerRegistry, names: () -> Collection<String>) {
        breakers.addListener { name, _, _ -> register(breakers, name) }
        names().forEach { register(breakers, it) }
    }

    private fun register(breakers: CircuitBreakerRegistry, name: String) {
        Gauge.builder("ext_api.circuit.state") {
            when (breakers.find(name)?.currentState) {
                CircuitState.CLOSED -> 0.0; CircuitState.OPEN -> 1.0; CircuitState.HALF_OPEN -> 2.0
                CircuitState.FORCED_OPEN -> 3.0; CircuitState.DISABLED, null -> -1.0
            }
        }.tag("breaker", name).register(registry)
    }
}
