package dev.viren.extapi.support

import dev.viren.extapi.config.ClientProperties
import dev.viren.extapi.config.ExtApiAutoConfiguration
import dev.viren.extapi.error.DefaultFailureClassifier
import dev.viren.extapi.http.ExternalApiClient
import dev.viren.extapi.http.ExternalApiClientRegistry
import dev.viren.extapi.http.HttpClientFactory
import dev.viren.extapi.http.TlsContextFactory
import dev.viren.extapi.resilience.CircuitBreakerRegistry
import org.springframework.core.io.DefaultResourceLoader
import java.time.Duration

/** Builds a real client without Spring, with fast timeouts/backoff suitable for tests. */
object Clients {
    fun registry(vararg clients: Pair<String, ClientProperties>) = ExternalApiClientRegistry(
        configured = clients.toMap(),
        httpFactory = HttpClientFactory(TlsContextFactory(DefaultResourceLoader())),
        mapper = ExtApiAutoConfiguration.defaultMapper(),
        classifier = DefaultFailureClassifier(),
        circuitBreakers = CircuitBreakerRegistry(),
    )

    fun client(baseUrl: String, customize: ClientProperties.() -> Unit = {}): ExternalApiClient =
        registry("test" to props(baseUrl, customize)).client("test")

    fun props(baseUrl: String, customize: ClientProperties.() -> Unit = {}) = ClientProperties().apply {
        this.baseUrl = baseUrl
        timeouts.connect = Duration.ofMillis(500)
        timeouts.response = Duration.ofSeconds(2)
        timeouts.total = Duration.ofSeconds(10)
        retry.initialBackoff = Duration.ofMillis(10)
        retry.maxBackoff = Duration.ofMillis(50)
        circuitBreaker.minimumCalls = 4
        circuitBreaker.slidingWindowSize = 10
        customize()
    }
}
