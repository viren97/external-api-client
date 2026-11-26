package dev.viren.extapi.http

import com.fasterxml.jackson.databind.ObjectMapper
import dev.viren.extapi.auth.AuthProvider
import dev.viren.extapi.auth.NoAuth
import dev.viren.extapi.auth.OAuth2ClientCredentialsAuth
import dev.viren.extapi.auth.StaticHeaderAuth
import dev.viren.extapi.config.AuthType
import dev.viren.extapi.config.ClientProperties
import dev.viren.extapi.config.RateLimitBackend
import dev.viren.extapi.error.FailureClassifier
import dev.viren.extapi.observability.ApiMetrics
import dev.viren.extapi.ratelimit.LocalRateLimiterFactory
import dev.viren.extapi.ratelimit.LocalUpstreamCooldown
import dev.viren.extapi.ratelimit.RateLimitGate
import dev.viren.extapi.ratelimit.RateLimiterFactory
import dev.viren.extapi.ratelimit.UpstreamCooldown
import dev.viren.extapi.resilience.Bulkhead
import dev.viren.extapi.resilience.CircuitBreakerRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/** Lets the host application adjust a client's settings in code before it is built. */
fun interface ExternalApiClientCustomizer {
    fun customize(name: String, properties: ClientProperties)
}

/**
 * Builds and owns every named client. Inject it and call `client("payments")`.
 * Clients can also be registered at runtime (e.g. per-tenant endpoints loaded from a DB).
 */
class ExternalApiClientRegistry(
    configured: Map<String, ClientProperties>,
    private val httpFactory: HttpClientFactory,
    private val mapper: ObjectMapper,
    private val classifier: FailureClassifier,
    val circuitBreakers: CircuitBreakerRegistry,
    private val localLimiters: RateLimiterFactory = LocalRateLimiterFactory(),
    private val redisLimiters: RateLimiterFactory? = null,
    private val localCooldown: UpstreamCooldown = LocalUpstreamCooldown(),
    private val sharedCooldown: UpstreamCooldown? = null,
    private val interceptors: List<ApiCallInterceptor> = emptyList(),
    private val customizers: List<ExternalApiClientCustomizer> = emptyList(),
    private val authBeans: Map<String, AuthProvider> = emptyMap(),
    private val metrics: ApiMetrics = ApiMetrics.Noop,
) : AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)
    private val clients = ConcurrentHashMap<String, DefaultExternalApiClient>()
    private val properties = ConcurrentHashMap<String, ClientProperties>()

    init {
        // Build eagerly so bad TLS material or missing settings fail application startup.
        configured.forEach { (name, props) -> register(name, props) }
    }

    fun client(name: String): ExternalApiClient =
        clients[name] ?: throw IllegalArgumentException("No ext-api client named '$name'. Configured: ${clients.keys}")

    fun clientOrNull(name: String): ExternalApiClient? = clients[name]

    fun names(): Set<String> = clients.keys

    /** Effective settings of a client (after customizers ran). */
    fun properties(name: String): ClientProperties? = properties[name]

    /** Creates (or replaces) a client at runtime. A replaced client's pool is closed. */
    fun register(name: String, props: ClientProperties): ExternalApiClient {
        customizers.forEach { it.customize(name, props) }
        val pooled = httpFactory.create(name, props)
        val rl = props.rateLimit
        val factory = if (rl.backend == RateLimitBackend.REDIS) {
            redisLimiters ?: run {
                if (rl.enabled) log.warn("ext-api client '{}' wants REDIS rate limiting but Redis is not configured; using LOCAL", name)
                localLimiters
            }
        } else localLimiters
        val cooldown = if (rl.backend == RateLimitBackend.REDIS && sharedCooldown != null) sharedCooldown else localCooldown

        val client = DefaultExternalApiClient(
            name = name,
            props = props,
            pooled = pooled,
            mapper = mapper,
            auth = authFor(name, props, pooled),
            classifier = classifier,
            breakers = circuitBreakers,
            rateLimits = RateLimitGate(name, rl, factory, cooldown),
            bulkhead = props.bulkhead.takeIf { it.enabled }?.let { Bulkhead(it.maxConcurrentCalls, it.maxWait) },
            interceptors = interceptors,
            metrics = metrics,
        )
        properties[name] = props
        clients.put(name, client)?.close()
        log.info("ext-api client '{}' ready: baseUrl={} tls={} rateLimit={} circuitBreaker={} retry={}x",
            name, props.baseUrl, props.tls.mode, if (rl.enabled) "${rl.limit}/${rl.window} ${rl.backend}" else "off",
            props.circuitBreaker.enabled, if (props.retry.enabled) props.retry.maxAttempts else 1)
        return client
    }

    private fun authFor(name: String, props: ClientProperties, pooled: PooledHttpClient): AuthProvider {
        val a = props.auth
        fun req(v: String?, field: String) = requireNotNull(v) { "ext-api client '$name': auth.$field is required for ${a.type}" }
        return when (a.type) {
            AuthType.NONE -> NoAuth
            AuthType.BEARER -> StaticHeaderAuth.bearer(req(a.token, "token"))
            AuthType.API_KEY -> StaticHeaderAuth.apiKey(a.apiKeyHeader, req(a.apiKey, "api-key"))
            AuthType.BASIC -> StaticHeaderAuth.basic(req(a.username, "username"), req(a.password, "password"))
            AuthType.OAUTH2_CLIENT_CREDENTIALS -> OAuth2ClientCredentialsAuth(a, pooled.http, mapper)
            AuthType.CUSTOM -> authBeans[req(a.customBean, "custom-bean")]
                ?: throw IllegalArgumentException("ext-api client '$name': no AuthProvider bean named '${a.customBean}'")
        }
    }

    override fun close() {
        clients.values.forEach { runCatching { it.close() } }
        clients.clear()
    }
}
