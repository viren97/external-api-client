package dev.viren.extapi.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * Root configuration. Every external system is a named entry under `ext-api.clients`:
 *
 * ```yaml
 * ext-api:
 *   clients:
 *     payments:
 *       base-url: https://api.psp.com
 *       tls: { mode: MTLS, key-store: { location: file:/certs/client.p12, password: ${KS_PASS} } }
 *       rate-limit: { enabled: true, backend: REDIS, limit: 50, window: 1s }
 * ```
 */
@ConfigurationProperties(prefix = "ext-api")
class ExtApiProperties {
    var enabled: Boolean = true
    var clients: MutableMap<String, ClientProperties> = linkedMapOf()
    var redis = RedisProperties()
    var async = AsyncProperties()
    var webhooks = WebhookProperties()

    class RedisProperties {
        /** Every Redis key this module writes starts with this prefix. */
        var keyPrefix: String = "extapi"
    }
}

class ClientProperties {
    /** Base URL; request paths are resolved against it. Absolute request URLs bypass it. */
    var baseUrl: String = ""
    var userAgent: String = "ext-api-client/0.1"
    var defaultHeaders: MutableMap<String, String> = linkedMapOf()
    /** Response bodies larger than this are rejected (protects heap from runaway upstreams). */
    var maxResponseBytes: Long = 10L * 1024 * 1024
    var followRedirects: Boolean = false

    var timeouts = TimeoutProperties()
    var pool = PoolProperties()
    var tls = TlsProperties()
    var auth = AuthProperties()
    var retry = RetryProperties()
    var circuitBreaker = CircuitBreakerProperties()
    var rateLimit = RateLimitProperties()
    var bulkhead = BulkheadProperties()
    var logging = LoggingProperties()
}

class TimeoutProperties {
    /** TCP + TLS handshake. */
    var connect: Duration = Duration.ofSeconds(2)
    /** Socket inactivity while waiting for / reading the response. */
    var response: Duration = Duration.ofSeconds(10)
    /** Waiting for a free pooled connection. */
    var connectionRequest: Duration = Duration.ofSeconds(1)
    /** Hard deadline for the whole call, including every retry and backoff sleep. */
    var total: Duration = Duration.ofSeconds(30)
}

class PoolProperties {
    var maxTotal: Int = 100
    var maxPerRoute: Int = 20
    var timeToLive: Duration = Duration.ofMinutes(5)
    var evictIdleAfter: Duration = Duration.ofSeconds(30)
    var validateAfterInactivity: Duration = Duration.ofSeconds(2)
}

enum class TlsMode {
    /** Plain client: http or https using the JVM's default trust store. */
    DEFAULT,
    /** One-way TLS with a custom trust store / PEM CA bundle (private CAs). */
    TLS,
    /** Mutual TLS: we also present a client certificate. */
    MTLS,
    /** Trusts every certificate. Local development only; logs a warning at startup. */
    INSECURE_TRUST_ALL,
}

class TlsProperties {
    var mode: TlsMode = TlsMode.DEFAULT
    /** Name of a Spring Boot SSL bundle (`spring.ssl.bundle.*`). When set it replaces the store settings below. */
    var bundle: String? = null
    var trustStore = StoreProperties()
    /** PEM file with one or more CA certificates. Alternative to [trustStore]. */
    var trustCertificatesPem: String? = null
    var keyStore = StoreProperties()
    /** Password of the private key entry; defaults to the key store password. */
    var keyPassword: String? = null
    /** Pick a specific entry when the key store holds several client certificates. */
    var keyAlias: String? = null
    /** PEM client certificate chain + PKCS#8 private key. Alternative to [keyStore]. */
    var clientCertificatePem: String? = null
    var clientPrivateKeyPem: String? = null
    var protocols: MutableList<String> = mutableListOf("TLSv1.3", "TLSv1.2")
    var cipherSuites: MutableList<String> = mutableListOf()
    var hostnameVerification: Boolean = true
    /** Optional public-key pins: base64 SHA-256 of the server certificate's SubjectPublicKeyInfo. */
    var pinnedSha256: MutableList<String> = mutableListOf()
}

class StoreProperties {
    /** Spring resource location, e.g. `classpath:certs/trust.p12` or `file:/etc/certs/client.p12`. */
    var location: String? = null
    var password: String? = null
    var type: String = "PKCS12"
}

enum class AuthType { NONE, BEARER, API_KEY, BASIC, OAUTH2_CLIENT_CREDENTIALS, CUSTOM }

class AuthProperties {
    var type: AuthType = AuthType.NONE
    var token: String? = null
    var apiKey: String? = null
    var apiKeyHeader: String = "X-API-Key"
    var username: String? = null
    var password: String? = null
    var tokenUrl: String? = null
    var clientId: String? = null
    var clientSecret: String? = null
    var scope: String? = null
    var audience: String? = null
    /** Refresh OAuth tokens this long before they expire. */
    var refreshSkew: Duration = Duration.ofSeconds(30)
    /** Bean name of an [dev.viren.extapi.auth.AuthProvider] when [type] is CUSTOM. */
    var customBean: String? = null
}

class RetryProperties {
    var enabled: Boolean = true
    /** Total attempts including the first one. */
    var maxAttempts: Int = 3
    var initialBackoff: Duration = Duration.ofMillis(200)
    var maxBackoff: Duration = Duration.ofSeconds(5)
    var multiplier: Double = 2.0
    /** 0 = no jitter, 1 = full jitter. Spreads retries from many callers apart. */
    var jitter: Double = 0.5
    var retryableStatuses: MutableSet<Int> = mutableSetOf(408, 425, 429, 500, 502, 503, 504)
    /** Retry POST/PATCH even without an idempotency key. Only if the upstream dedupes on its own. */
    var retryNonIdempotent: Boolean = false
    var respectRetryAfter: Boolean = true
    /** Retry-After values above this make the call fail instead of sleeping. */
    var maxRetryAfter: Duration = Duration.ofSeconds(30)
    /**
     * Generate an idempotency key for POST/PATCH that lack one, and treat them as safe to retry.
     * Enable only for upstreams that actually dedupe on [idempotencyHeader] (Stripe, Adyen, ...).
     */
    var autoIdempotencyKey: Boolean = false
    var idempotencyHeader: String = "Idempotency-Key"
}

class CircuitBreakerProperties {
    var enabled: Boolean = true
    /** Keep a separate breaker per operation instead of one per client. */
    var perOperation: Boolean = false
    /** Number of most recent calls the failure rate is computed over. */
    var slidingWindowSize: Int = 50
    /** No decision is made until this many calls are in the window. */
    var minimumCalls: Int = 20
    var failureRateThreshold: Double = 50.0
    var slowCallThreshold: Duration = Duration.ofSeconds(5)
    var slowCallRateThreshold: Double = 100.0
    /** How long the breaker stays open before letting probe calls through. */
    var openDuration: Duration = Duration.ofSeconds(30)
    /** Probe calls allowed in HALF_OPEN; all of them decide whether to close or reopen. */
    var halfOpenPermittedCalls: Int = 5
    /** Count 4xx (other than 408/429) as failures. Off by default: they are the caller's fault. */
    var countClientErrors: Boolean = false
}

enum class RateLimitBackend { LOCAL, REDIS }
enum class RateLimitAlgorithm { TOKEN_BUCKET, SLIDING_WINDOW }
/** Behaviour when the distributed rate-limit store is unreachable. */
enum class FailMode { OPEN, CLOSED, LOCAL_FALLBACK }

class RateLimitProperties {
    var enabled: Boolean = false
    var backend: RateLimitBackend = RateLimitBackend.LOCAL
    var algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    /** Requests allowed per [window]. */
    var limit: Long = 100
    var window: Duration = Duration.ofSeconds(1)
    /** Token bucket capacity (burst). Defaults to [limit]. */
    var burst: Long? = null
    /** How long a caller may block waiting for a permit. 0 = fail fast. */
    var maxWait: Duration = Duration.ZERO
    /** Redis unreachable: OPEN lets calls through, CLOSED rejects them, LOCAL_FALLBACK limits per instance. */
    var failMode: FailMode = FailMode.OPEN
    /** Additional limits per operation name (e.g. a stricter quota on `create-payout`). */
    var operations: MutableMap<String, OperationLimit> = linkedMapOf()
    /**
     * When the upstream answers 429/503 with Retry-After, pause every instance for that long
     * (shared through Redis when the backend is REDIS) instead of letting each one discover it.
     */
    var adaptiveCooldown: Boolean = true

    class OperationLimit {
        var limit: Long = 10
        var window: Duration = Duration.ofSeconds(1)
        var burst: Long? = null
    }
}

class BulkheadProperties {
    var enabled: Boolean = false
    var maxConcurrentCalls: Int = 50
    var maxWait: Duration = Duration.ZERO
}

class LoggingProperties {
    var enabled: Boolean = true
    var logBodies: Boolean = false
    var maxBodyChars: Int = 2000
    var maskedHeaders: MutableSet<String> = mutableSetOf(
        "authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key",
    )
    /** JSON field names whose values are masked when bodies are logged. */
    var maskedFields: MutableSet<String> = mutableSetOf("password", "secret", "token", "cardNumber", "cvv", "pan")
}

/** KAFKA spreads calls across every instance; IN_PROCESS runs them on a local pool (no broker). */
enum class AsyncTransportType { KAFKA, IN_PROCESS }

class AsyncProperties {
    var enabled: Boolean = false
    var transport: AsyncTransportType = AsyncTransportType.KAFKA
    var requestTopic: String = "ext-api.requests"
    var resultTopic: String = "ext-api.results"
    var deadLetterTopic: String = "ext-api.requests.dlt"
    var consumerGroup: String = "ext-api-workers"
    var consumerConcurrency: Int = 3
    var tableName: String = "ext_api_outbox"
    var initializeSchema: Boolean = true
    var relayInterval: Duration = Duration.ofMillis(500)
    var batchSize: Int = 100
    /**
     * A PROCESSING row not finished within this time is considered abandoned and re-queued.
     * Must exceed the client's `timeouts.total`, otherwise a slow call could run twice.
     */
    var lease: Duration = Duration.ofSeconds(60)
    /** A QUEUED row no worker picked up within this time (lost message, long consumer lag) is re-dispatched. */
    var queuedLease: Duration = Duration.ofMinutes(10)
    var maxAttempts: Int = 8
    var initialBackoff: Duration = Duration.ofSeconds(5)
    var maxBackoff: Duration = Duration.ofHours(1)
    var publishTimeout: Duration = Duration.ofSeconds(5)
    /** Completed rows older than this are deleted. */
    var retention: Duration = Duration.ofDays(7)
    var maxStoredResponseChars: Int = 16_000
}

class WebhookProperties {
    var outbound = Outbound()
    var inbound = Inbound()

    class Outbound {
        /** Name of the client used for deliveries; created with defaults if not configured. */
        var clientName: String = "webhooks"
        var signingSecret: String? = null
        var signatureHeader: String = "X-Webhook-Signature"
        var timestampHeader: String = "X-Webhook-Timestamp"
        var idHeader: String = "X-Webhook-Id"
        var eventTypeHeader: String = "X-Webhook-Event"
        var maxAttempts: Int = 10
    }

    class Inbound {
        var enabled: Boolean = false
        var path: String = "/webhooks"
        var tolerance: Duration = Duration.ofMinutes(5)
        var replayTtl: Duration = Duration.ofHours(24)
        var sources: MutableMap<String, Source> = linkedMapOf()
    }

    class Source {
        /** Several secrets may be active at once so they can be rotated without downtime. */
        var secrets: MutableList<String> = mutableListOf()
        var signatureHeader: String = "X-Webhook-Signature"
        var timestampHeader: String = "X-Webhook-Timestamp"
        var idHeader: String = "X-Webhook-Id"
    }
}
