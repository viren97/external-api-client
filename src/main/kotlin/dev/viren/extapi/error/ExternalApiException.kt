package dev.viren.extapi.error

import java.time.Duration

/** Stable failure categories, used for metrics tags, outbox records and branching in callers. */
enum class FailureKind {
    CLIENT_ERROR, UNAUTHORIZED, SERVER_ERROR, UPSTREAM_RATE_LIMITED,
    CONNECT_TIMEOUT, RESPONSE_TIMEOUT, POOL_TIMEOUT, DEADLINE_EXCEEDED,
    CONNECTION_FAILED, DNS_FAILURE, TLS_FAILURE,
    CIRCUIT_OPEN, RATE_LIMITED, BULKHEAD_FULL,
    SERIALIZATION, RESPONSE_TOO_LARGE, INTERRUPTED, UNEXPECTED,
}

/**
 * Every failure the module raises. It is sealed so callers can `when` over it exhaustively:
 *
 * ```kotlin
 * try { client.get<Account>("/accounts/1") }
 * catch (e: ExternalApiException) { when (e) { is ClientErrorException -> ...; is CircuitOpenException -> ...; else -> ... } }
 * ```
 *
 * @property retryable whether sending the same request again can succeed and is safe.
 * @property countsAsFailure whether the circuit breaker should treat it as an upstream fault.
 */
sealed class ExternalApiException(
    val kind: FailureKind,
    message: String,
    cause: Throwable? = null,
    val client: String,
    val operation: String?,
    val retryable: Boolean,
    val countsAsFailure: Boolean,
    val status: Int? = null,
    val responseBody: String? = null,
    val responseHeaders: Map<String, List<String>> = emptyMap(),
    val retryAfter: Duration? = null,
) : RuntimeException("[$client${operation?.let { "/$it" } ?: ""}] $message", cause) {
    /** Attempts made before giving up; set by the client once the retry loop ends. */
    var attempts: Int = 1
        internal set
}

// ---- HTTP status failures -------------------------------------------------------------

class ClientErrorException(client: String, operation: String?, status: Int, body: String?, headers: Map<String, List<String>>, retryable: Boolean, countsAsFailure: Boolean) :
    ExternalApiException(FailureKind.CLIENT_ERROR, "HTTP $status", null, client, operation, retryable, countsAsFailure, status, body, headers)

class UnauthorizedException(client: String, operation: String?, status: Int, body: String?, headers: Map<String, List<String>>) :
    ExternalApiException(FailureKind.UNAUTHORIZED, "HTTP $status (credentials rejected)", null, client, operation, false, false, status, body, headers)

class ServerErrorException(client: String, operation: String?, status: Int, body: String?, headers: Map<String, List<String>>, retryable: Boolean, retryAfter: Duration?) :
    ExternalApiException(FailureKind.SERVER_ERROR, "HTTP $status", null, client, operation, retryable, true, status, body, headers, retryAfter)

class UpstreamRateLimitedException(client: String, operation: String?, status: Int, body: String?, headers: Map<String, List<String>>, retryable: Boolean, retryAfter: Duration?) :
    ExternalApiException(FailureKind.UPSTREAM_RATE_LIMITED, "HTTP $status rate limited by upstream" + (retryAfter?.let { ", retry after ${it.toMillis()}ms" } ?: ""), null, client, operation, retryable, false, status, body, headers, retryAfter)

// ---- Transport failures ---------------------------------------------------------------

class ApiTimeoutException(kind: FailureKind, client: String, operation: String?, message: String, cause: Throwable?, retryable: Boolean) :
    ExternalApiException(kind, message, cause, client, operation, retryable, kind != FailureKind.POOL_TIMEOUT)

class ConnectionFailedException(kind: FailureKind, client: String, operation: String?, message: String, cause: Throwable?, retryable: Boolean) :
    ExternalApiException(kind, message, cause, client, operation, retryable, true)

/** Handshake / certificate problems. Almost always configuration, so never retried. */
class TlsFailureException(client: String, operation: String?, message: String, cause: Throwable?) :
    ExternalApiException(FailureKind.TLS_FAILURE, message, cause, client, operation, false, true)

// ---- Local protection rejections (the request was never sent) -------------------------

class CircuitOpenException(client: String, operation: String?, breaker: String, retryAfter: Duration?) :
    ExternalApiException(FailureKind.CIRCUIT_OPEN, "circuit '$breaker' is open", null, client, operation, false, false, retryAfter = retryAfter)

class RateLimitExceededException(client: String, operation: String?, limiter: String, retryAfter: Duration?) :
    ExternalApiException(FailureKind.RATE_LIMITED, "local rate limit '$limiter' exceeded" + (retryAfter?.let { ", retry after ${it.toMillis()}ms" } ?: ""), null, client, operation, false, false, retryAfter = retryAfter)

class BulkheadFullException(client: String, operation: String?, maxConcurrent: Int) :
    ExternalApiException(FailureKind.BULKHEAD_FULL, "more than $maxConcurrent concurrent calls", null, client, operation, false, false)

class DeadlineExceededException(client: String, operation: String?, message: String, cause: Throwable? = null) :
    ExternalApiException(FailureKind.DEADLINE_EXCEEDED, message, cause, client, operation, false, false)

// ---- Payload / misc -------------------------------------------------------------------

class SerializationFailureException(client: String, operation: String?, message: String, cause: Throwable?, status: Int? = null, body: String? = null) :
    ExternalApiException(FailureKind.SERIALIZATION, message, cause, client, operation, false, false, status, body)

class ResponseTooLargeException(client: String, operation: String?, limit: Long, status: Int) :
    ExternalApiException(FailureKind.RESPONSE_TOO_LARGE, "response body exceeds $limit bytes", null, client, operation, false, false, status)

class InterruptedCallException(client: String, operation: String?, cause: Throwable) :
    ExternalApiException(FailureKind.INTERRUPTED, "call interrupted", cause, client, operation, false, false)

class UnexpectedApiException(client: String, operation: String?, message: String, cause: Throwable?) :
    ExternalApiException(FailureKind.UNEXPECTED, message, cause, client, operation, false, true)
