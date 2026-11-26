package dev.viren.extapi.error

import dev.viren.extapi.config.ClientProperties
import org.apache.hc.client5.http.ConnectTimeoutException
import org.apache.hc.client5.http.HttpHostConnectException
import org.apache.hc.core5.http.ConnectionRequestTimeoutException
import org.apache.hc.core5.http.NoHttpResponseException
import java.io.InterruptedIOException
import java.net.ConnectException
import java.net.NoRouteToHostException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.security.cert.CertificateException
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import javax.net.ssl.SSLException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException

/** What the classifier knows about the call being judged. */
data class CallContext(
    val client: String,
    val operation: String,
    val method: String,
    /** True if repeating the request cannot cause a duplicate side effect. */
    val safeToRepeat: Boolean,
    val properties: ClientProperties,
)

/**
 * Turns HTTP statuses and transport exceptions into [ExternalApiException]s and decides
 * whether each is retryable and whether it should trip the circuit breaker.
 *
 * Replace the bean to teach the module about an upstream's quirks (for example a provider
 * that returns 200 with `{"error": "..."}`, or 409 meaning "already processed").
 */
interface FailureClassifier {
    /** Return null when [status] is a success. */
    fun classifyStatus(ctx: CallContext, status: Int, headers: Map<String, List<String>>, body: String?): ExternalApiException?
    fun classifyException(ctx: CallContext, error: Throwable): ExternalApiException
}

open class DefaultFailureClassifier : FailureClassifier {

    override fun classifyStatus(ctx: CallContext, status: Int, headers: Map<String, List<String>>, body: String?): ExternalApiException? {
        if (status in 200..299) return null
        val retry = ctx.properties.retry
        val retryableStatus = status in retry.retryableStatuses
        val retryAfter = parseRetryAfter(headers)
        return when {
            status == 401 || status == 403 -> UnauthorizedException(ctx.client, ctx.operation, status, body, headers)
            // 429: the upstream rejected without processing, so it is safe even for POST.
            status == 429 -> UpstreamRateLimitedException(ctx.client, ctx.operation, status, body, headers, retryableStatus, retryAfter)
            status == 503 && retryAfter != null ->
                UpstreamRateLimitedException(ctx.client, ctx.operation, status, body, headers, retryableStatus, retryAfter)
            status >= 500 -> ServerErrorException(
                ctx.client, ctx.operation, status, body, headers,
                // A 500 after a POST may have half-applied; only retry it when we can dedupe.
                retryable = retryableStatus && (ctx.safeToRepeat || status == 502 || status == 503),
                retryAfter = retryAfter,
            )
            else -> ClientErrorException(
                ctx.client, ctx.operation, status, body, headers,
                retryable = retryableStatus,
                countsAsFailure = ctx.properties.circuitBreaker.countClientErrors || status == 408,
            )
        }
    }

    override fun classifyException(ctx: CallContext, error: Throwable): ExternalApiException {
        if (error is ExternalApiException) return error
        val c = ctx.client
        val op = ctx.operation
        val root = rootCause(error)
        return when {
            // Request never left this process: always safe to retry.
            error is ConnectionRequestTimeoutException ->
                ApiTimeoutException(FailureKind.POOL_TIMEOUT, c, op, "no pooled connection available within ${ctx.properties.timeouts.connectionRequest}", error, true)
            error is ConnectTimeoutException ->
                ApiTimeoutException(FailureKind.CONNECT_TIMEOUT, c, op, "connect timed out: ${error.message}", error, true)
            error is UnknownHostException ->
                ConnectionFailedException(FailureKind.DNS_FAILURE, c, op, "unknown host ${error.message}", error, true)
            error is HttpHostConnectException || error is ConnectException || error is NoRouteToHostException ->
                ConnectionFailedException(FailureKind.CONNECTION_FAILED, c, op, "connection refused: ${error.message}", error, true)

            // TLS: check before generic SocketException/IOException.
            error is SSLHandshakeException || error is SSLPeerUnverifiedException || root is CertificateException ->
                TlsFailureException(c, op, "TLS handshake failed: ${describeTls(error)}", error)
            error is SSLException && error.message?.contains("closed", ignoreCase = true) == true ->
                ConnectionFailedException(FailureKind.CONNECTION_FAILED, c, op, "TLS connection closed: ${error.message}", error, ctx.safeToRepeat)
            error is SSLException -> TlsFailureException(c, op, "TLS error: ${describeTls(error)}", error)

            // Request may have been processed: retry only when repeating is safe.
            error is SocketTimeoutException ->
                ApiTimeoutException(FailureKind.RESPONSE_TIMEOUT, c, op, "no response within ${ctx.properties.timeouts.response} (outcome unknown)", error, ctx.safeToRepeat)
            error is NoHttpResponseException ->
                ConnectionFailedException(FailureKind.CONNECTION_FAILED, c, op, "server closed connection without response", error, ctx.safeToRepeat)
            error is SocketException ->
                ConnectionFailedException(FailureKind.CONNECTION_FAILED, c, op, "socket error: ${error.message}", error, ctx.safeToRepeat)
            error is InterruptedException || (error is InterruptedIOException && root is InterruptedException) -> {
                Thread.currentThread().interrupt()
                InterruptedCallException(c, op, error)
            }
            error is java.io.IOException ->
                ConnectionFailedException(FailureKind.CONNECTION_FAILED, c, op, "I/O error: ${error.message}", error, ctx.safeToRepeat)
            else -> UnexpectedApiException(c, op, "${error.javaClass.simpleName}: ${error.message}", error)
        }
    }

    private fun describeTls(error: Throwable): String {
        val chain = generateSequence(error) { it.cause }.mapNotNull { it.message }.distinct().toList()
        val hint = when {
            chain.any { it.contains("PKIX", true) || it.contains("unable to find valid certification path", true) } ->
                " (server certificate not trusted: check trust-store / trust-certificates-pem)"
            chain.any { it.contains("No subject alternative", true) || it.contains("doesn't match", true) } ->
                " (hostname does not match certificate SAN)"
            chain.any { it.contains("bad_certificate", true) || it.contains("certificate_required", true) } ->
                " (server rejected our client certificate: check mTLS key-store)"
            chain.any { it.contains("protocol_version", true) || it.contains("No appropriate protocol", true) } ->
                " (no common TLS protocol version)"
            chain.any { it.contains("handshake_failure", true) } -> " (no common cipher suite or client cert missing)"
            chain.any { it.contains("pin", true) } -> " (certificate pin mismatch)"
            else -> ""
        }
        return chain.joinToString(" <- ") + hint
    }

    companion object {
        fun rootCause(t: Throwable): Throwable = generateSequence(t) { it.cause }.last()

        /** Retry-After is either delta-seconds or an HTTP date (RFC 9110 §10.2.3). */
        fun parseRetryAfter(headers: Map<String, List<String>>): Duration? {
            val raw = headers.entries.firstOrNull { it.key.equals("Retry-After", true) }?.value?.firstOrNull()?.trim()
                ?: headers.entries.firstOrNull { it.key.equals("X-RateLimit-Reset-After", true) }?.value?.firstOrNull()?.trim()
                ?: return null
            raw.toLongOrNull()?.let { return Duration.ofSeconds(it.coerceAtLeast(0)) }
            raw.toDoubleOrNull()?.let { return Duration.ofMillis((it * 1000).toLong().coerceAtLeast(0)) }
            return runCatching {
                val at = ZonedDateTime.parse(raw, DateTimeFormatter.RFC_1123_DATE_TIME)
                Duration.between(ZonedDateTime.now(at.zone), at).takeIf { !it.isNegative } ?: Duration.ZERO
            }.getOrNull()
        }
    }
}
