package dev.viren.extapi.http

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import dev.viren.extapi.auth.AuthProvider
import dev.viren.extapi.config.ClientProperties
import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.core.ApiResponse
import dev.viren.extapi.error.BulkheadFullException
import dev.viren.extapi.error.CallContext
import dev.viren.extapi.error.CircuitOpenException
import dev.viren.extapi.error.DeadlineExceededException
import dev.viren.extapi.error.ExternalApiException
import dev.viren.extapi.error.FailureClassifier
import dev.viren.extapi.error.FailureKind
import dev.viren.extapi.error.ResponseTooLargeException
import dev.viren.extapi.error.SerializationFailureException
import dev.viren.extapi.error.UnauthorizedException
import dev.viren.extapi.observability.ApiMetrics
import dev.viren.extapi.ratelimit.RateLimitGate
import dev.viren.extapi.resilience.Bulkhead
import dev.viren.extapi.resilience.CircuitBreaker
import dev.viren.extapi.resilience.CircuitBreakerRegistry
import dev.viren.extapi.resilience.RetryPolicy
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.ByteArrayEntity
import org.apache.hc.core5.net.URIBuilder
import org.apache.hc.core5.util.Timeout
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.Charset
import java.time.Duration
import java.util.UUID

class DefaultExternalApiClient(
    override val name: String,
    private val props: ClientProperties,
    private val pooled: PooledHttpClient,
    private val mapper: ObjectMapper,
    private val auth: AuthProvider,
    private val classifier: FailureClassifier,
    private val breakers: CircuitBreakerRegistry,
    private val rateLimits: RateLimitGate,
    private val bulkhead: Bulkhead?,
    private val interceptors: List<ApiCallInterceptor> = emptyList(),
    private val metrics: ApiMetrics = ApiMetrics.Noop,
    private val sleeper: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) : ExternalApiClient, AutoCloseable {

    private val log = LoggerFactory.getLogger("dev.viren.extapi.client.$name")
    private val retryPolicy = RetryPolicy(props.retry)
    private val logger = HttpLogFormatter(props.logging)
    private val myInterceptors = interceptors.filter { it.supports(name) }

    init {
        if (props.circuitBreaker.enabled && !props.circuitBreaker.perOperation) breakers.get(name, props.circuitBreaker)
    }

    override fun <T> execute(request: ApiRequest, type: Class<T>): ApiResponse<T> = execute(request, mapper.typeFactory.constructType(type))
    override fun <T> execute(request: ApiRequest, type: TypeReference<T>): ApiResponse<T> = execute(request, mapper.typeFactory.constructType(type))

    override fun <T> execute(request: ApiRequest, type: JavaType): ApiResponse<T> {
        val started = System.nanoTime()
        val deadline = started + props.timeouts.total.toNanos()
        val req = withIdempotencyKey(request)
        val op = req.operationName
        val correlationId = req.headers[CORRELATION_HEADER] ?: MDC.get("correlationId") ?: MDC.get("traceId") ?: UUID.randomUUID().toString()
        val ctx = CallContext(name, op, req.method.name, safeToRepeat(req), props)
        val maxAttempts = if (props.retry.enabled) (req.maxAttempts ?: props.retry.maxAttempts).coerceAtLeast(1) else 1

        val body = try {
            encodeBody(req)
        } catch (e: Exception) {
            throw SerializationFailureException(name, op, "cannot encode request body: ${e.message}", e)
        }

        var attempt = 0
        var authRefreshed = false
        while (true) {
            attempt++
            try {
                val response = attemptOnce<T>(req, body, ctx, type, deadline, attempt, correlationId)
                val elapsed = Duration.ofNanos(System.nanoTime() - started)
                metrics.call(name, op, req.method.name, "success", response.status, attempt, elapsed)
                return response.copy(attempts = attempt, duration = elapsed)
            } catch (e: ExternalApiException) {
                e.attempts = attempt
                if (e is UnauthorizedException && e.status == 401 && !authRefreshed && auth.onUnauthorized()) {
                    // Expired token: refresh once, without spending a retry attempt.
                    authRefreshed = true
                    attempt--
                    log.info("{} {} got 401, refreshed credentials and retrying", name, op)
                    continue
                }
                val delay = retryDelay(e, attempt, maxAttempts, deadline)
                if (delay == null) {
                    val elapsed = Duration.ofNanos(System.nanoTime() - started)
                    metrics.call(name, op, req.method.name, e.kind.name, e.status, attempt, elapsed)
                    log.warn("{} {} failed after {} attempt(s) in {}ms: {} retryable={}", name, op, attempt, elapsed.toMillis(), e.message, e.retryable)
                    throw e
                }
                metrics.retry(name, op, e.kind.name)
                log.info("{} {} attempt {}/{} failed ({}), retrying in {}ms", name, op, attempt, maxAttempts, e.kind, delay.toMillis())
                try {
                    sleeper(delay)
                } catch (ie: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw dev.viren.extapi.error.InterruptedCallException(name, op, ie)
                }
            }
        }
    }

    private fun retryDelay(e: ExternalApiException, attempt: Int, maxAttempts: Int, deadline: Long): Duration? {
        if (!e.retryable || attempt >= maxAttempts) return null
        if (retryPolicy.retryAfterTooLong(e.retryAfter)) return null
        val delay = retryPolicy.backoff(attempt, e.retryAfter)
        // Leave room for the next attempt itself; sleeping into the deadline is pointless.
        return if (System.nanoTime() + delay.toNanos() >= deadline) null else delay
    }

    private fun <T> attemptOnce(
        req: ApiRequest, body: EncodedBody?, ctx: CallContext, type: JavaType,
        deadline: Long, attempt: Int, correlationId: String,
    ): ApiResponse<T> {
        val op = ctx.operation
        try {
            rateLimits.acquire(op, deadline)
        } catch (e: ExternalApiException) {
            metrics.rejected(name, op, e.kind.name)
            throw e
        }
        if (bulkhead != null && !bulkhead.tryEnter()) {
            metrics.rejected(name, op, FailureKind.BULKHEAD_FULL.name)
            throw BulkheadFullException(name, op, bulkhead.maxConcurrent)
        }
        try {
            val breaker = breakerFor(op)
            if (breaker != null && !breaker.tryAcquirePermission()) {
                metrics.rejected(name, op, FailureKind.CIRCUIT_OPEN.name)
                throw CircuitOpenException(name, op, breaker.name, breaker.remainingOpen())
            }

            val t0 = System.nanoTime()
            var raw: RawResponse? = null
            RecordingTlsStrategy.clear()
            val error: ExternalApiException? = try {
                raw = send(req, body, attempt, deadline, correlationId)
                classifier.classifyStatus(ctx, raw.status, raw.headers, raw.text)
            } catch (t: Throwable) {
                // A later address's "connection refused" must not hide an earlier TLS failure.
                val tls = RecordingTlsStrategy.consume()
                val cause = if (tls != null && t !is javax.net.ssl.SSLException && t is java.io.IOException) tls.also { it.addSuppressed(t) } else t
                classifier.classifyException(ctx, cause)
            } finally {
                RecordingTlsStrategy.clear()
            }
            val duration = Duration.ofNanos(System.nanoTime() - t0)

            if (breaker != null) when {
                error == null -> breaker.onSuccess(duration)
                error.countsAsFailure -> breaker.onError(duration)
                error.status != null -> breaker.onSuccess(duration) // upstream answered properly (e.g. 404)
                else -> breaker.onIgnored()
            }
            if (error?.kind == FailureKind.UPSTREAM_RATE_LIMITED) error.retryAfter?.let(rateLimits::tripCooldown)

            logger.response(log, name, req, attempt, raw, error, duration, correlationId)
            val event = AttemptEvent(name, req, attempt, raw?.status, error, duration, correlationId)
            myInterceptors.forEach { runCatching { it.afterAttempt(event) }.onFailure { log.warn("interceptor failed", it) } }

            if (error != null) throw error
            val response = raw!!
            return ApiResponse(response.status, response.headers, decode(response, type, op), response.text, attempt, duration, correlationId)
        } finally {
            bulkhead?.exit()
        }
    }

    private fun breakerFor(op: String): CircuitBreaker? {
        val cb = props.circuitBreaker
        if (!cb.enabled) return null
        return breakers.get(if (cb.perOperation) "$name:$op" else name, cb)
    }

    private fun send(req: ApiRequest, body: EncodedBody?, attempt: Int, deadline: Long, correlationId: String): RawResponse {
        val remainingNanos = deadline - System.nanoTime()
        if (remainingNanos <= 0) throw DeadlineExceededException(name, req.operationName, "total timeout ${props.timeouts.total} exceeded")
        val remaining = Duration.ofNanos(remainingNanos)

        val http = HttpUriRequestBase(req.method.name, resolveUri(req))
        val headers = linkedMapOf<String, String>()
        headers["Accept"] = "application/json"
        headers.putAll(props.defaultHeaders)
        headers.putAll(auth.headers())
        headers.putAll(req.headers)
        headers[CORRELATION_HEADER] = correlationId
        myInterceptors.forEach { it.beforeSend(name, req, attempt, headers) }
        headers.forEach(http::setHeader)
        body?.let { http.entity = ByteArrayEntity(it.bytes, ContentType.parse(it.contentType)) }

        val responseTimeout = minOf(req.timeout ?: props.timeouts.response, remaining)
        http.config = RequestConfig.custom()
            .setResponseTimeout(Timeout.of(responseTimeout))
            .setConnectionRequestTimeout(Timeout.of(minOf(props.timeouts.connectionRequest, remaining)))
            .setRedirectsEnabled(props.followRedirects && req.method.idempotent)
            .build()

        logger.request(log, name, req, attempt, http.uri, headers, body, correlationId)
        return pooled.http.execute(http) { response -> read(response, req) }
    }

    private fun read(response: ClassicHttpResponse, req: ApiRequest): RawResponse {
        val headers = response.headers.groupBy({ it.name }, { it.value })
        val entity = response.entity ?: return RawResponse(response.code, headers, null, null)
        val limit = props.maxResponseBytes
        if (entity.contentLength > limit) throw ResponseTooLargeException(name, req.operationName, limit, response.code)
        val bytes = entity.content?.use { readBounded(it, limit, response.code, req) }
        val charset = runCatching { ContentType.parseLenient(entity.contentType)?.charset }.getOrNull() ?: Charsets.UTF_8
        return RawResponse(response.code, headers, bytes, charset)
    }

    private fun readBounded(input: InputStream, limit: Long, status: Int, req: ApiRequest): ByteArray {
        val out = ByteArrayOutputStream()
        val buffer = ByteArray(8192)
        var total = 0L
        while (true) {
            val n = input.read(buffer)
            if (n < 0) break
            total += n
            if (total > limit) throw ResponseTooLargeException(name, req.operationName, limit, status)
            out.write(buffer, 0, n)
        }
        return out.toByteArray()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> decode(raw: RawResponse, type: JavaType, op: String): T? {
        val cls = type.rawClass
        if (cls == Unit::class.java || cls == Void::class.java || cls == Void.TYPE) return null
        if (cls == ByteArray::class.java) return raw.bytes as T?
        val text = raw.text
        if (text.isNullOrBlank()) return null
        if (cls == String::class.java) return text as T
        return try {
            mapper.readValue(text, type)
        } catch (e: Exception) {
            throw SerializationFailureException(name, op, "cannot decode ${raw.status} response as ${type.toCanonical()}: ${e.message}", e, raw.status, text.take(2000))
        }
    }

    private fun resolveUri(req: ApiRequest): URI {
        var path = req.path
        req.pathParams.forEach { (k, v) -> path = path.replace("{$k}", URLEncoder.encode(v, Charsets.UTF_8).replace("+", "%20")) }
        val full = if (path.startsWith("http://") || path.startsWith("https://")) path
        else props.baseUrl.trimEnd('/') + "/" + path.trimStart('/')
        val builder = URIBuilder(full)
        req.query.forEach { (k, values) -> values.forEach { builder.addParameter(k, it) } }
        return builder.build()
    }

    private fun encodeBody(req: ApiRequest): EncodedBody? {
        val body = req.body ?: return null
        val ct = req.contentType
        return when {
            body is ByteArray -> EncodedBody(body, ct)
            body is String && !ct.startsWith(ApiRequest.JSON) -> EncodedBody(body.toByteArray(), ct)
            ct.startsWith(ApiRequest.FORM) && body is Map<*, *> -> EncodedBody(
                body.entries.joinToString("&") { (k, v) ->
                    URLEncoder.encode(k.toString(), Charsets.UTF_8) + "=" + URLEncoder.encode(v?.toString().orEmpty(), Charsets.UTF_8)
                }.toByteArray(),
                ct,
            )
            body is String -> EncodedBody(body.toByteArray(), ct) // pre-serialized JSON
            else -> EncodedBody(mapper.writeValueAsBytes(body), ct)
        }
    }

    private fun withIdempotencyKey(r: ApiRequest): ApiRequest {
        if (r.method.idempotent) return r
        val header = props.retry.idempotencyHeader
        val key = r.idempotencyKey
            ?: r.headers.entries.firstOrNull { it.key.equals(header, true) }?.value
            ?: if (props.retry.autoIdempotencyKey) UUID.randomUUID().toString() else return r
        return r.copy(idempotencyKey = key, headers = r.headers + (header to key))
    }

    private fun safeToRepeat(r: ApiRequest) = r.method.idempotent || r.idempotencyKey != null || props.retry.retryNonIdempotent

    override fun close() = pooled.close()

    companion object {
        const val CORRELATION_HEADER = "X-Correlation-Id"
    }
}

internal class EncodedBody(val bytes: ByteArray, val contentType: String)

internal class RawResponse(val status: Int, val headers: Map<String, List<String>>, val bytes: ByteArray?, charset: Charset?) {
    val text: String? by lazy { bytes?.toString(charset ?: Charsets.UTF_8) }
}
