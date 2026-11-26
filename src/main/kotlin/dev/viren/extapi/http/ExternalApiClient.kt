package dev.viren.extapi.http

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JavaType
import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.core.ApiResponse
import dev.viren.extapi.error.ExternalApiException
import java.time.Duration

/**
 * Synchronous client for one named external API. Every call passes through:
 *
 * `deadline -> upstream cooldown -> rate limiters -> bulkhead -> circuit breaker -> HTTP -> classify -> retry`
 *
 * Failures always surface as a subtype of [ExternalApiException].
 */
interface ExternalApiClient {
    val name: String

    fun <T> execute(request: ApiRequest, type: JavaType): ApiResponse<T>
    fun <T> execute(request: ApiRequest, type: Class<T>): ApiResponse<T>
    fun <T> execute(request: ApiRequest, type: TypeReference<T>): ApiResponse<T>
}

inline fun <reified T> ExternalApiClient.call(request: ApiRequest): ApiResponse<T> =
    execute(request, object : TypeReference<T>() {})

inline fun <reified T> ExternalApiClient.get(path: String, operation: String? = null): T? =
    call<T>(ApiRequest.get(path, operation)).body

inline fun <reified T> ExternalApiClient.post(path: String, body: Any?, operation: String? = null, idempotencyKey: String? = null): T? =
    call<T>(ApiRequest.post(path, body, operation, idempotencyKey)).body

/** Returns [fallback]'s value instead of throwing, e.g. a cached quote when the pricing API is down. */
inline fun <reified T> ExternalApiClient.callOrElse(request: ApiRequest, fallback: (ExternalApiException) -> T): T =
    try { call<T>(request).body ?: fallback(NoBody(name, request.operationName)) } catch (e: ExternalApiException) { fallback(e) }

@PublishedApi
internal fun NoBody(client: String, op: String) =
    dev.viren.extapi.error.SerializationFailureException(client, op, "response had no body", null)

/** Hook into every attempt: add headers, record audit trails, emit traces. Register as a bean. */
interface ApiCallInterceptor {
    /** Applies to clients whose name matches; default is every client. */
    fun supports(client: String): Boolean = true
    fun beforeSend(client: String, request: ApiRequest, attempt: Int, headers: MutableMap<String, String>) {}
    fun afterAttempt(event: AttemptEvent) {}
}

data class AttemptEvent(
    val client: String,
    val request: ApiRequest,
    val attempt: Int,
    val status: Int?,
    val error: ExternalApiException?,
    val duration: Duration,
    val correlationId: String,
)
