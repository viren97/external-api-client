package dev.viren.extapi.core

import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Duration

enum class HttpMethod {
    GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS;

    /** RFC 9110 idempotent methods: repeating them has the same effect as sending once. */
    @get:JsonIgnore
    val idempotent: Boolean get() = this != POST && this != PATCH
}

/**
 * Transport-neutral description of one call. It is a plain data class so it can be stored
 * in the outbox and replayed later by an async worker.
 *
 * @param path relative to the client's base URL, or an absolute `http(s)://` URL.
 * @param operation logical name used for metrics, per-operation rate limits and circuit
 *   breakers. Keep it low-cardinality (`get-account`, not `/accounts/123`).
 * @param body serialized as JSON unless [contentType] says otherwise. `String`/`ByteArray`
 *   are sent as-is; a `Map` with form content type is URL-encoded.
 */
data class ApiRequest(
    val method: HttpMethod,
    val path: String,
    /** Values for `{name}` placeholders in [path]; each is URL-encoded. */
    val pathParams: Map<String, String> = emptyMap(),
    val query: Map<String, List<String>> = emptyMap(),
    val headers: Map<String, String> = emptyMap(),
    val body: Any? = null,
    val contentType: String = JSON,
    val operation: String? = null,
    val idempotencyKey: String? = null,
    /** Overrides the client's response timeout for this call only. */
    val timeout: Duration? = null,
    /** Overrides the client's retry max attempts for this call only. */
    val maxAttempts: Int? = null,
) {
    /**
     * Name used for metrics and per-operation limits. Without an explicit [operation] it is the
     * method plus the path *template* when [pathParams] are used (`GET /accounts/{id}`), else just
     * the method, so raw ids never end up as metric tags.
     */
    @get:JsonIgnore
    val operationName: String get() = operation ?: if (pathParams.isNotEmpty()) "${method.name} ${path.substringBefore('?')}" else method.name

    fun withPathParam(name: String, value: Any) = copy(pathParams = pathParams + (name to value.toString()))

    fun withHeader(name: String, value: String) = copy(headers = headers + (name to value))
    fun withQuery(name: String, vararg values: String) = copy(query = query + (name to values.toList()))
    fun named(operation: String) = copy(operation = operation)

    companion object {
        const val JSON = "application/json"
        const val FORM = "application/x-www-form-urlencoded"

        fun get(path: String, operation: String? = null) = ApiRequest(HttpMethod.GET, path, operation = operation)
        fun delete(path: String, operation: String? = null) = ApiRequest(HttpMethod.DELETE, path, operation = operation)
        fun post(path: String, body: Any?, operation: String? = null, idempotencyKey: String? = null) =
            ApiRequest(HttpMethod.POST, path, body = body, operation = operation, idempotencyKey = idempotencyKey)
        fun put(path: String, body: Any?, operation: String? = null) =
            ApiRequest(HttpMethod.PUT, path, body = body, operation = operation)
        fun patch(path: String, body: Any?, operation: String? = null, idempotencyKey: String? = null) =
            ApiRequest(HttpMethod.PATCH, path, body = body, operation = operation, idempotencyKey = idempotencyKey)
    }
}

data class ApiResponse<T>(
    val status: Int,
    val headers: Map<String, List<String>>,
    val body: T?,
    val rawBody: String?,
    /** How many attempts it took, 1 when the first one succeeded. */
    val attempts: Int,
    val duration: Duration,
    val correlationId: String?,
) {
    fun header(name: String): String? =
        headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value?.firstOrNull()

    fun requireBody(): T = body ?: error("Response $status had no body")
}
