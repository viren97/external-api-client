package dev.viren.extapi.http

import dev.viren.extapi.config.LoggingProperties
import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.error.ExternalApiException
import org.slf4j.Logger
import java.net.URI
import java.time.Duration

/** Request/response logging with secrets masked in headers and JSON bodies. */
internal class HttpLogFormatter(private val props: LoggingProperties) {
    private val masked = props.maskedHeaders.map { it.lowercase() }.toSet()
    private val fieldPattern = props.maskedFields.takeIf { it.isNotEmpty() }?.let { fields ->
        Regex("(\"(?:${fields.joinToString("|") { Regex.escape(it) }})\"\\s*:\\s*)(\"(?:[^\"\\\\]|\\\\.)*\"|[-0-9.]+)", RegexOption.IGNORE_CASE)
    }

    fun request(log: Logger, client: String, req: ApiRequest, attempt: Int, uri: URI, headers: Map<String, String>, body: EncodedBody?, cid: String) {
        if (!props.enabled || !log.isDebugEnabled) return
        val safeHeaders = headers.mapValues { (k, v) -> if (k.lowercase() in masked) "***" else v }
        val bodyText = if (props.logBodies && body != null) " body=" + maskBody(body.bytes.decodeToString()) else ""
        log.debug("--> {} {} {} attempt={} cid={} headers={}{}", client, req.method, stripQuerySecrets(uri), attempt, cid, safeHeaders, bodyText)
    }

    fun response(log: Logger, client: String, req: ApiRequest, attempt: Int, raw: RawResponse?, error: ExternalApiException?, took: Duration, cid: String) {
        if (!props.enabled) return
        if (error == null && !log.isDebugEnabled) return
        val bodyText = if (props.logBodies && raw?.text != null) " body=" + maskBody(raw.text!!) else ""
        val status = raw?.status?.toString() ?: "-"
        if (error == null) {
            log.debug("<-- {} {} {} {}ms attempt={} cid={}{}", client, req.operationName, status, took.toMillis(), attempt, cid, bodyText)
        } else if (log.isInfoEnabled) {
            log.info("<-- {} {} {} {}ms attempt={} cid={} failure={} retryable={}{}", client, req.operationName, status, took.toMillis(), attempt, cid, error.kind, error.retryable, bodyText)
        }
    }

    fun maskBody(text: String): String {
        val masked = fieldPattern?.replace(text) { it.groupValues[1] + "\"***\"" } ?: text
        return if (masked.length > props.maxBodyChars) masked.take(props.maxBodyChars) + "...(${masked.length} chars)" else masked
    }

    private fun stripQuerySecrets(uri: URI): String {
        val q = uri.rawQuery ?: return uri.toString()
        val cleaned = q.split('&').joinToString("&") { p ->
            val k = p.substringBefore('=')
            if (props.maskedFields.any { it.equals(k, true) } || k.contains("key", true) || k.contains("token", true)) "$k=***" else p
        }
        return uri.toString().substringBefore('?') + "?" + cleaned
    }
}
