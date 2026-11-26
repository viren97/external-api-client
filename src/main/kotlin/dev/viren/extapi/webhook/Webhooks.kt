package dev.viren.extapi.webhook

import dev.viren.extapi.config.WebhookProperties
import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.outbox.AsyncApiDispatcher
import dev.viren.extapi.outbox.AsyncOptions
import dev.viren.extapi.outbox.OutboxKind
import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.security.MessageDigest
import java.time.Clock
import java.time.Instant
import java.util.HexFormat
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Signature scheme (same on both sides):
 *
 * ```
 * X-Webhook-Id:        <unique event id>
 * X-Webhook-Timestamp: <unix seconds>
 * X-Webhook-Signature: v1=<hex HMAC-SHA256(secret, "<id>.<timestamp>.<raw body>")>
 * ```
 * Binding id and timestamp into the MAC stops replay of an old body under a new timestamp.
 */
object WebhookSignatures {
    const val VERSION = "v1"

    fun sign(secret: String, id: String, timestamp: Long, body: ByteArray): String {
        val mac = Mac.getInstance("HmacSHA256").apply { init(SecretKeySpec(secret.toByteArray(), "HmacSHA256")) }
        mac.update("$id.$timestamp.".toByteArray())
        return "$VERSION=" + HexFormat.of().formatHex(mac.doFinal(body))
    }

    /** Constant-time; accepts a header with several space/comma separated signatures (during rotation). */
    fun matches(header: String, secret: String, id: String, timestamp: Long, body: ByteArray): Boolean {
        val expected = sign(secret, id, timestamp, body).toByteArray()
        return header.split(' ', ',').map { it.trim() }.filter { it.isNotEmpty() }
            .any { MessageDigest.isEqual(it.toByteArray(), expected) }
    }
}

/** Picks the signing secret per destination, so each subscriber can have its own. */
fun interface WebhookSecretResolver {
    fun secretFor(url: String): String?
}

/** Signs outbound webhook requests at delivery time, so retries hours later carry a fresh timestamp. */
class WebhookSigner(
    private val props: WebhookProperties.Outbound,
    private val mapper: ObjectMapper,
    private val secrets: WebhookSecretResolver,
    private val clock: Clock = Clock.systemUTC(),
) {
    fun sign(request: ApiRequest, deliveryId: String): ApiRequest {
        val secret = secrets.secretFor(request.path) ?: return request
        val body = when (val b = request.body) {
            null -> ByteArray(0)
            is String -> b.toByteArray()
            is ByteArray -> b
            else -> mapper.writeValueAsBytes(b)
        }
        val ts = clock.instant().epochSecond
        val eventId = request.headers[props.idHeader] ?: deliveryId
        return request.copy(
            body = body.decodeToString(), // freeze the exact bytes that were signed
            headers = request.headers + mapOf(
                props.idHeader to eventId,
                props.timestampHeader to ts.toString(),
                props.signatureHeader to WebhookSignatures.sign(secret, eventId, ts, body),
            ),
        )
    }
}

data class WebhookEvent<T>(val id: String, val type: String, val createdAt: Instant, val data: T)

/**
 * Sends webhooks to subscribers through the outbox, so deliveries survive restarts, are retried
 * with backoff for hours, and are only sent if the surrounding transaction commits.
 * Each destination host gets its own circuit breaker, so one dead subscriber does not block others.
 */
class WebhookPublisher(
    private val dispatcher: AsyncApiDispatcher,
    private val props: WebhookProperties.Outbound,
    private val clock: Clock = Clock.systemUTC(),
) {
    /** @return the outbox id of the delivery. */
    fun publish(url: String, eventType: String, data: Any?, dedupeKey: String? = null, eventId: String = UUID.randomUUID().toString()): String {
        require(url.startsWith("https://") || url.startsWith("http://")) { "webhook url must be absolute: $url" }
        val event = WebhookEvent(eventId, eventType, clock.instant(), data)
        val request = ApiRequest.post(url, event, operation = "webhook:" + (URI(url).host ?: "unknown"))
            .withHeader(props.idHeader, eventId)
            .withHeader(props.eventTypeHeader, eventType)
        return dispatcher.insert(OutboxKind.WEBHOOK, props.clientName, request, AsyncOptions(dedupeKey = dedupeKey), props.maxAttempts)
    }
}
