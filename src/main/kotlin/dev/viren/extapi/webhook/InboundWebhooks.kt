package dev.viren.extapi.webhook

import dev.viren.extapi.config.WebhookProperties
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.abs

/** Remembers delivered event ids so a replayed or re-sent webhook is processed once. */
interface ReplayGuard {
    /** @return true if [key] was not seen within [ttl] (and records it). */
    fun firstSeen(key: String, ttl: Duration): Boolean
    fun forget(key: String)
}

class LocalReplayGuard(private val clock: Clock = Clock.systemUTC()) : ReplayGuard {
    private val seen = ConcurrentHashMap<String, Long>()

    override fun firstSeen(key: String, ttl: Duration): Boolean {
        val now = clock.millis()
        if (seen.size > 10_000) seen.entries.removeIf { it.value < now }
        val expiry = now + ttl.toMillis()
        var fresh = false
        seen.compute(key) { _, old -> if (old == null || old < now) { fresh = true; expiry } else old }
        return fresh
    }

    override fun forget(key: String) { seen.remove(key) }
}

class RedisReplayGuard(private val redis: StringRedisTemplate, private val prefix: String) : ReplayGuard {
    override fun firstSeen(key: String, ttl: Duration): Boolean =
        redis.opsForValue().setIfAbsent("$prefix:webhook:seen:$key", "1", ttl) == true

    override fun forget(key: String) { redis.delete("$prefix:webhook:seen:$key") }
}

sealed interface Verification {
    data class Valid(val eventId: String) : Verification
    data class Rejected(val status: Int, val reason: String) : Verification
    data class Duplicate(val eventId: String) : Verification
}

/** Verifies signature, freshness and uniqueness of an incoming webhook. */
class InboundWebhookVerifier(
    private val props: WebhookProperties.Inbound,
    private val replay: ReplayGuard,
    private val clock: Clock = Clock.systemUTC(),
) {
    fun verify(source: String, headers: Map<String, String>, body: ByteArray): Verification {
        val cfg = props.sources[source] ?: return Verification.Rejected(404, "unknown webhook source")
        fun h(name: String) = headers.entries.firstOrNull { it.key.equals(name, true) }?.value
        val id = h(cfg.idHeader) ?: return Verification.Rejected(400, "missing ${cfg.idHeader}")
        val ts = h(cfg.timestampHeader)?.toLongOrNull() ?: return Verification.Rejected(400, "missing or invalid ${cfg.timestampHeader}")
        val sig = h(cfg.signatureHeader) ?: return Verification.Rejected(401, "missing ${cfg.signatureHeader}")

        if (abs(clock.instant().epochSecond - ts) > props.tolerance.seconds) return Verification.Rejected(400, "timestamp outside tolerance")
        if (cfg.secrets.none { WebhookSignatures.matches(sig, it, id, ts, body) }) return Verification.Rejected(401, "invalid signature")
        val fresh = try {
            replay.firstSeen("$source:$id", props.replayTtl)
        } catch (e: RuntimeException) {
            // Cannot tell if it is a replay: ask the sender to retry rather than risk double processing.
            return Verification.Rejected(503, "replay store unavailable")
        }
        if (!fresh) return Verification.Duplicate(id)
        return Verification.Valid(id)
    }

    fun release(source: String, id: String) { runCatching { replay.forget("$source:$id") } }
}

data class InboundWebhook(val source: String, val eventId: String, val headers: Map<String, String>, val body: ByteArray) {
    val text: String get() = body.decodeToString()
}

/** Implement as a bean to receive verified webhooks from [source]. Throw to make the sender retry. */
interface InboundWebhookHandler {
    val source: String
    fun handle(webhook: InboundWebhook)
}

/**
 * `POST {ext-api.webhooks.inbound.path}/{source}`
 *
 * - 202 handled, 200 duplicate (acknowledged so the sender stops retrying)
 * - 400 malformed / stale, 401 bad signature, 404 unknown source
 * - 500 handler failed: the event is un-marked so the sender's retry is processed
 */
@RestController
class InboundWebhookController(
    private val verifier: InboundWebhookVerifier,
    handlers: List<InboundWebhookHandler>,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val bySource = handlers.groupBy { it.source }

    @PostMapping("\${ext-api.webhooks.inbound.path:/webhooks}/{source}")
    fun receive(@PathVariable source: String, @RequestHeader headers: Map<String, String>, @RequestBody(required = false) body: ByteArray?): ResponseEntity<Map<String, String>> {
        val payload = body ?: ByteArray(0)
        return when (val v = verifier.verify(source, headers, payload)) {
            is Verification.Rejected -> {
                log.warn("Rejected webhook from '{}': {}", source, v.reason)
                ResponseEntity.status(v.status).body(mapOf("error" to v.reason))
            }
            is Verification.Duplicate -> ResponseEntity.ok(mapOf("status" to "duplicate", "id" to v.eventId))
            is Verification.Valid -> try {
                bySource[source].orEmpty().forEach { it.handle(InboundWebhook(source, v.eventId, headers, payload)) }
                ResponseEntity.status(HttpStatus.ACCEPTED).body(mapOf("status" to "accepted", "id" to v.eventId))
            } catch (e: Exception) {
                verifier.release(source, v.eventId)
                log.error("Webhook handler for '{}' failed on {}", source, v.eventId, e)
                ResponseEntity.status(500).body(mapOf("error" to "handler failed", "id" to v.eventId))
            }
        }
    }
}
