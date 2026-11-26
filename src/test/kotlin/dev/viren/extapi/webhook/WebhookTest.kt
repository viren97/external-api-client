package dev.viren.extapi.webhook

import dev.viren.extapi.config.WebhookProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class WebhookTest {
    private val now = Instant.parse("2026-11-26T10:00:00Z")
    private val clock = Clock.fixed(now, ZoneOffset.UTC)
    private val body = """{"id":"evt_1"}""".toByteArray()

    private fun verifier(vararg secrets: String) = InboundWebhookVerifier(
        WebhookProperties.Inbound().apply { sources["psp"] = WebhookProperties.Source().apply { this.secrets = secrets.toMutableList() } },
        LocalReplayGuard(clock), clock,
    )

    private fun headers(secret: String, id: String = "evt_1", ts: Long = now.epochSecond, payload: ByteArray = body) = mapOf(
        "X-Webhook-Id" to id,
        "X-Webhook-Timestamp" to ts.toString(),
        "X-Webhook-Signature" to WebhookSignatures.sign(secret, id, ts, payload),
    )

    @Test
    fun `valid signature is accepted once, then reported as duplicate`() {
        val v = verifier("s3cret")
        assertThat(v.verify("psp", headers("s3cret"), body)).isEqualTo(Verification.Valid("evt_1"))
        assertThat(v.verify("psp", headers("s3cret"), body)).isEqualTo(Verification.Duplicate("evt_1"))
    }

    @Test
    fun `released event can be processed again (handler failed)`() {
        val v = verifier("s3cret")
        v.verify("psp", headers("s3cret"), body)
        v.release("psp", "evt_1")
        assertThat(v.verify("psp", headers("s3cret"), body)).isInstanceOf(Verification.Valid::class.java)
    }

    @Test
    fun `tampered body, wrong secret, stale timestamp and unknown source are rejected`() {
        val v = verifier("s3cret")
        assertThat((v.verify("psp", headers("s3cret"), "{}".toByteArray()) as Verification.Rejected).status).isEqualTo(401)
        assertThat((v.verify("psp", headers("other"), body) as Verification.Rejected).status).isEqualTo(401)
        assertThat((v.verify("psp", headers("s3cret", ts = now.epochSecond - 3600), body) as Verification.Rejected).reason).contains("tolerance")
        assertThat((v.verify("nope", headers("s3cret"), body) as Verification.Rejected).status).isEqualTo(404)
        assertThat((v.verify("psp", emptyMap(), body) as Verification.Rejected).status).isEqualTo(400)
    }

    @Test
    fun `secret rotation accepts old and new secret`() {
        val v = verifier("new", "old")
        assertThat(v.verify("psp", headers("old", id = "a"), body)).isInstanceOf(Verification.Valid::class.java)
        assertThat(v.verify("psp", headers("new", id = "b"), body)).isInstanceOf(Verification.Valid::class.java)
    }
}

class WebhookReplayStoreDownTest {
    @org.junit.jupiter.api.Test
    fun `replay store outage asks the sender to retry`() {
        val down = object : ReplayGuard {
            override fun firstSeen(key: String, ttl: java.time.Duration): Boolean = throw IllegalStateException("redis down")
            override fun forget(key: String) = throw IllegalStateException("redis down")
        }
        val v = InboundWebhookVerifier(
            WebhookProperties.Inbound().apply { sources["psp"] = WebhookProperties.Source().apply { secrets = mutableListOf("s") } }, down,
        )
        val ts = java.time.Instant.now().epochSecond
        val h = mapOf("X-Webhook-Id" to "1", "X-Webhook-Timestamp" to "$ts", "X-Webhook-Signature" to WebhookSignatures.sign("s", "1", ts, ByteArray(0)))
        org.assertj.core.api.Assertions.assertThat((v.verify("psp", h, ByteArray(0)) as Verification.Rejected).status).isEqualTo(503)
    }
}
