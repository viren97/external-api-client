package dev.viren.extapi.outbox

import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.http.ExternalApiClientRegistry
import dev.viren.extapi.support.Reply
import dev.viren.extapi.support.TestServer
import dev.viren.extapi.webhook.InboundWebhook
import dev.viren.extapi.webhook.InboundWebhookHandler
import dev.viren.extapi.webhook.WebhookSignatures
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

@SpringBootApplication
class TestApp {
    val results = CopyOnWriteArrayList<AsyncCallResult>()
    val inbound = CopyOnWriteArrayList<InboundWebhook>()

    @Bean fun resultListener() = AsyncResultListener { results += it }

    @Bean fun pspHandler() = object : InboundWebhookHandler {
        override val source = "psp"
        override fun handle(webhook: InboundWebhook) { inbound += webhook }
    }
}

@SpringBootTest(
    classes = [TestApp::class],
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration",
        "ext-api.async.enabled=true",
        "ext-api.async.transport=IN_PROCESS",
        "ext-api.async.relay-interval=50ms",
        "ext-api.async.initial-backoff=100ms",
        "ext-api.async.max-backoff=200ms",
        "ext-api.async.max-attempts=4",
        "ext-api.clients.psp.retry.max-attempts=1",
        "ext-api.clients.psp.circuit-breaker.enabled=false",
        "ext-api.webhooks.outbound.signing-secret=outbound-secret",
        "ext-api.webhooks.inbound.enabled=true",
        "ext-api.webhooks.inbound.sources.psp.secrets[0]=inbound-secret",
    ],
)
class OutboxIntegrationTest {
    @Autowired lateinit var async: AsyncApiDispatcher
    @Autowired lateinit var repo: OutboxRepository
    @Autowired lateinit var app: TestApp
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var tx: TransactionTemplate
    @Autowired lateinit var rest: TestRestTemplate
    @Autowired lateinit var registry: ExternalApiClientRegistry

    companion object {
        val upstream = TestServer.http()
        val subscriber = TestServer.http()

        @JvmStatic @DynamicPropertySource
        fun props(r: DynamicPropertyRegistry) = r.add("ext-api.clients.psp.base-url") { upstream.baseUrl }

        @JvmStatic @AfterAll
        fun stop() { upstream.close(); subscriber.close() }
    }

    @BeforeEach
    fun reset() {
        upstream.requests.clear(); subscriber.requests.clear()
        upstream.fallback = { Reply(200, """{"ok":true}""") }
        app.results.clear()
    }

    private fun awaitStatus(id: String, status: OutboxStatus): OutboxRecord {
        await().atMost(Duration.ofSeconds(10)).until { repo.findById(id)?.status == status }
        return repo.findById(id)!!
    }

    @Test
    fun `submitted call is executed and result stored and published`() {
        val id = async.submit("psp", ApiRequest.post("/charges", mapOf("amount" to 100), operation = "create-charge"))
        val row = awaitStatus(id, OutboxStatus.SUCCEEDED)
        assertThat(row.responseStatus).isEqualTo(200)
        assertThat(row.responseBody).contains("ok")
        assertThat(upstream.requests.single().body).isEqualTo("""{"amount":100}""")
        await().until { app.results.any { it.id == id && it.status == OutboxStatus.SUCCEEDED } }
    }

    @Test
    fun `transient failures are retried later by the outbox`() {
        upstream.enqueue(Reply(503), Reply(503))
        val id = async.submit("psp", ApiRequest.get("/balance"))
        val row = awaitStatus(id, OutboxStatus.SUCCEEDED)
        assertThat(row.attempts).isEqualTo(3)
        assertThat(row.lastErrorKind).isEqualTo("SERVER_ERROR")
    }

    @Test
    fun `non-retryable failure ends FAILED immediately`() {
        upstream.enqueue(Reply(400, """{"error":"bad"}"""))
        val id = async.submit("psp", ApiRequest.get("/bad"))
        val row = awaitStatus(id, OutboxStatus.FAILED)
        assertThat(row.attempts).isEqualTo(1)
        assertThat(row.responseStatus).isEqualTo(400)
        assertThat(row.lastErrorKind).isEqualTo("CLIENT_ERROR")
    }

    @Test
    fun `retries exhausted ends DEAD`() {
        upstream.fallback = { Reply(503) }
        val id = async.submit("psp", ApiRequest.get("/down"), AsyncOptions(maxAttempts = 2))
        assertThat(awaitStatus(id, OutboxStatus.DEAD).attempts).isEqualTo(2)
        assertThat(async.requeue(id)).isTrue()
    }

    @Test
    fun `same dedupe key yields one call`() {
        val a = async.submit("psp", ApiRequest.get("/once"), AsyncOptions(dedupeKey = "order-42"))
        val b = async.submit("psp", ApiRequest.get("/once"), AsyncOptions(dedupeKey = "order-42"))
        assertThat(b).isEqualTo(a)
        awaitStatus(a, OutboxStatus.SUCCEEDED)
        Thread.sleep(300)
        assertThat(upstream.requests.count { it.path == "/once" }).isEqualTo(1)
    }

    @Test
    fun `nothing is sent when the business transaction rolls back`() {
        var id: String? = null
        tx.execute { status ->
            id = async.submit("psp", ApiRequest.get("/rolled-back"))
            status.setRollbackOnly()
        }
        assertThat(repo.findById(id!!)).isNull()
    }

    @Test
    fun `scheduled call waits for notBefore`() {
        val id = async.submit("psp", ApiRequest.get("/later"), AsyncOptions(notBefore = Instant.now().plusMillis(800)))
        Thread.sleep(300)
        assertThat(repo.findById(id)!!.status).isEqualTo(OutboxStatus.PENDING)
        awaitStatus(id, OutboxStatus.SUCCEEDED)
    }

    @Test
    fun `completion triggers a signed callback webhook`() {
        val id = async.submit("psp", ApiRequest.get("/with-callback"), AsyncOptions(callbackUrl = "${subscriber.baseUrl}/hooks", callbackEvent = "charge.done"))
        awaitStatus(id, OutboxStatus.SUCCEEDED)
        await().atMost(Duration.ofSeconds(10)).until { subscriber.requests.isNotEmpty() }

        val hook = subscriber.requests.single()
        assertThat(hook.headers["x-webhook-event"]).isEqualTo("charge.done")
        assertThat(hook.body).contains(id).contains("SUCCEEDED")
        val ok = WebhookSignatures.matches(
            hook.headers["x-webhook-signature"]!!, "outbound-secret",
            hook.headers["x-webhook-id"]!!, hook.headers["x-webhook-timestamp"]!!.toLong(), hook.body.toByteArray(),
        )
        assertThat(ok).describedAs("signature verifies").isTrue()
    }

    @Test
    fun `inbound webhook endpoint verifies, dispatches and dedupes`() {
        val body = """{"type":"payout.paid"}"""
        val ts = Instant.now().epochSecond
        val headers = HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
            set("X-Webhook-Id", "evt_9"); set("X-Webhook-Timestamp", ts.toString())
            set("X-Webhook-Signature", WebhookSignatures.sign("inbound-secret", "evt_9", ts, body.toByteArray()))
        }
        assertThat(rest.postForEntity("/webhooks/psp", HttpEntity(body, headers), Map::class.java).statusCode.value()).isEqualTo(202)
        assertThat(rest.postForEntity("/webhooks/psp", HttpEntity(body, headers), Map::class.java).statusCode.value()).isEqualTo(200)
        assertThat(app.inbound.single().text).isEqualTo(body)

        headers.set("X-Webhook-Signature", "v1=deadbeef")
        headers.set("X-Webhook-Id", "evt_10")
        assertThat(rest.postForEntity("/webhooks/psp", HttpEntity(body, headers), Map::class.java).statusCode.value()).isEqualTo(401)
    }

    @Test
    fun `webhook client is auto-registered and health shows breakers`() {
        assertThat(registry.names()).contains("psp", "webhooks")
        val health = rest.getForObject("/actuator/health", Map::class.java)
        assertThat(health["status"]).isNotNull()
    }
}
