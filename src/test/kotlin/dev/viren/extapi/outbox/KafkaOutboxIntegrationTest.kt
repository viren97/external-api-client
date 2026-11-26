package dev.viren.extapi.outbox

import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.support.Reply
import dev.viren.extapi.support.TestServer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration

@SpringBootTest(
    classes = [TestApp::class],
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "ext-api.async.enabled=true",
        "ext-api.async.transport=KAFKA",
        "ext-api.async.relay-interval=50ms",
        "ext-api.async.initial-backoff=100ms",
        "ext-api.async.max-backoff=200ms",
        "ext-api.clients.psp.retry.max-attempts=1",
        "ext-api.async.table-name=ext_api_outbox_kafka",
    ],
)
@EmbeddedKafka(partitions = 2, topics = ["ext-api.requests", "ext-api.results", "ext-api.requests.dlt"])
class KafkaOutboxIntegrationTest {
    @Autowired lateinit var async: AsyncApiDispatcher
    @Autowired lateinit var repo: OutboxRepository
    @Value("\${spring.embedded.kafka.brokers}") lateinit var brokers: String

    companion object {
        val upstream = TestServer.http()

        @JvmStatic @DynamicPropertySource
        fun props(r: DynamicPropertyRegistry) = r.add("ext-api.clients.psp.base-url") { upstream.baseUrl }

        @JvmStatic @AfterAll
        fun stop() = upstream.close()
    }

    @Test
    fun `calls flow relay to kafka to worker, outcomes land on result and DLT topics`() {
        upstream.enqueue(Reply(503)) // first attempt of the first call fails, outbox retries it
        val ok = (1..5).map { async.submit("psp", ApiRequest.get("/k/$it")) }
        upstream.fallback = { r -> if (r.path == "/k/bad") Reply(400) else Reply(200, """{"ok":true}""") }
        val bad = async.submit("psp", ApiRequest.get("/k/bad"))

        await().atMost(Duration.ofSeconds(30)).until {
            ok.all { repo.findById(it)?.status == OutboxStatus.SUCCEEDED } && repo.findById(bad)?.status == OutboxStatus.FAILED
        }
        assertThat(upstream.requests.count { it.path.startsWith("/k/") && it.path != "/k/bad" }).isEqualTo(6)

        val consumer = KafkaConsumer<String, String>(mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to brokers,
            ConsumerConfig.GROUP_ID_CONFIG to "verifier",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ))
        consumer.use { c ->
            c.subscribe(listOf("ext-api.results", "ext-api.requests.dlt"))
            val seen = mutableMapOf<String, MutableList<String>>()
            await().atMost(Duration.ofSeconds(20)).until {
                c.poll(Duration.ofMillis(200)).forEach { seen.getOrPut(it.topic()) { mutableListOf() } += it.value() }
                (seen["ext-api.results"]?.size ?: 0) >= 5 && (seen["ext-api.requests.dlt"]?.size ?: 0) >= 1
            }
            assertThat(seen["ext-api.requests.dlt"]!!.single()).contains(bad).contains("CLIENT_ERROR")
        }
    }
}
