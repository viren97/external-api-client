package dev.viren.extapi.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.MessageListener
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService

/** Moves "row is ready" notifications from the relay to workers. */
interface AsyncTransport {
    fun dispatch(envelope: OutboxEnvelope): CompletableFuture<*>
}

/** Publishes final outcomes (result topic, dead-letter topic). */
interface OutcomePublisher {
    fun publish(result: AsyncCallResult)

    object Noop : OutcomePublisher {
        override fun publish(result: AsyncCallResult) = Unit
    }
}

/** Kafka transport: any instance in the consumer group may execute the call. */
class KafkaAsyncTransport(
    private val kafka: KafkaTemplate<String, String>,
    private val mapper: ObjectMapper,
    private val topic: String,
) : AsyncTransport {
    override fun dispatch(envelope: OutboxEnvelope): CompletableFuture<*> =
        kafka.send(topic, envelope.key, mapper.writeValueAsString(envelope))
}

class KafkaOutcomePublisher(
    private val kafka: KafkaTemplate<String, String>,
    private val mapper: ObjectMapper,
    private val resultTopic: String,
    private val deadLetterTopic: String,
) : OutcomePublisher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun publish(result: AsyncCallResult) {
        val topic = if (result.status == OutboxStatus.SUCCEEDED) resultTopic else deadLetterTopic
        // Outcome is already committed in the DB; a failed publish is logged, not retried forever.
        kafka.send(topic, result.dedupeKey ?: result.id, mapper.writeValueAsString(result))
            .whenComplete { _, e -> if (e != null) log.warn("Could not publish outcome of {} to {}: {}", result.id, topic, e.message) }
    }
}

/** Kafka listener body: parses the envelope and hands the id to the worker. */
class KafkaRequestListener(
    private val worker: AsyncCallWorker,
    private val mapper: ObjectMapper,
) : MessageListener<String, String> {
    override fun onMessage(record: ConsumerRecord<String, String>) {
        val envelope = mapper.readValue(record.value(), OutboxEnvelope::class.java)
        worker.process(envelope.id)
    }
}

/** No broker: the relay runs calls on a local thread pool. For small deployments and tests. */
class InProcessAsyncTransport(
    private val executor: ExecutorService,
    private val worker: () -> AsyncCallWorker,
) : AsyncTransport {
    // Completes once the task is queued; the relay must not wait for the call itself.
    override fun dispatch(envelope: OutboxEnvelope): CompletableFuture<*> = try {
        executor.execute { worker().process(envelope.id) }
        CompletableFuture.completedFuture(null)
    } catch (e: java.util.concurrent.RejectedExecutionException) {
        CompletableFuture.failedFuture<Any>(e)
    }
}
