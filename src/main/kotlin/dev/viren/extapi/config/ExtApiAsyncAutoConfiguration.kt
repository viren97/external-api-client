package dev.viren.extapi.config

import com.fasterxml.jackson.databind.ObjectMapper
import dev.viren.extapi.http.ExternalApiClientRegistry
import dev.viren.extapi.observability.ApiMetrics
import dev.viren.extapi.outbox.AsyncApiDispatcher
import dev.viren.extapi.outbox.AsyncCallWorker
import dev.viren.extapi.outbox.AsyncResultListener
import dev.viren.extapi.outbox.AsyncTransport
import dev.viren.extapi.outbox.InProcessAsyncTransport
import dev.viren.extapi.outbox.KafkaAsyncTransport
import dev.viren.extapi.outbox.KafkaOutcomePublisher
import dev.viren.extapi.outbox.KafkaRequestListener
import dev.viren.extapi.outbox.OutboxRelay
import dev.viren.extapi.outbox.OutboxRepository
import dev.viren.extapi.outbox.OutcomePublisher
import dev.viren.extapi.webhook.InboundWebhookController
import dev.viren.extapi.webhook.InboundWebhookHandler
import dev.viren.extapi.webhook.InboundWebhookVerifier
import dev.viren.extapi.webhook.LocalReplayGuard
import dev.viren.extapi.webhook.RedisReplayGuard
import dev.viren.extapi.webhook.ReplayGuard
import dev.viren.extapi.webhook.WebhookPublisher
import dev.viren.extapi.webhook.WebhookSecretResolver
import dev.viren.extapi.webhook.WebhookSigner
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.ssl.SslBundles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.util.backoff.FixedBackOff
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * Async processing: transactional outbox (JDBC) + Kafka (or an in-process pool) + webhook delivery.
 * Enabled with `ext-api.async.enabled=true`; needs a DataSource, and spring-kafka for KAFKA transport.
 */
@AutoConfiguration(
    after = [ExtApiAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
        "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
    ],
)
@ConditionalOnClass(JdbcTemplate::class)
@ConditionalOnProperty(prefix = "ext-api.async", name = ["enabled"], havingValue = "true")
@ConditionalOnBean(ExternalApiClientRegistry::class)
class ExtApiAsyncAutoConfiguration {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    @ConditionalOnMissingBean
    fun extApiOutboxRepository(jdbc: JdbcTemplate, props: ExtApiProperties) =
        OutboxRepository(jdbc, props.async.tableName).also { if (props.async.initializeSchema) it.createSchemaIfMissing() }

    @Bean
    @ConditionalOnMissingBean
    fun asyncApiDispatcher(repo: OutboxRepository, mapper: ObjectProvider<ObjectMapper>, props: ExtApiProperties, clients: ExternalApiClientRegistry): AsyncApiDispatcher {
        val maxTotal = props.clients.values.maxOfOrNull { it.timeouts.total }
        if (maxTotal != null && maxTotal >= props.async.lease) {
            log.warn("ext-api.async.lease ({}) should be longer than the longest client timeouts.total ({}) or slow calls may run twice", props.async.lease, maxTotal)
        }
        return AsyncApiDispatcher(repo, mapper.ifAvailable ?: ExtApiAutoConfiguration.defaultMapper(), props.async, clients::properties)
    }

    @Bean
    @ConditionalOnMissingBean
    fun webhookSecretResolver(props: ExtApiProperties) = WebhookSecretResolver { props.webhooks.outbound.signingSecret }

    @Bean
    @ConditionalOnMissingBean
    fun webhookPublisher(dispatcher: AsyncApiDispatcher, props: ExtApiProperties) = WebhookPublisher(dispatcher, props.webhooks.outbound)

    @Bean
    @ConditionalOnMissingBean
    fun asyncCallWorker(
        repo: OutboxRepository,
        clients: ExternalApiClientRegistry,
        mapper: ObjectProvider<ObjectMapper>,
        props: ExtApiProperties,
        secrets: WebhookSecretResolver,
        webhooks: ObjectProvider<WebhookPublisher>,
        listeners: ObjectProvider<AsyncResultListener>,
        outcomes: ObjectProvider<OutcomePublisher>,
        txManager: ObjectProvider<PlatformTransactionManager>,
        metrics: ObjectProvider<ApiMetrics>,
    ): AsyncCallWorker {
        val m = mapper.ifAvailable ?: ExtApiAutoConfiguration.defaultMapper()
        return AsyncCallWorker(
            repository = repo, clients = clients, mapper = m, props = props.async,
            signer = WebhookSigner(props.webhooks.outbound, m, secrets),
            webhooks = { webhooks.ifAvailable },
            listeners = listeners.orderedStream().toList(),
            outcomes = outcomes.ifAvailable ?: OutcomePublisher.Noop,
            tx = txManager.ifAvailable?.let(::TransactionTemplate),
            metrics = metrics.ifAvailable ?: ApiMetrics.Noop,
        )
    }

    @Bean
    @ConditionalOnMissingBean
    fun extApiOutboxRelay(repo: OutboxRepository, transport: AsyncTransport, props: ExtApiProperties) = OutboxRelay(repo, transport, props.async)

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "ext-api.async", name = ["transport"], havingValue = "IN_PROCESS")
    class InProcessConfiguration {
        @Bean(destroyMethod = "shutdown")
        fun extApiAsyncExecutor(props: ExtApiProperties): ExecutorService = ThreadPoolExecutor(
            props.async.consumerConcurrency, props.async.consumerConcurrency, 60, TimeUnit.SECONDS,
            LinkedBlockingQueue(props.async.batchSize * 4),
        ) { r -> Thread(r, "ext-api-async-worker").apply { isDaemon = true } }

        @Bean
        @ConditionalOnMissingBean
        fun extApiInProcessTransport(executor: ExecutorService, worker: ObjectProvider<AsyncCallWorker>): AsyncTransport =
            InProcessAsyncTransport(executor) { worker.getObject() }
    }

    /** Dedicated String/String producer and consumer, isolated from the app's own Kafka serializers. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KafkaTemplate::class)
    @ConditionalOnProperty(prefix = "ext-api.async", name = ["transport"], havingValue = "KAFKA", matchIfMissing = true)
    class KafkaConfiguration {
        /** Not exposed as a KafkaTemplate bean, so it never clashes with the application's own template. */
        @Bean(destroyMethod = "destroy")
        fun extApiKafkaTemplateHolder(kafka: KafkaProperties, ssl: ObjectProvider<SslBundles>): KafkaTemplateHolder {
            val cfg = kafka.buildProducerProperties(ssl.ifAvailable).toMutableMap()
            cfg[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            cfg[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            cfg[ProducerConfig.ACKS_CONFIG] = "all"
            cfg[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true
            cfg.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
            return KafkaTemplateHolder(KafkaTemplate(DefaultKafkaProducerFactory<String, String>(cfg)))
        }

        @Bean
        @ConditionalOnMissingBean
        fun extApiKafkaTransport(template: KafkaTemplateHolder, mapper: ObjectProvider<ObjectMapper>, props: ExtApiProperties): AsyncTransport =
            KafkaAsyncTransport(template.template, mapper.ifAvailable ?: ExtApiAutoConfiguration.defaultMapper(), props.async.requestTopic)

        @Bean
        @ConditionalOnMissingBean
        fun extApiOutcomePublisher(template: KafkaTemplateHolder, mapper: ObjectProvider<ObjectMapper>, props: ExtApiProperties): OutcomePublisher =
            KafkaOutcomePublisher(template.template, mapper.ifAvailable ?: ExtApiAutoConfiguration.defaultMapper(), props.async.resultTopic, props.async.deadLetterTopic)

        @Bean
        fun extApiRequestListenerContainer(
            kafka: KafkaProperties,
            ssl: ObjectProvider<SslBundles>,
            worker: AsyncCallWorker,
            mapper: ObjectProvider<ObjectMapper>,
            props: ExtApiProperties,
        ): ConcurrentMessageListenerContainer<String, String> {
            val cfg = kafka.buildConsumerProperties(ssl.ifAvailable).toMutableMap()
            cfg[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            cfg[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            cfg[ConsumerConfig.GROUP_ID_CONFIG] = props.async.consumerGroup
            cfg.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            val containerProps = ContainerProperties(props.async.requestTopic).apply {
                messageListener = KafkaRequestListener(worker, mapper.ifAvailable ?: ExtApiAutoConfiguration.defaultMapper())
                ackMode = ContainerProperties.AckMode.RECORD
            }
            return ConcurrentMessageListenerContainer(DefaultKafkaConsumerFactory<String, String>(cfg), containerProps).apply {
                concurrency = props.async.consumerConcurrency
                // Worker failures are DB-level; after a few tries skip the message, the lease recovers the row.
                setCommonErrorHandler(DefaultErrorHandler(FixedBackOff(1000, 3)))
            }
        }
    }

    /** Wrapper so our template never competes with the application's own KafkaTemplate for injection. */
    class KafkaTemplateHolder(val template: KafkaTemplate<String, String>) {
        fun destroy() = template.producerFactory.reset()
    }
}

/** Inbound webhooks do not need the outbox, so they are configured separately. */
@AutoConfiguration(after = [ExtApiAutoConfiguration::class], afterName = ["org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration"])
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(name = ["org.springframework.web.bind.annotation.RestController"])
@ConditionalOnProperty(prefix = "ext-api.webhooks.inbound", name = ["enabled"], havingValue = "true")
class ExtApiInboundWebhookAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(StringRedisTemplate::class)
    @ConditionalOnBean(StringRedisTemplate::class)
    class RedisReplay {
        @Bean
        @ConditionalOnMissingBean
        fun extApiReplayGuard(redis: StringRedisTemplate, props: ExtApiProperties): ReplayGuard = RedisReplayGuard(redis, props.redis.keyPrefix)
    }

    @Bean
    @ConditionalOnMissingBean
    fun extApiLocalReplayGuard(): ReplayGuard = LocalReplayGuard()

    @Bean
    @ConditionalOnMissingBean
    fun inboundWebhookVerifier(props: ExtApiProperties, replay: ReplayGuard) = InboundWebhookVerifier(props.webhooks.inbound, replay)

    @Bean
    fun inboundWebhookController(verifier: InboundWebhookVerifier, handlers: ObjectProvider<InboundWebhookHandler>) =
        InboundWebhookController(verifier, handlers.orderedStream().toList())
}
