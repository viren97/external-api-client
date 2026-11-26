package dev.viren.extapi.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.viren.extapi.auth.AuthProvider
import dev.viren.extapi.error.DefaultFailureClassifier
import dev.viren.extapi.error.FailureClassifier
import dev.viren.extapi.http.ApiCallInterceptor
import dev.viren.extapi.http.ExternalApiClientCustomizer
import dev.viren.extapi.http.ExternalApiClientRegistry
import dev.viren.extapi.http.HttpClientFactory
import dev.viren.extapi.http.TlsContextFactory
import dev.viren.extapi.observability.ApiMetrics
import dev.viren.extapi.observability.CircuitBreakerHealthIndicator
import dev.viren.extapi.observability.MicrometerApiMetrics
import dev.viren.extapi.ratelimit.LocalRateLimiterFactory
import dev.viren.extapi.ratelimit.LocalUpstreamCooldown
import dev.viren.extapi.ratelimit.RateLimiterFactory
import dev.viren.extapi.ratelimit.RedisRateLimiterFactory
import dev.viren.extapi.ratelimit.RedisUpstreamCooldown
import dev.viren.extapi.ratelimit.UpstreamCooldown
import dev.viren.extapi.resilience.CircuitBreakerRegistry
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.ssl.SslBundles
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ResourceLoader
import org.springframework.data.redis.core.StringRedisTemplate

@AutoConfiguration(
    afterName = [
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration",
        "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration",
        "org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
    ],
)
@ConditionalOnProperty(prefix = "ext-api", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(ExtApiProperties::class)
class ExtApiAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun extApiFailureClassifier(): FailureClassifier = DefaultFailureClassifier()

    @Bean
    @ConditionalOnMissingBean
    fun extApiCircuitBreakerRegistry(): CircuitBreakerRegistry = CircuitBreakerRegistry()

    @Bean
    @ConditionalOnMissingBean
    fun extApiTlsContextFactory(resourceLoader: ResourceLoader, sslBundles: ObjectProvider<SslBundles>) =
        TlsContextFactory(resourceLoader, sslBundles.ifAvailable)

    @Bean
    @ConditionalOnMissingBean
    fun extApiHttpClientFactory(tls: TlsContextFactory) = HttpClientFactory(tls)

    @Bean
    @ConditionalOnMissingBean(name = ["extApiLocalRateLimiterFactory"])
    fun extApiLocalRateLimiterFactory(): RateLimiterFactory = LocalRateLimiterFactory()

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    fun externalApiClientRegistry(
        props: ExtApiProperties,
        httpFactory: HttpClientFactory,
        mapper: ObjectProvider<ObjectMapper>,
        classifier: FailureClassifier,
        breakers: CircuitBreakerRegistry,
        @Qualifier("extApiLocalRateLimiterFactory") localLimiters: RateLimiterFactory,
        @Qualifier("extApiRedisRateLimiterFactory") redisLimiters: ObjectProvider<RateLimiterFactory>,
        @Qualifier("extApiSharedCooldown") sharedCooldown: ObjectProvider<UpstreamCooldown>,
        interceptors: ObjectProvider<ApiCallInterceptor>,
        customizers: ObjectProvider<ExternalApiClientCustomizer>,
        context: org.springframework.context.ApplicationContext,
        metrics: ObjectProvider<ApiMetrics>,
    ): ExternalApiClientRegistry {
        val clients = LinkedHashMap(props.clients)
        // The webhook sender needs a client even when the user did not configure one.
        if (props.async.enabled) clients.putIfAbsent(props.webhooks.outbound.clientName, defaultWebhookClient())
        return ExternalApiClientRegistry(
            configured = clients,
            httpFactory = httpFactory,
            mapper = mapper.ifAvailable ?: defaultMapper(),
            classifier = classifier,
            circuitBreakers = breakers,
            localLimiters = localLimiters,
            redisLimiters = redisLimiters.ifAvailable,
            localCooldown = LocalUpstreamCooldown(),
            sharedCooldown = sharedCooldown.ifAvailable,
            interceptors = interceptors.orderedStream().toList(),
            customizers = customizers.orderedStream().toList(),
            authBeans = context.getBeansOfType(AuthProvider::class.java),
            metrics = metrics.ifAvailable ?: ApiMetrics.Noop,
        )
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(StringRedisTemplate::class)
    @ConditionalOnBean(StringRedisTemplate::class)
    class RedisConfiguration {
        @Bean
        @ConditionalOnMissingBean(name = ["extApiRedisRateLimiterFactory"])
        fun extApiRedisRateLimiterFactory(redis: StringRedisTemplate, props: ExtApiProperties): RateLimiterFactory =
            RedisRateLimiterFactory(redis, props.redis.keyPrefix)

        @Bean
        @ConditionalOnMissingBean(name = ["extApiSharedCooldown"])
        fun extApiSharedCooldown(redis: StringRedisTemplate, props: ExtApiProperties): UpstreamCooldown =
            RedisUpstreamCooldown(redis, props.redis.keyPrefix)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry::class)
    class MetricsConfiguration {
        @Bean
        @ConditionalOnMissingBean(ApiMetrics::class)
        @ConditionalOnBean(MeterRegistry::class)
        fun extApiMetrics(registry: MeterRegistry, breakers: CircuitBreakerRegistry): ApiMetrics =
            MicrometerApiMetrics(registry).also { m -> m.bindCircuitBreakers(breakers) { breakers.all().keys } }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["org.springframework.boot.actuate.health.HealthIndicator"])
    class HealthConfiguration {
        @Bean("extApiHealthIndicator")
        @ConditionalOnMissingBean(name = ["extApiHealthIndicator"])
        fun extApiHealthIndicator(breakers: CircuitBreakerRegistry) = CircuitBreakerHealthIndicator(breakers)
    }

    companion object {
        fun defaultMapper(): ObjectMapper = ObjectMapper()
            .registerKotlinModule()
            .registerModule(JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        fun defaultWebhookClient() = ClientProperties().apply {
            retry.maxAttempts = 2 // the outbox owns long-horizon retries
            circuitBreaker.perOperation = true // one bad subscriber must not block the rest
            timeouts.response = java.time.Duration.ofSeconds(10)
        }
    }
}
