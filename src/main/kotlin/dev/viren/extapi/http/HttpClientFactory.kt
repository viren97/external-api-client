package dev.viren.extapi.http

import dev.viren.extapi.config.ClientProperties
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.core5.http.io.SocketConfig
import org.apache.hc.core5.pool.PoolConcurrencyPolicy
import org.apache.hc.core5.pool.PoolReusePolicy
import org.apache.hc.core5.util.TimeValue
import org.apache.hc.core5.util.Timeout

/** The pooled Apache client behind one named external API, plus its pool for metrics. */
class PooledHttpClient(val http: CloseableHttpClient, val pool: PoolingHttpClientConnectionManager) : AutoCloseable {
    override fun close() = http.close()
}

/**
 * One connection pool per external system, so a slow provider cannot starve the others.
 * The library's own retry is disabled: retries happen in [DefaultExternalApiClient] where
 * they respect idempotency, rate limits, the circuit breaker and the overall deadline.
 */
class HttpClientFactory(private val tls: TlsContextFactory) {

    fun create(name: String, props: ClientProperties): PooledHttpClient {
        val t = props.timeouts
        val pool = PoolingHttpClientConnectionManagerBuilder.create()
            .setTlsSocketStrategy(tls.createStrategy(name, props.tls))
            .setMaxConnTotal(props.pool.maxTotal)
            .setMaxConnPerRoute(props.pool.maxPerRoute)
            .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
            .setConnPoolPolicy(PoolReusePolicy.LIFO)
            .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(Timeout.of(t.response)).setTcpNoDelay(true).build())
            .setDefaultConnectionConfig(
                ConnectionConfig.custom()
                    .setConnectTimeout(Timeout.of(t.connect))
                    .setSocketTimeout(Timeout.of(t.response))
                    .setTimeToLive(TimeValue.of(props.pool.timeToLive))
                    .setValidateAfterInactivity(TimeValue.of(props.pool.validateAfterInactivity))
                    .build(),
            )
            .build()

        val http = HttpClients.custom()
            .setConnectionManager(pool)
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.of(t.connectionRequest))
                    .setResponseTimeout(Timeout.of(t.response))
                    .setRedirectsEnabled(props.followRedirects)
                    .build(),
            )
            .setRetryStrategy(DefaultHttpRequestRetryStrategy(0, TimeValue.ZERO_MILLISECONDS))
            .evictExpiredConnections()
            .evictIdleConnections(TimeValue.of(props.pool.evictIdleAfter))
            .setUserAgent(props.userAgent)
            .disableCookieManagement()
            .build()
        return PooledHttpClient(http, pool)
    }
}
