package dev.viren.extapi.error

import dev.viren.extapi.config.ClientProperties
import org.apache.hc.client5.http.ConnectTimeoutException
import org.apache.hc.core5.http.ConnectionRequestTimeoutException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import javax.net.ssl.SSLHandshakeException

class FailureClassifierTest {
    private val c = DefaultFailureClassifier()
    private fun ctx(safe: Boolean) = CallContext("psp", "op", if (safe) "GET" else "POST", safe, ClientProperties())

    @Test
    fun `status mapping`() {
        assertThat(c.classifyStatus(ctx(true), 204, emptyMap(), null)).isNull()
        assertThat(c.classifyStatus(ctx(true), 400, emptyMap(), "bad")).isInstanceOf(ClientErrorException::class.java)
        assertThat(c.classifyStatus(ctx(true), 401, emptyMap(), null)).isInstanceOf(UnauthorizedException::class.java)
        assertThat(c.classifyStatus(ctx(true), 503, emptyMap(), null)!!.retryable).isTrue()
        assertThat(c.classifyStatus(ctx(false), 500, emptyMap(), null)!!.retryable).describedAs("500 after POST").isFalse()
        assertThat(c.classifyStatus(ctx(false), 503, emptyMap(), null)!!.retryable).describedAs("503 = not processed").isTrue()
        val tooMany = c.classifyStatus(ctx(false), 429, mapOf("Retry-After" to listOf("7")), null)!!
        assertThat(tooMany).isInstanceOf(UpstreamRateLimitedException::class.java)
        assertThat(tooMany.retryAfter).isEqualTo(Duration.ofSeconds(7))
        assertThat(tooMany.countsAsFailure).isFalse()
        assertThat(c.classifyStatus(ctx(true), 408, emptyMap(), null)!!.countsAsFailure).isTrue()
    }

    @Test
    fun `exception mapping respects idempotency`() {
        assertThat(c.classifyException(ctx(false), ConnectTimeoutException("x")).let { it.kind to it.retryable })
            .isEqualTo(FailureKind.CONNECT_TIMEOUT to true)
        assertThat(c.classifyException(ctx(false), ConnectionRequestTimeoutException("pool")).kind).isEqualTo(FailureKind.POOL_TIMEOUT)
        assertThat(c.classifyException(ctx(false), SocketTimeoutException("read")).retryable).isFalse()
        assertThat(c.classifyException(ctx(true), SocketTimeoutException("read")).retryable).isTrue()
        assertThat(c.classifyException(ctx(true), UnknownHostException("nope")).kind).isEqualTo(FailureKind.DNS_FAILURE)
        val tls = c.classifyException(ctx(true), SSLHandshakeException("PKIX path building failed"))
        assertThat(tls).isInstanceOf(TlsFailureException::class.java)
        assertThat(tls.message).contains("not trusted")
        assertThat(c.classifyException(ctx(true), IllegalStateException("boom")).kind).isEqualTo(FailureKind.UNEXPECTED)
    }

    @Test
    fun `retry-after formats`() {
        assertThat(DefaultFailureClassifier.parseRetryAfter(mapOf("retry-after" to listOf("1.5")))).isEqualTo(Duration.ofMillis(1500))
        val date = ZonedDateTime.now().plusSeconds(30).format(DateTimeFormatter.RFC_1123_DATE_TIME)
        assertThat(DefaultFailureClassifier.parseRetryAfter(mapOf("Retry-After" to listOf(date)))).isBetween(Duration.ofSeconds(28), Duration.ofSeconds(30))
        assertThat(DefaultFailureClassifier.parseRetryAfter(mapOf("Retry-After" to listOf("garbage")))).isNull()
    }
}
