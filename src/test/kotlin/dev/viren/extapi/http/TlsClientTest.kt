package dev.viren.extapi.http

import dev.viren.extapi.config.ClientProperties
import dev.viren.extapi.config.TlsMode
import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.error.TlsFailureException
import dev.viren.extapi.support.Clients
import dev.viren.extapi.support.TestServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.security.KeyStore
import java.security.MessageDigest
import java.util.Base64

class TlsClientTest {
    private fun ClientProperties.trustTestCa() {
        tls.trustStore.location = "classpath:certs/truststore.p12"
        tls.trustStore.password = "changeit"
    }

    private fun ClientProperties.clientCert() {
        tls.keyStore.location = "classpath:certs/client.p12"
        tls.keyStore.password = "changeit"
    }

    private fun ok(client: ExternalApiClient) = assertThat(client.call<Map<String, Any>>(ApiRequest.get("/ping")).status).isEqualTo(200)

    @Test
    fun `TLS with custom trust store`(): Unit = TestServer.https().use { s ->
        ok(Clients.client(s.baseUrl) { tls.mode = TlsMode.TLS; trustTestCa() })
    }

    @Test
    fun `TLS with PEM CA bundle`(): Unit = TestServer.https().use { s ->
        ok(Clients.client(s.baseUrl) { tls.mode = TlsMode.TLS; tls.trustCertificatesPem = "classpath:certs/ca.pem" })
    }

    @Test
    fun `DEFAULT mode rejects private CA with a helpful message and no retries`(): Unit = TestServer.https().use { s ->
        assertThatThrownBy { Clients.client(s.baseUrl).call<Any>(ApiRequest.get("/ping")) }
            .isInstanceOfSatisfying(TlsFailureException::class.java) {
                assertThat(it.retryable).isFalse()
                assertThat(it.message).contains("not trusted")
            }
        assertThat(s.hits.get()).isZero()
    }

    @Test
    fun `wrong CA is rejected`(): Unit = TestServer.https().use { s ->
        val client = Clients.client(s.baseUrl) { tls.mode = TlsMode.TLS; tls.trustCertificatesPem = "classpath:certs/other-ca.pem" }
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/ping")) }.isInstanceOf(TlsFailureException::class.java)
    }

    @Test
    fun `mTLS with PKCS12 client certificate`(): Unit = TestServer.https(requireClientCert = true).use { s ->
        ok(Clients.client(s.baseUrl) { tls.mode = TlsMode.MTLS; trustTestCa(); clientCert() })
    }

    @Test
    fun `mTLS with PEM certificate and key`(): Unit = TestServer.https(requireClientCert = true).use { s ->
        ok(Clients.client(s.baseUrl) {
            tls.mode = TlsMode.MTLS; trustTestCa()
            tls.clientCertificatePem = "classpath:certs/client-chain.pem"
            tls.clientPrivateKeyPem = "classpath:certs/client-key.pem"
        })
    }

    @Test
    fun `server requiring client cert rejects a plain TLS client`(): Unit = TestServer.https(requireClientCert = true).use { s ->
        val client = Clients.client(s.baseUrl) { tls.mode = TlsMode.TLS; trustTestCa(); retry.enabled = false }
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/ping")) }
            .isInstanceOf(dev.viren.extapi.error.ExternalApiException::class.java)
        assertThat(s.hits.get()).isZero()
    }

    @Test
    fun `public key pinning accepts the right pin and rejects others`(): Unit = TestServer.https().use { s ->
        val ks = KeyStore.getInstance("PKCS12").apply { load(TlsClientTest::class.java.getResourceAsStream("/certs/server.p12"), "changeit".toCharArray()) }
        val pin = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(ks.getCertificate("server").publicKey.encoded))

        ok(Clients.client(s.baseUrl) { tls.mode = TlsMode.TLS; trustTestCa(); tls.pinnedSha256 = mutableListOf(pin) })
        val wrong = Clients.client(s.baseUrl) { tls.mode = TlsMode.TLS; trustTestCa(); tls.pinnedSha256 = mutableListOf("AAAA") }
        assertThatThrownBy { wrong.call<Any>(ApiRequest.get("/ping")) }.isInstanceOf(TlsFailureException::class.java)
    }

    @Test
    fun `misconfigured MTLS fails at startup`() {
        assertThatThrownBy { Clients.client("https://localhost") { tls.mode = TlsMode.MTLS } }
            .isInstanceOf(TlsConfigurationException::class.java)
            .hasMessageContaining("'test'").hasMessageContaining("key-store.location")
        assertThatThrownBy { Clients.client("https://localhost") { tls.mode = TlsMode.TLS; tls.trustStore.location = "classpath:nope.p12" } }
            .hasMessageContaining("not found")
    }
}
