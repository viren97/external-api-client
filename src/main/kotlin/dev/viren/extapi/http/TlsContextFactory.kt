package dev.viren.extapi.http

import dev.viren.extapi.config.TlsMode
import dev.viren.extapi.config.TlsProperties
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier
import org.apache.hc.client5.http.ssl.TlsSocketStrategy
import org.apache.hc.core5.reactor.ssl.SSLBufferMode
import org.slf4j.LoggerFactory
import org.springframework.boot.ssl.SslBundles
import org.springframework.core.io.ResourceLoader
import java.net.Socket
import javax.net.ssl.SSLException
import javax.net.ssl.SSLSocket
import org.apache.hc.core5.http.protocol.HttpContext
import java.security.KeyFactory
import java.security.KeyStore
import java.security.MessageDigest
import java.security.Principal
import java.security.PrivateKey
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Duration
import java.time.Instant
import java.util.Base64
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509ExtendedKeyManager
import javax.net.ssl.X509ExtendedTrustManager
import javax.net.ssl.X509TrustManager

class TlsConfigurationException(message: String, cause: Throwable? = null) : IllegalStateException(message, cause)

/**
 * Builds the TLS strategy for a client from [TlsProperties]. Supports:
 * - DEFAULT: JVM trust store (works for plain http too)
 * - TLS: custom trust store or PEM CA bundle for private CAs
 * - MTLS: client certificate from PKCS12/JKS, PEM cert+key, or a Spring Boot SSL bundle
 * - optional public-key pinning on top of normal chain validation
 *
 * Misconfiguration fails at startup with a message naming the client, not on the first request.
 */
class TlsContextFactory(
    private val resourceLoader: ResourceLoader,
    private val sslBundles: SslBundles? = null,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun createStrategy(client: String, tls: TlsProperties): TlsSocketStrategy = RecordingTlsStrategy(createDelegate(client, tls))

    private fun createDelegate(client: String, tls: TlsProperties): TlsSocketStrategy {
        val context = createSslContext(client, tls)
        val protocols = tls.protocols.takeIf { it.isNotEmpty() }?.toTypedArray()
        val ciphers = tls.cipherSuites.takeIf { it.isNotEmpty() }?.toTypedArray()
        val verify = tls.hostnameVerification && tls.mode != TlsMode.INSECURE_TRUST_ALL
        return if (verify) {
            DefaultClientTlsStrategy(context, protocols, ciphers, SSLBufferMode.STATIC, HostnameVerificationPolicy.BOTH, null)
        } else {
            log.warn("ext-api client '{}': TLS hostname verification is DISABLED", client)
            DefaultClientTlsStrategy(context, protocols, ciphers, SSLBufferMode.STATIC, HostnameVerificationPolicy.CLIENT, NoopHostnameVerifier.INSTANCE)
        }
    }

    fun createSslContext(client: String, tls: TlsProperties): SSLContext = try {
        val keyManagers = keyManagers(client, tls)
        var trustManagers = trustManagers(client, tls)
        if (tls.pinnedSha256.isNotEmpty()) {
            trustManagers = trustManagers.map { tm -> if (tm is X509TrustManager) PinningTrustManager(tm, tls.pinnedSha256.toSet()) else tm }.toTypedArray()
        }
        SSLContext.getInstance("TLS").apply { init(keyManagers, trustManagers, null) }
    } catch (e: TlsConfigurationException) {
        throw e
    } catch (e: Exception) {
        throw TlsConfigurationException("ext-api client '$client': cannot build TLS context: ${e.message}", e)
    }

    private fun trustManagers(client: String, tls: TlsProperties): Array<TrustManager> {
        if (tls.mode == TlsMode.INSECURE_TRUST_ALL) {
            log.warn("ext-api client '{}' uses INSECURE_TRUST_ALL: every server certificate is accepted. Never use this in production.", client)
            return arrayOf(TrustAllManager)
        }
        tls.bundle?.let { return bundles(client).getBundle(it).managers.trustManagers }

        val store: KeyStore? = when {
            tls.trustStore.location != null -> loadStore(tls.trustStore.location!!, tls.trustStore.type, tls.trustStore.password)
            tls.trustCertificatesPem != null -> KeyStore.getInstance(KeyStore.getDefaultType()).apply {
                load(null, null)
                readCertificates(tls.trustCertificatesPem!!).forEachIndexed { i, cert -> setCertificateEntry("ca-$i", cert) }
            }
            tls.mode == TlsMode.TLS ->
                throw TlsConfigurationException("ext-api client '$client': mode TLS needs trust-store.location or trust-certificates-pem (use DEFAULT for public CAs)")
            else -> null // JVM default trust store
        }
        return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(store) }.trustManagers
    }

    private fun keyManagers(client: String, tls: TlsProperties): Array<KeyManager>? {
        if (tls.mode != TlsMode.MTLS) return null
        tls.bundle?.let { return bundles(client).getBundle(it).managers.keyManagers }

        val password = (tls.keyPassword ?: tls.keyStore.password ?: "").toCharArray()
        val store: KeyStore = when {
            tls.keyStore.location != null -> loadStore(tls.keyStore.location!!, tls.keyStore.type, tls.keyStore.password)
            tls.clientCertificatePem != null && tls.clientPrivateKeyPem != null -> KeyStore.getInstance("PKCS12").apply {
                load(null, null)
                val chain = readCertificates(tls.clientCertificatePem!!)
                setKeyEntry("client", readPrivateKey(tls.clientPrivateKeyPem!!), password, chain.toTypedArray())
            }
            else -> throw TlsConfigurationException(
                "ext-api client '$client': mode MTLS needs key-store.location or client-certificate-pem + client-private-key-pem",
            )
        }
        warnOnExpiry(client, store)
        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply { init(store, password) }
        val alias = tls.keyAlias ?: return kmf.keyManagers
        if (!store.isKeyEntry(alias)) throw TlsConfigurationException("ext-api client '$client': key-alias '$alias' not found in key store")
        return kmf.keyManagers.map { km -> if (km is X509ExtendedKeyManager) FixedAliasKeyManager(km, alias) else km }.toTypedArray()
    }

    private fun warnOnExpiry(client: String, store: KeyStore) {
        for (alias in store.aliases()) {
            val cert = store.getCertificate(alias) as? X509Certificate ?: continue
            val left = Duration.between(Instant.now(), cert.notAfter.toInstant())
            when {
                left.isNegative -> throw TlsConfigurationException("ext-api client '$client': client certificate '$alias' expired on ${cert.notAfter}")
                left < Duration.ofDays(30) -> log.warn("ext-api client '{}': client certificate '{}' expires in {} days", client, alias, left.toDays())
            }
        }
    }

    private fun bundles(client: String) =
        sslBundles ?: throw TlsConfigurationException("ext-api client '$client' references an SSL bundle but no SslBundles bean exists")

    private fun loadStore(location: String, type: String, password: String?): KeyStore {
        val resource = resourceLoader.getResource(location)
        if (!resource.exists()) throw TlsConfigurationException("key/trust store not found: $location")
        return KeyStore.getInstance(type).apply { resource.inputStream.use { load(it, password?.toCharArray()) } }
    }

    private fun readText(location: String): String {
        val resource = resourceLoader.getResource(location)
        if (!resource.exists()) throw TlsConfigurationException("PEM file not found: $location")
        return resource.inputStream.use { it.readBytes().decodeToString() }
    }

    private fun readCertificates(location: String): List<X509Certificate> {
        val certs = readText(location).byteInputStream().use {
            CertificateFactory.getInstance("X.509").generateCertificates(it).map { c -> c as X509Certificate }
        }
        if (certs.isEmpty()) throw TlsConfigurationException("no certificates in $location")
        return certs
    }

    private fun readPrivateKey(location: String): PrivateKey {
        val pem = readText(location)
        if (pem.contains("BEGIN RSA PRIVATE KEY") || pem.contains("BEGIN EC PRIVATE KEY")) {
            throw TlsConfigurationException(
                "$location is PKCS#1/SEC1; convert it with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem",
            )
        }
        val base64 = pem.substringAfter("-----BEGIN PRIVATE KEY-----", "").substringBefore("-----END PRIVATE KEY-----")
        if (base64.isBlank()) throw TlsConfigurationException("$location has no unencrypted PKCS#8 'BEGIN PRIVATE KEY' block")
        val spec = PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(base64))
        for (algorithm in listOf("RSA", "EC", "Ed25519")) {
            runCatching { return KeyFactory.getInstance(algorithm).generatePrivate(spec) }
        }
        throw TlsConfigurationException("$location: unsupported private key algorithm")
    }
}

/** Standard chain validation, then requires one certificate in the chain to match a pinned SPKI hash. */
internal class PinningTrustManager(private val delegate: X509TrustManager, private val pins: Set<String>) : X509ExtendedTrustManager() {
    private fun pin(chain: Array<out X509Certificate>) {
        val digest = MessageDigest.getInstance("SHA-256")
        val match = chain.any { Base64.getEncoder().encodeToString(digest.digest(it.publicKey.encoded)) in pins }
        if (!match) throw CertificateException("certificate pin mismatch for ${chain.firstOrNull()?.subjectX500Principal}")
    }

    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String) {
        delegate.checkServerTrusted(chain, authType); pin(chain)
    }
    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String, socket: Socket?) {
        (delegate as? X509ExtendedTrustManager)?.checkServerTrusted(chain, authType, socket) ?: delegate.checkServerTrusted(chain, authType)
        pin(chain)
    }
    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String, engine: SSLEngine?) {
        (delegate as? X509ExtendedTrustManager)?.checkServerTrusted(chain, authType, engine) ?: delegate.checkServerTrusted(chain, authType)
        pin(chain)
    }
    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String) = delegate.checkClientTrusted(chain, authType)
    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String, socket: Socket?) = delegate.checkClientTrusted(chain, authType)
    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String, engine: SSLEngine?) = delegate.checkClientTrusted(chain, authType)
    override fun getAcceptedIssuers(): Array<X509Certificate> = delegate.acceptedIssuers
}

internal object TrustAllManager : X509ExtendedTrustManager() {
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit
    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?, socket: Socket?) = Unit
    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?, socket: Socket?) = Unit
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?, engine: SSLEngine?) = Unit
    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?, engine: SSLEngine?) = Unit
    override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
}

/** Always presents the configured alias, for key stores that hold several client certs. */
internal class FixedAliasKeyManager(private val delegate: X509ExtendedKeyManager, private val alias: String) : X509ExtendedKeyManager() {
    override fun chooseClientAlias(keyType: Array<out String>?, issuers: Array<out Principal>?, socket: Socket?) = alias
    override fun chooseEngineClientAlias(keyType: Array<out String>?, issuers: Array<out Principal>?, engine: SSLEngine?) = alias
    override fun getClientAliases(keyType: String?, issuers: Array<out Principal>?): Array<String> = arrayOf(alias)
    override fun getServerAliases(keyType: String?, issuers: Array<out Principal>?): Array<String>? = delegate.getServerAliases(keyType, issuers)
    override fun chooseServerAlias(keyType: String?, issuers: Array<out Principal>?, socket: Socket?): String? = delegate.chooseServerAlias(keyType, issuers, socket)
    override fun getCertificateChain(alias: String?): Array<X509Certificate>? = delegate.getCertificateChain(alias)
    override fun getPrivateKey(alias: String?): PrivateKey? = delegate.getPrivateKey(alias)
}

/**
 * When a host resolves to several addresses (e.g. `localhost` -> 127.0.0.1 and ::1), the HTTP
 * client moves on to the next address after a TLS handshake failure and reports only the last
 * error, often a misleading "connection refused". This remembers the handshake failure on the
 * calling thread so the real cause can be reported.
 */
internal class RecordingTlsStrategy(private val delegate: TlsSocketStrategy) : TlsSocketStrategy {
    override fun upgrade(socket: Socket, target: String, port: Int, attachment: Any?, context: HttpContext?): SSLSocket = try {
        delegate.upgrade(socket, target, port, attachment, context)
    } catch (e: SSLException) {
        LAST_FAILURE.set(e)
        throw e
    }

    companion object {
        private val LAST_FAILURE = ThreadLocal<SSLException>()

        fun clear() = LAST_FAILURE.remove()

        /** Returns and clears the handshake failure seen during the last connect on this thread. */
        fun consume(): SSLException? = LAST_FAILURE.get().also { LAST_FAILURE.remove() }
    }
}
