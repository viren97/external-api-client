package dev.viren.extapi.support

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import com.sun.net.httpserver.HttpsConfigurator
import com.sun.net.httpserver.HttpsParameters
import com.sun.net.httpserver.HttpsServer
import java.net.InetSocketAddress
import java.security.KeyStore
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

data class Recorded(val method: String, val path: String, val query: String?, val headers: Map<String, String>, val body: String)

data class Reply(val status: Int, val body: String = "", val headers: Map<String, String> = emptyMap(), val delayMs: Long = 0)

/**
 * Tiny scriptable HTTP/HTTPS server: queue replies with [enqueue] (or set [fallback]) and
 * inspect what the client sent in [requests].
 */
class TestServer private constructor(private val server: HttpServer) : AutoCloseable {
    private val script = ConcurrentLinkedQueue<Reply>()
    val requests = ConcurrentLinkedQueue<Recorded>()
    val hits = AtomicInteger()
    @Volatile var fallback: (Recorded) -> Reply = { Reply(200, """{"ok":true}""", mapOf("Content-Type" to "application/json")) }

    init {
        server.createContext("/") { ex -> handle(ex) }
        server.executor = Executors.newCachedThreadPool()
        server.start()
    }

    val port: Int get() = server.address.port
    val baseUrl: String get() = (if (server is HttpsServer) "https" else "http") + "://localhost:$port"

    fun enqueue(vararg replies: Reply) = apply { script.addAll(replies) }

    private fun handle(ex: HttpExchange) {
        hits.incrementAndGet()
        val rec = Recorded(
            ex.requestMethod, ex.requestURI.path, ex.requestURI.rawQuery,
            ex.requestHeaders.entries.associate { it.key.lowercase() to it.value.joinToString(",") },
            ex.requestBody.readBytes().decodeToString(),
        )
        requests += rec
        val reply = script.poll() ?: fallback(rec)
        try {
            if (reply.delayMs > 0) Thread.sleep(reply.delayMs)
            reply.headers.forEach { (k, v) -> ex.responseHeaders.add(k, v) }
            if (!reply.headers.keys.any { it.equals("Content-Type", true) }) ex.responseHeaders.add("Content-Type", "application/json")
            val bytes = reply.body.toByteArray()
            ex.sendResponseHeaders(reply.status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
            if (bytes.isNotEmpty()) ex.responseBody.use { it.write(bytes) }
        } catch (_: Exception) {
            // client gave up (timeout test)
        } finally {
            ex.close()
        }
    }

    override fun close() = server.stop(0)

    companion object {
        fun http() = TestServer(HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0))

        /** HTTPS with the test server certificate; [requireClientCert] turns on mTLS. */
        fun https(requireClientCert: Boolean = false): TestServer {
            val pass = "changeit".toCharArray()
            val ks = KeyStore.getInstance("PKCS12").apply { load(cert("server.p12"), pass) }
            val ts = KeyStore.getInstance("PKCS12").apply { load(cert("truststore.p12"), pass) }
            val ctx = SSLContext.getInstance("TLS").apply {
                init(
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply { init(ks, pass) }.keyManagers,
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(ts) }.trustManagers,
                    null,
                )
            }
            val server = HttpsServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            server.httpsConfigurator = object : HttpsConfigurator(ctx) {
                override fun configure(params: HttpsParameters) {
                    val p = sslContext.defaultSSLParameters
                    p.needClientAuth = requireClientCert
                    params.setSSLParameters(p)
                }
            }
            return TestServer(server)
        }

        private fun cert(name: String) = TestServer::class.java.getResourceAsStream("/certs/$name")!!
    }
}
