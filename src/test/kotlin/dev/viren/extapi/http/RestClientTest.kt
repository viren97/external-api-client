package dev.viren.extapi.http

import dev.viren.extapi.config.AuthType
import dev.viren.extapi.core.ApiRequest
import dev.viren.extapi.error.ApiTimeoutException
import dev.viren.extapi.error.BulkheadFullException
import dev.viren.extapi.error.CircuitOpenException
import dev.viren.extapi.error.ClientErrorException
import dev.viren.extapi.error.ConnectionFailedException
import dev.viren.extapi.error.FailureKind
import dev.viren.extapi.error.RateLimitExceededException
import dev.viren.extapi.error.ResponseTooLargeException
import dev.viren.extapi.error.SerializationFailureException
import dev.viren.extapi.error.ServerErrorException
import dev.viren.extapi.error.UnauthorizedException
import dev.viren.extapi.error.UpstreamRateLimitedException
import dev.viren.extapi.support.Clients
import dev.viren.extapi.support.Reply
import dev.viren.extapi.support.TestServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.ServerSocket
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class RestClientTest {
    private val server = TestServer.http()

    @AfterEach fun close() = server.close()

    data class Account(val id: String, val balance: Long)

    @Test
    fun `decodes JSON, sends headers, query and path params`() {
        server.enqueue(Reply(200, """{"id":"a 1","balance":42,"extra":"ignored"}"""))
        val client = Clients.client(server.baseUrl) { defaultHeaders["X-Tenant"] = "t1" }

        val res = client.call<Account>(
            ApiRequest(dev.viren.extapi.core.HttpMethod.GET, "/accounts/{id}", pathParams = mapOf("id" to "a 1"))
                .withQuery("expand", "owner"),
        )

        assertThat(res.body).isEqualTo(Account("a 1", 42))
        assertThat(res.attempts).isEqualTo(1)
        val sent = server.requests.single()
        assertThat(sent.path).isEqualTo("/accounts/a 1")
        assertThat(sent.query).isEqualTo("expand=owner")
        assertThat(sent.headers["x-tenant"]).isEqualTo("t1")
        assertThat(sent.headers["x-correlation-id"]).isNotBlank()
        assertThat(ApiRequest(dev.viren.extapi.core.HttpMethod.GET, "/accounts/{id}", pathParams = mapOf("id" to "x")).operationName)
            .isEqualTo("GET /accounts/{id}")
    }

    @Test
    fun `retries idempotent GET on 503 then succeeds`() {
        server.enqueue(Reply(503), Reply(502), Reply(200, """{"id":"1","balance":1}"""))
        val res = Clients.client(server.baseUrl).call<Account>(ApiRequest.get("/a"))
        assertThat(res.attempts).isEqualTo(3)
        assertThat(server.hits.get()).isEqualTo(3)
    }

    @Test
    fun `does not retry POST on 500 without idempotency key`() {
        server.enqueue(Reply(500))
        assertThatThrownBy { Clients.client(server.baseUrl).call<Any>(ApiRequest.post("/charges", mapOf("amount" to 1))) }
            .isInstanceOfSatisfying(ServerErrorException::class.java) { assertThat(it.retryable).isFalse() }
        assertThat(server.hits.get()).isEqualTo(1)
    }

    @Test
    fun `retries POST with idempotency key and sends the same key every time`() {
        server.enqueue(Reply(500), Reply(201, """{"id":"c1","balance":0}"""))
        val res = Clients.client(server.baseUrl).call<Account>(ApiRequest.post("/charges", mapOf("amount" to 1), idempotencyKey = "k-1"))
        assertThat(res.status).isEqualTo(201)
        assertThat(server.requests.map { it.headers["idempotency-key"] }).containsExactly("k-1", "k-1")
    }

    @Test
    fun `honours Retry-After on 429 and trips shared cooldown`() {
        server.enqueue(Reply(429, headers = mapOf("Retry-After" to "1")), Reply(200, "{}"))
        val started = System.nanoTime()
        val res = Clients.client(server.baseUrl).call<Map<String, Any>>(ApiRequest.get("/x"))
        assertThat(res.attempts).isEqualTo(2)
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(Duration.ofMillis(950))
    }

    @Test
    fun `429 with Retry-After beyond limit fails fast and blocks other callers via cooldown`() {
        server.enqueue(Reply(429, headers = mapOf("Retry-After" to "120")))
        val client = Clients.client(server.baseUrl)
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }
            .isInstanceOfSatisfying(UpstreamRateLimitedException::class.java) { assertThat(it.retryAfter).isEqualTo(Duration.ofSeconds(120)) }
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }
            .isInstanceOf(RateLimitExceededException::class.java)
        assertThat(server.hits.get()).describedAs("second call never left the process").isEqualTo(1)
    }

    @Test
    fun `4xx is not retried and carries status and body`() {
        server.enqueue(Reply(422, """{"error":"invalid amount"}"""))
        assertThatThrownBy { Clients.client(server.baseUrl).call<Any>(ApiRequest.get("/x")) }
            .isInstanceOfSatisfying(ClientErrorException::class.java) {
                assertThat(it.status).isEqualTo(422)
                assertThat(it.responseBody).contains("invalid amount")
                assertThat(it.retryable).isFalse()
            }
        assertThat(server.hits.get()).isEqualTo(1)
    }

    @Test
    fun `response timeout is classified and retried for GET`() {
        server.enqueue(Reply(200, "{}", delayMs = 1500), Reply(200, "{}"))
        val client = Clients.client(server.baseUrl) { timeouts.response = Duration.ofMillis(300) }
        val res = client.call<Map<String, Any>>(ApiRequest.get("/slow"))
        assertThat(res.attempts).isEqualTo(2)
    }

    @Test
    fun `response timeout on POST is not retried because outcome is unknown`() {
        server.enqueue(Reply(200, "{}", delayMs = 1500))
        val client = Clients.client(server.baseUrl) { timeouts.response = Duration.ofMillis(300) }
        assertThatThrownBy { client.call<Any>(ApiRequest.post("/pay", mapOf("a" to 1))) }
            .isInstanceOfSatisfying(ApiTimeoutException::class.java) {
                assertThat(it.kind).isEqualTo(FailureKind.RESPONSE_TIMEOUT)
                assertThat(it.retryable).isFalse()
            }
    }

    @Test
    fun `total deadline caps retries`() {
        server.fallback = { Reply(503) }
        val client = Clients.client(server.baseUrl) {
            retry.maxAttempts = 50; retry.initialBackoff = Duration.ofMillis(200); retry.maxBackoff = Duration.ofMillis(200); retry.jitter = 0.0
            timeouts.total = Duration.ofMillis(700)
            circuitBreaker.enabled = false
        }
        val started = System.nanoTime()
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }.isInstanceOf(ServerErrorException::class.java)
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2))
        assertThat(server.hits.get()).isBetween(2, 5)
    }

    @Test
    fun `connection refused is retryable connection failure`() {
        val port = ServerSocket(0).use { it.localPort }
        val client = Clients.client("http://127.0.0.1:$port") { retry.maxAttempts = 2 }
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }
            .isInstanceOfSatisfying(ConnectionFailedException::class.java) {
                assertThat(it.kind).isEqualTo(FailureKind.CONNECTION_FAILED)
                assertThat(it.retryable).isTrue()
                assertThat(it.attempts).isEqualTo(2)
            }
    }

    @Test
    fun `circuit opens after repeated failures and short-circuits without network`() {
        server.fallback = { Reply(500) }
        val client = Clients.client(server.baseUrl) { retry.enabled = false }
        repeat(4) { runCatching { client.call<Any>(ApiRequest.get("/x")) } }
        val before = server.hits.get()
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }
            .isInstanceOfSatisfying(CircuitOpenException::class.java) { assertThat(it.retryAfter).isPositive() }
        assertThat(server.hits.get()).isEqualTo(before)
    }

    @Test
    fun `4xx does not open the circuit`() {
        server.fallback = { Reply(404) }
        val client = Clients.client(server.baseUrl)
        repeat(10) { runCatching { client.call<Any>(ApiRequest.get("/x")) } }
        assertThat(server.hits.get()).isEqualTo(10)
    }

    @Test
    fun `rejects oversized responses`() {
        server.enqueue(Reply(200, "x".repeat(5000)))
        val client = Clients.client(server.baseUrl) { maxResponseBytes = 1000 }
        assertThatThrownBy { client.call<String>(ApiRequest.get("/big")) }.isInstanceOf(ResponseTooLargeException::class.java)
    }

    @Test
    fun `undecodable body raises serialization failure with raw body`() {
        server.enqueue(Reply(200, "<html>oops</html>"))
        assertThatThrownBy { Clients.client(server.baseUrl).call<Account>(ApiRequest.get("/x")) }
            .isInstanceOfSatisfying(SerializationFailureException::class.java) { assertThat(it.responseBody).contains("oops") }
    }

    @Test
    fun `bulkhead rejects calls above the concurrency cap`() {
        val gate = CountDownLatch(1)
        server.fallback = { gate.await(5, TimeUnit.SECONDS); Reply(200, "{}") }
        val client = Clients.client(server.baseUrl) { bulkhead.enabled = true; bulkhead.maxConcurrentCalls = 1 }
        val pool = Executors.newSingleThreadExecutor()
        val first = pool.submit { client.call<Any>(ApiRequest.get("/hold")) }
        Thread.sleep(200)
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }.isInstanceOf(BulkheadFullException::class.java)
        gate.countDown()
        first.get(5, TimeUnit.SECONDS)
        pool.shutdown()
    }

    @Test
    fun `oauth2 client credentials token is cached and refreshed after 401`() {
        var tokens = 0
        server.fallback = { r ->
            when {
                r.path == "/oauth/token" -> { tokens++; Reply(200, """{"access_token":"t$tokens","expires_in":3600}""") }
                r.headers["authorization"] == "Bearer t1" && server.hits.get() > 3 -> Reply(401)
                else -> Reply(200, """{"auth":"${r.headers["authorization"]}"}""")
            }
        }
        val client = Clients.client(server.baseUrl) {
            auth.type = AuthType.OAUTH2_CLIENT_CREDENTIALS
            auth.tokenUrl = "${server.baseUrl}/oauth/token"; auth.clientId = "id"; auth.clientSecret = "secret"; auth.scope = "read"
        }
        assertThat(client.call<Map<String, String>>(ApiRequest.get("/a")).body!!["auth"]).isEqualTo("Bearer t1")
        assertThat(client.call<Map<String, String>>(ApiRequest.get("/b")).body!!["auth"]).isEqualTo("Bearer t1")
        assertThat(tokens).isEqualTo(1)
        // Next call gets 401 with t1 -> token refreshed -> retried transparently with t2.
        assertThat(client.call<Map<String, String>>(ApiRequest.get("/c")).body!!["auth"]).isEqualTo("Bearer t2")
        assertThat(server.requests.first { it.path == "/oauth/token" }.body).contains("grant_type=client_credentials", "scope=read")
    }

    @Test
    fun `persistent 403 is not retried`() {
        server.fallback = { Reply(403) }
        val client = Clients.client(server.baseUrl) { auth.type = AuthType.BEARER; auth.token = "abc" }
        assertThatThrownBy { client.call<Any>(ApiRequest.get("/x")) }.isInstanceOf(UnauthorizedException::class.java)
        assertThat(server.requests.single().headers["authorization"]).isEqualTo("Bearer abc")
    }

    @Test
    fun `callOrElse returns fallback on failure`() {
        server.fallback = { Reply(500) }
        val value = Clients.client(server.baseUrl) { retry.enabled = false }.callOrElse(ApiRequest.get("/price")) { _: Any -> "cached" }
        assertThat(value).isEqualTo("cached")
    }

    @Test
    fun `form bodies are url encoded`() {
        Clients.client(server.baseUrl).call<Any>(ApiRequest(dev.viren.extapi.core.HttpMethod.POST, "/f", body = mapOf("a" to "x y", "b" to "&"), contentType = ApiRequest.FORM))
        assertThat(server.requests.single().body).isEqualTo("a=x+y&b=%26")
    }
}
