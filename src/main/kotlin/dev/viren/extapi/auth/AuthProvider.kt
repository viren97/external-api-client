package dev.viren.extapi.auth

import com.fasterxml.jackson.databind.ObjectMapper
import dev.viren.extapi.config.AuthProperties
import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.http.io.entity.StringEntity
import java.net.URLEncoder
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Adds credentials to outgoing requests. Implement it (and set `auth.type: CUSTOM`,
 * `auth.custom-bean: myBean`) for HMAC request signing, AWS SigV4, rotating keys, etc.
 */
interface AuthProvider {
    fun headers(): Map<String, String>

    /** Called after a 401. Return true if credentials were refreshed and the call should be retried once. */
    fun onUnauthorized(): Boolean = false
}

object NoAuth : AuthProvider {
    override fun headers() = emptyMap<String, String>()
}

class StaticHeaderAuth(private val values: Map<String, String>) : AuthProvider {
    override fun headers() = values

    companion object {
        fun bearer(token: String) = StaticHeaderAuth(mapOf("Authorization" to "Bearer $token"))
        fun apiKey(header: String, key: String) = StaticHeaderAuth(mapOf(header to key))
        fun basic(user: String, password: String) =
            StaticHeaderAuth(mapOf("Authorization" to "Basic " + Base64.getEncoder().encodeToString("$user:$password".toByteArray())))
    }
}

/**
 * OAuth2 client-credentials grant with a cached token. The token request goes through the
 * client's own HTTP connection pool, so it uses the same TLS / mTLS settings as the API calls
 * (needed for providers that require mTLS on the token endpoint too, e.g. open-banking).
 * Only one thread refreshes; the others reuse the result.
 */
class OAuth2ClientCredentialsAuth(
    private val props: AuthProperties,
    private val http: CloseableHttpClient,
    private val mapper: ObjectMapper,
    private val clock: () -> Instant = Instant::now,
) : AuthProvider {
    private val lock = ReentrantLock()
    @Volatile private var token: String? = null
    @Volatile private var expiresAt: Instant = Instant.EPOCH

    init {
        requireNotNull(props.tokenUrl) { "auth.token-url is required for OAUTH2_CLIENT_CREDENTIALS" }
        requireNotNull(props.clientId) { "auth.client-id is required for OAUTH2_CLIENT_CREDENTIALS" }
        requireNotNull(props.clientSecret) { "auth.client-secret is required for OAUTH2_CLIENT_CREDENTIALS" }
    }

    override fun headers(): Map<String, String> = mapOf("Authorization" to "Bearer ${currentToken()}")

    override fun onUnauthorized(): Boolean {
        lock.withLock { token = null; expiresAt = Instant.EPOCH }
        return true
    }

    private fun currentToken(): String {
        val cached = token
        if (cached != null && clock().isBefore(expiresAt.minus(props.refreshSkew))) return cached
        return lock.withLock {
            val again = token
            if (again != null && clock().isBefore(expiresAt.minus(props.refreshSkew))) return again
            fetch()
        }
    }

    private fun fetch(): String {
        val form = buildMap {
            put("grant_type", "client_credentials")
            put("client_id", props.clientId!!)
            put("client_secret", props.clientSecret!!)
            props.scope?.let { put("scope", it) }
            props.audience?.let { put("audience", it) }
        }.entries.joinToString("&") { "${enc(it.key)}=${enc(it.value)}" }

        val post = HttpPost(props.tokenUrl).apply {
            entity = StringEntity(form, ContentType.APPLICATION_FORM_URLENCODED)
            setHeader("Accept", "application/json")
        }
        return http.execute(post) { response ->
            val body = response.entity?.let { EntityUtils.toString(it) }.orEmpty()
            if (response.code !in 200..299) throw IllegalStateException("token endpoint returned ${response.code}: ${body.take(300)}")
            val json = mapper.readTree(body)
            val access = json.path("access_token").asText(null) ?: throw IllegalStateException("token response has no access_token")
            val ttl = json.path("expires_in").asLong(300)
            token = access
            expiresAt = clock().plus(Duration.ofSeconds(ttl))
            access
        }
    }

    private fun enc(v: String) = URLEncoder.encode(v, Charsets.UTF_8)
}
