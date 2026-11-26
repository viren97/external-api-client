package dev.viren.extapi.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import dev.viren.extapi.config.AsyncProperties
import dev.viren.extapi.config.ClientProperties
import dev.viren.extapi.core.ApiRequest
import org.slf4j.MDC
import org.springframework.dao.DuplicateKeyException
import java.time.Clock
import java.util.UUID

/**
 * Entry point for fire-and-forget calls. [submit] only inserts a row, so when it runs inside
 * the caller's `@Transactional` method the call is scheduled **if and only if** the business
 * transaction commits (transactional outbox). Nothing is sent to the network here.
 *
 * ```kotlin
 * @Transactional
 * fun placeOrder(cmd: PlaceOrder) {
 *     orders.save(order)
 *     async.submit("payments", ApiRequest.post("/charges", charge, operation = "create-charge"),
 *                  AsyncOptions(dedupeKey = "charge-${order.id}", callbackUrl = "https://me/hooks/payments"))
 * }
 * ```
 */
class AsyncApiDispatcher(
    private val repository: OutboxRepository,
    private val mapper: ObjectMapper,
    private val props: AsyncProperties,
    private val clientProps: (String) -> ClientProperties?,
    private val clock: Clock = Clock.systemUTC(),
) {
    /** @return the outbox id; with a [AsyncOptions.dedupeKey] seen before, the existing id. */
    fun submit(client: String, request: ApiRequest, options: AsyncOptions = AsyncOptions()): String =
        insert(OutboxKind.API_CALL, client, request, options, options.maxAttempts ?: props.maxAttempts)

    internal fun insert(kind: OutboxKind, client: String, request: ApiRequest, options: AsyncOptions, maxAttempts: Int): String {
        val cp = requireNotNull(clientProps(client)) { "No ext-api client named '$client'" }
        val id = UUID.randomUUID().toString()
        // The upstream must see the same idempotency key on every redelivery, so fix it now.
        val req = if (!request.method.idempotent && request.idempotencyKey == null && cp.retry.autoIdempotencyKey) {
            val key = options.dedupeKey ?: id
            request.copy(idempotencyKey = key, headers = request.headers + (cp.retry.idempotencyHeader to key))
        } else request

        val now = clock.instant()
        val record = OutboxRecord(
            id = id, kind = kind, clientName = client, operation = req.operationName,
            payload = mapper.writeValueAsString(StoredRequest(request = req)),
            callbackUrl = options.callbackUrl, callbackEvent = options.callbackEvent,
            dedupeKey = options.dedupeKey,
            correlationId = options.correlationId ?: MDC.get("correlationId") ?: MDC.get("traceId"),
            status = OutboxStatus.PENDING, attempts = 0, maxAttempts = maxAttempts.coerceAtLeast(1),
            nextAttemptAt = options.notBefore?.takeIf { it.isAfter(now) } ?: now,
            lockedUntil = null, lastErrorKind = null, lastError = null, responseStatus = null, responseBody = null,
            createdAt = now, updatedAt = now,
        )
        return try {
            repository.insert(record)
            id
        } catch (e: DuplicateKeyException) {
            val key = options.dedupeKey ?: throw e
            repository.findByDedupeKey(key)?.id ?: throw e
        }
    }

    fun status(id: String): OutboxRecord? = repository.findById(id)
    fun cancel(id: String): Boolean = repository.cancel(id)
    fun requeue(id: String): Boolean = repository.requeue(id)
}
