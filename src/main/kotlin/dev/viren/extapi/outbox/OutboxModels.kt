package dev.viren.extapi.outbox

import dev.viren.extapi.core.ApiRequest
import java.time.Instant

enum class OutboxKind { API_CALL, WEBHOOK }

/**
 * Lifecycle of an async call:
 *
 * ```
 * PENDING --relay--> QUEUED --worker--> PROCESSING --> SUCCEEDED
 *    ^                  |                    |------> FAILED   (non-retryable, e.g. 400)
 *    |                  | lease expired      |------> DEAD     (retries exhausted)
 *    +------------------+--------------------+------> PENDING  (retry at next_attempt_at)
 * ```
 * The database row is the source of truth; Kafka only carries "row X is ready" notifications,
 * so a lost or duplicated message can never lose or double-execute a call.
 */
enum class OutboxStatus {
    PENDING, QUEUED, PROCESSING, SUCCEEDED, FAILED, DEAD, CANCELLED;

    val terminal: Boolean get() = this == SUCCEEDED || this == FAILED || this == DEAD || this == CANCELLED
}

data class OutboxRecord(
    val id: String,
    val kind: OutboxKind,
    val clientName: String,
    val operation: String?,
    val payload: String,
    val callbackUrl: String?,
    val callbackEvent: String?,
    val dedupeKey: String?,
    val correlationId: String?,
    val status: OutboxStatus,
    val attempts: Int,
    val maxAttempts: Int,
    val nextAttemptAt: Instant,
    val lockedUntil: Instant?,
    val lastErrorKind: String?,
    val lastError: String?,
    val responseStatus: Int?,
    val responseBody: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
)

/** Notification sent over the transport. Small on purpose: workers load the row from the DB. */
data class OutboxEnvelope(val id: String, val kind: OutboxKind, val client: String, val key: String)

/** Options for [AsyncApiDispatcher.submit]. */
data class AsyncOptions(
    /** Submitting twice with the same key returns the first submission's id instead of a duplicate call. */
    val dedupeKey: String? = null,
    /** Receives a signed webhook with the outcome. */
    val callbackUrl: String? = null,
    val callbackEvent: String? = null,
    val maxAttempts: Int? = null,
    /** Do not start before this instant (scheduled calls). */
    val notBefore: Instant? = null,
    val correlationId: String? = null,
)

/** Final outcome of an async call, delivered to [AsyncResultListener]s, the result topic and callbacks. */
data class AsyncCallResult(
    val id: String,
    val kind: OutboxKind,
    val client: String,
    val operation: String?,
    val status: OutboxStatus,
    val attempts: Int,
    val responseStatus: Int?,
    val responseBody: String?,
    val errorKind: String?,
    val error: String?,
    val dedupeKey: String?,
    val correlationId: String?,
    val completedAt: Instant,
)

/** In-process hook for outcomes, e.g. to update an order when the payment call finishes. */
fun interface AsyncResultListener {
    fun onResult(result: AsyncCallResult)
}

/** Stored request for an async call. Wraps [ApiRequest] so the format can evolve. */
data class StoredRequest(val version: Int = 1, val request: ApiRequest)
