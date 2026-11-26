package dev.viren.extapi.outbox

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Clock
import java.time.Instant

/**
 * Plain JDBC so it works with any stack (JPA, jOOQ, plain JDBC) and joins the caller's
 * transaction through Spring's DataSource transaction synchronization.
 *
 * Concurrency between instances relies on compare-and-set updates
 * (`UPDATE ... WHERE id = ? AND status = ?`): only the instance whose update hits one row owns it.
 * That is portable across PostgreSQL, MySQL, H2, Oracle and SQL Server without `SKIP LOCKED`.
 */
class OutboxRepository(
    private val jdbc: JdbcTemplate,
    private val table: String,
    private val clock: Clock = Clock.systemUTC(),
) {
    init {
        require(table.matches(Regex("[A-Za-z_][A-Za-z0-9_.]*"))) { "invalid outbox table name '$table'" }
    }

    fun createSchemaIfMissing() {
        schemaSql(table).split(";").map { it.trim() }.filter { it.isNotEmpty() }.forEach { jdbc.execute(it) }
    }

    fun insert(r: OutboxRecord) {
        jdbc.update(
            """INSERT INTO $table (id, kind, client_name, operation, payload, callback_url, callback_event, dedupe_key,
               correlation_id, status, attempts, max_attempts, next_attempt_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            r.id, r.kind.name, r.clientName, r.operation, r.payload, r.callbackUrl, r.callbackEvent, r.dedupeKey,
            r.correlationId, r.status.name, r.attempts, r.maxAttempts, ts(r.nextAttemptAt), ts(r.createdAt), ts(r.updatedAt),
        )
    }

    fun findById(id: String): OutboxRecord? =
        jdbc.query("SELECT * FROM $table WHERE id = ?", mapper, id).firstOrNull()

    fun findByDedupeKey(key: String): OutboxRecord? =
        jdbc.query("SELECT * FROM $table WHERE dedupe_key = ?", mapper, key).firstOrNull()

    /** Due PENDING rows, oldest first. */
    fun findDue(limit: Int): List<OutboxEnvelopeRow> {
        val now = ts(clock.instant())
        return jdbc.query(
            { con ->
                con.prepareStatement(
                    "SELECT id, kind, client_name, dedupe_key FROM $table WHERE status = 'PENDING' AND next_attempt_at <= ? ORDER BY next_attempt_at",
                ).apply { setTimestamp(1, now); maxRows = limit }
            },
        ) { rs, _ -> OutboxEnvelopeRow(rs.getString("id"), OutboxKind.valueOf(rs.getString("kind")), rs.getString("client_name"), rs.getString("dedupe_key")) }
    }

    /** Atomically moves a row between states. @return true when this caller won the row. */
    fun transition(id: String, from: OutboxStatus, to: OutboxStatus, lockedUntil: Instant?): Boolean =
        jdbc.update(
            "UPDATE $table SET status = ?, locked_until = ?, updated_at = ? WHERE id = ? AND status = ?",
            to.name, lockedUntil?.let(::ts), ts(clock.instant()), id, from.name,
        ) == 1

    fun reschedule(id: String, attempts: Int, nextAttemptAt: Instant, errorKind: String?, error: String?, responseStatus: Int?, responseBody: String?): Boolean =
        jdbc.update(
            """UPDATE $table SET status = 'PENDING', attempts = ?, next_attempt_at = ?, locked_until = NULL,
               last_error_kind = ?, last_error = ?, response_status = ?, response_body = ?, updated_at = ?
               WHERE id = ? AND status = 'PROCESSING'""",
            attempts, ts(nextAttemptAt), errorKind, error, responseStatus, responseBody, ts(clock.instant()), id,
        ) == 1

    fun complete(id: String, status: OutboxStatus, attempts: Int, errorKind: String?, error: String?, responseStatus: Int?, responseBody: String?): Boolean =
        jdbc.update(
            // COALESCE keeps the last failure visible on rows that eventually succeeded.
            """UPDATE $table SET status = ?, attempts = ?, locked_until = NULL,
               last_error_kind = COALESCE(?, last_error_kind), last_error = COALESCE(?, last_error),
               response_status = ?, response_body = ?, updated_at = ? WHERE id = ? AND status = 'PROCESSING'""",
            status.name, attempts, errorKind, error, responseStatus, responseBody, ts(clock.instant()), id,
        ) == 1

    /** Puts rows whose worker or transport vanished (crash, lost message) back in the queue. */
    fun recoverExpiredLeases(): Int =
        jdbc.update(
            "UPDATE $table SET status = 'PENDING', locked_until = NULL, updated_at = ? WHERE status IN ('QUEUED', 'PROCESSING') AND locked_until < ?",
            ts(clock.instant()), ts(clock.instant()),
        )

    fun cancel(id: String): Boolean =
        jdbc.update("UPDATE $table SET status = 'CANCELLED', updated_at = ? WHERE id = ? AND status = 'PENDING'", ts(clock.instant()), id) == 1

    /** Manual replay of a FAILED/DEAD row, e.g. after fixing a bad config. */
    fun requeue(id: String): Boolean =
        jdbc.update(
            "UPDATE $table SET status = 'PENDING', attempts = 0, next_attempt_at = ?, updated_at = ? WHERE id = ? AND status IN ('FAILED', 'DEAD')",
            ts(clock.instant()), ts(clock.instant()), id,
        ) == 1

    fun deleteTerminalBefore(cutoff: Instant): Int =
        jdbc.update("DELETE FROM $table WHERE status IN ('SUCCEEDED', 'FAILED', 'DEAD', 'CANCELLED') AND updated_at < ?", ts(cutoff))

    fun countByStatus(): Map<OutboxStatus, Long> =
        jdbc.query("SELECT status, COUNT(*) AS c FROM $table GROUP BY status") { rs, _ -> OutboxStatus.valueOf(rs.getString(1)) to rs.getLong(2) }.toMap()

    private val mapper = RowMapper { rs: ResultSet, _: Int ->
        OutboxRecord(
            id = rs.getString("id"),
            kind = OutboxKind.valueOf(rs.getString("kind")),
            clientName = rs.getString("client_name"),
            operation = rs.getString("operation"),
            payload = rs.getString("payload"),
            callbackUrl = rs.getString("callback_url"),
            callbackEvent = rs.getString("callback_event"),
            dedupeKey = rs.getString("dedupe_key"),
            correlationId = rs.getString("correlation_id"),
            status = OutboxStatus.valueOf(rs.getString("status")),
            attempts = rs.getInt("attempts"),
            maxAttempts = rs.getInt("max_attempts"),
            nextAttemptAt = rs.getTimestamp("next_attempt_at").toInstant(),
            lockedUntil = rs.getTimestamp("locked_until")?.toInstant(),
            lastErrorKind = rs.getString("last_error_kind"),
            lastError = rs.getString("last_error"),
            responseStatus = rs.getInt("response_status").takeUnless { rs.wasNull() },
            responseBody = rs.getString("response_body"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
            updatedAt = rs.getTimestamp("updated_at").toInstant(),
        )
    }

    private fun ts(i: Instant) = Timestamp.from(i)

    data class OutboxEnvelopeRow(val id: String, val kind: OutboxKind, val client: String, val dedupeKey: String?)

    companion object {
        /** Portable DDL (PostgreSQL, H2). For MySQL/Oracle use `db/ext-api-outbox.sql` as a migration starting point. */
        fun schemaSql(table: String) = """
            CREATE TABLE IF NOT EXISTS $table (
              id              VARCHAR(36)   PRIMARY KEY,
              kind            VARCHAR(16)   NOT NULL,
              client_name     VARCHAR(100)  NOT NULL,
              operation       VARCHAR(200),
              payload         TEXT          NOT NULL,
              callback_url    VARCHAR(2000),
              callback_event  VARCHAR(200),
              dedupe_key      VARCHAR(200),
              correlation_id  VARCHAR(100),
              status          VARCHAR(16)   NOT NULL,
              attempts        INT           NOT NULL,
              max_attempts    INT           NOT NULL,
              next_attempt_at TIMESTAMP     NOT NULL,
              locked_until    TIMESTAMP,
              last_error_kind VARCHAR(40),
              last_error      TEXT,
              response_status INT,
              response_body   TEXT,
              created_at      TIMESTAMP     NOT NULL,
              updated_at      TIMESTAMP     NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_${table.replace('.', '_')}_dedupe ON $table (dedupe_key);
            CREATE INDEX IF NOT EXISTS ix_${table.replace('.', '_')}_due ON $table (status, next_attempt_at)
        """.trimIndent()
    }
}
