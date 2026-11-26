-- Outbox table for ext-api async calls (PostgreSQL / H2).
-- Copy into your Flyway/Liquibase migrations and set ext-api.async.initialize-schema=false.
-- MySQL: drop 'IF NOT EXISTS' from the CREATE INDEX lines. Oracle: use VARCHAR2/CLOB instead of TEXT.

CREATE TABLE IF NOT EXISTS ext_api_outbox (
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
CREATE UNIQUE INDEX IF NOT EXISTS ux_ext_api_outbox_dedupe ON ext_api_outbox (dedupe_key);
CREATE INDEX IF NOT EXISTS ix_ext_api_outbox_due ON ext_api_outbox (status, next_attempt_at);
