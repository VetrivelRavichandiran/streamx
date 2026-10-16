-- STREAMX metadata store — PostgreSQL DDL (CONTRACTS.md §4).
-- Idempotent: every statement uses IF NOT EXISTS / ON CONFLICT.

CREATE TABLE IF NOT EXISTS organizations (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    id          TEXT PRIMARY KEY,
    org_id      TEXT NOT NULL REFERENCES organizations(id),
    username    TEXT NOT NULL UNIQUE,
    role        TEXT NOT NULL,
    password    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tokens (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role        TEXT NOT NULL,
    token       TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tokens_token ON tokens(token);
CREATE INDEX IF NOT EXISTS idx_tokens_expires ON tokens(expires_at);

CREATE TABLE IF NOT EXISTS topics (
    name                TEXT PRIMARY KEY,
    partitions          INTEGER NOT NULL,
    replication_factor  INTEGER NOT NULL,
    retention_max_age_ms  BIGINT NOT NULL DEFAULT 0,
    retention_max_bytes   BIGINT NOT NULL DEFAULT 0,
    schema_json         TEXT NOT NULL DEFAULT '[]',
    schema_enabled      BOOLEAN NOT NULL DEFAULT FALSE,
    strategy            TEXT NOT NULL DEFAULT 'keyed',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS topic_partitions (
    topic       TEXT NOT NULL REFERENCES topics(name) ON DELETE CASCADE,
    partition   INTEGER NOT NULL,
    leader      TEXT NOT NULL,
    replicas    TEXT NOT NULL, -- comma-joined broker ids
    PRIMARY KEY (topic, partition)
);

CREATE TABLE IF NOT EXISTS brokers (
    id          TEXT PRIMARY KEY,
    addr        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'unknown',
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consumer_groups (
    id          TEXT PRIMARY KEY,
    generation  INTEGER NOT NULL DEFAULT 1,
    topic       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS consumers (
    id          TEXT NOT NULL,
    "group"     TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'up',
    assignment  TEXT NOT NULL DEFAULT '[]', -- JSON array of int32
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("group", id)
);

CREATE TABLE IF NOT EXISTS offsets (
    "group"     TEXT NOT NULL,
    topic       TEXT NOT NULL,
    partition   INTEGER NOT NULL,
    offset      BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("group", topic, partition)
);

CREATE TABLE IF NOT EXISTS dlq_entries (
    id              TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    "group"         TEXT NOT NULL,
    original_topic  TEXT NOT NULL,
    original_part   INTEGER NOT NULL,
    original_offset BIGINT NOT NULL,
    key             TEXT NOT NULL DEFAULT '',
    payload         BYTEA NOT NULL,
    reason          TEXT NOT NULL DEFAULT '',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dlq_topic ON dlq_entries(topic);
CREATE INDEX IF NOT EXISTS idx_dlq_group ON dlq_entries("group");
CREATE INDEX IF NOT EXISTS idx_dlq_created ON dlq_entries(created_at DESC);

CREATE TABLE IF NOT EXISTS benchmarks (
    id              TEXT PRIMARY KEY,
    scenario        TEXT NOT NULL,
    events          INTEGER NOT NULL,
    payload_bytes   INTEGER NOT NULL,
    producers       INTEGER NOT NULL,
    consumers       INTEGER NOT NULL,
    partitions      INTEGER NOT NULL,
    concurrency     INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS benchmark_results (
    id          TEXT PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES benchmarks(id) ON DELETE CASCADE,
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    unit        TEXT NOT NULL DEFAULT '',
    labels      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bench_results_run ON benchmark_results(run_id);

CREATE TABLE IF NOT EXISTS audit_logs (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    action      TEXT NOT NULL,
    target      TEXT NOT NULL DEFAULT '',
    detail      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at DESC);