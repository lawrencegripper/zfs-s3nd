-- +goose Up
PRAGMA foreign_keys = ON;

CREATE TABLE admin_credentials (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    password_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE app_settings (
    key TEXT PRIMARY KEY CHECK(key IN ('upload_throughput_limit_mbit', 'max_incremental_chain_depth')),
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE ssh_keys (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    public_key TEXT NOT NULL,
    fingerprint_sha256 TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_used_at TEXT
);

CREATE TABLE sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seen_at TEXT
);

CREATE TABLE pools (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seen_at TEXT,
    UNIQUE(source_id, name)
);

CREATE TABLE datasets (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    pool_id TEXT NOT NULL REFERENCES pools(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seen_at TEXT,
    UNIQUE(source_id, pool_id, path)
);

CREATE TABLE snapshots (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    parent_snapshot_id TEXT REFERENCES snapshots(id),
    manifest_object_key TEXT,
    status TEXT NOT NULL CHECK(status IN ('pending', 'uploading', 'committed', 'failed', 'aborted', 'deleting')),
    stream_validation_status TEXT NOT NULL DEFAULT 'pending',
    chain_validation_status TEXT NOT NULL DEFAULT 'pending',
    logical_bytes INTEGER,
    stored_bytes INTEGER NOT NULL DEFAULT 0,
    stream_sha256 TEXT,
    stream_from_guid TEXT NOT NULL DEFAULT '',
    stream_to_guid TEXT NOT NULL DEFAULT '',
    chunk_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    upload_started_at TEXT,
    upload_completed_at TEXT,
    failure_reason TEXT,
    UNIQUE(dataset_id, name)
);

CREATE TABLE upload_sessions (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    snapshot_id TEXT REFERENCES snapshots(id) ON DELETE SET NULL,
    target_snapshot_name TEXT NOT NULL,
    base_snapshot_id TEXT REFERENCES snapshots(id),
    status TEXT NOT NULL CHECK(status IN ('pending', 'uploading', 'writing_chunk', 'committing_manifest', 'committing_catalog', 'complete', 'failed', 'aborted')),
    chunk_size_bytes INTEGER NOT NULL,
    current_chunk_index INTEGER NOT NULL DEFAULT 0,
    chunks_completed INTEGER NOT NULL DEFAULT 0,
    bytes_received INTEGER NOT NULL DEFAULT 0,
    stream_sha256 TEXT,
    manifest_object_key TEXT,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_heartbeat_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at TEXT,
    failure_reason TEXT
);

CREATE TABLE snapshot_chunks (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    upload_session_id TEXT NOT NULL REFERENCES upload_sessions(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    object_key TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    zfs_offset_start INTEGER NOT NULL,
    zfs_offset_end INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('writing', 'uploaded', 'verified', 'failed')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at TEXT,
    UNIQUE(snapshot_id, chunk_index),
    UNIQUE(object_key)
);

CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK(type IN ('upload', 'validation', 'reconciliation', 'cleanup', 'catalog_backup')),
    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'succeeded', 'failed')),
    source_id TEXT REFERENCES sources(id) ON DELETE SET NULL,
    pool_id TEXT REFERENCES pools(id) ON DELETE SET NULL,
    dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
    snapshot_id TEXT REFERENCES snapshots(id) ON DELETE SET NULL,
    upload_session_id TEXT REFERENCES upload_sessions(id) ON DELETE SET NULL,
    validation_job_id TEXT,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    completed_at TEXT,
    summary TEXT,
    failure_reason TEXT
);

CREATE TABLE validation_jobs (
    id TEXT PRIMARY KEY,
    snapshot_id TEXT REFERENCES snapshots(id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK(type IN ('stream_check', 'restore_check')),
    executor TEXT NOT NULL CHECK(executor IN ('local', 'docker', 'railway_sandbox', 'external_vm')),
    status TEXT NOT NULL,
    logs_object_key TEXT,
    started_at TEXT,
    completed_at TEXT,
    result_summary TEXT
);

CREATE TABLE catalog_backups (
    id TEXT PRIMARY KEY,
    operation_id TEXT REFERENCES operations(id) ON DELETE SET NULL,
    object_key TEXT NOT NULL,
    metadata_object_key TEXT,
    size_bytes INTEGER NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running', 'succeeded', 'failed')),
    failure_reason TEXT
);

CREATE TABLE api_tokens (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    token_prefix TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_used_at TEXT,
    revoked_at TEXT
);

CREATE INDEX idx_snapshots_dataset_created ON snapshots(dataset_id, created_at DESC);
CREATE INDEX idx_snapshots_dataset_stream_to_guid ON snapshots(dataset_id, stream_to_guid) WHERE stream_to_guid != '';
CREATE INDEX idx_chunks_snapshot_index ON snapshot_chunks(snapshot_id, chunk_index);
CREATE INDEX idx_upload_sessions_status ON upload_sessions(status);
CREATE INDEX idx_operations_started ON operations(started_at DESC);
CREATE INDEX idx_api_tokens_created ON api_tokens(created_at DESC);
CREATE INDEX idx_api_tokens_hash_active ON api_tokens(token_hash) WHERE revoked_at IS NULL;

-- +goose Down
DROP TABLE api_tokens;
DROP TABLE catalog_backups;
DROP TABLE validation_jobs;
DROP TABLE operations;
DROP TABLE snapshot_chunks;
DROP TABLE upload_sessions;
DROP TABLE snapshots;
DROP TABLE datasets;
DROP TABLE pools;
DROP TABLE sources;
DROP TABLE ssh_keys;
DROP TABLE app_settings;
DROP TABLE admin_credentials;
