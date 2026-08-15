-- name: EnsureAdminCredentials :exec
INSERT OR IGNORE INTO admin_credentials (singleton, password_hash)
VALUES (1, ?);

-- name: GetAdminCredentials :one
SELECT singleton, password_hash, created_at
FROM admin_credentials
WHERE singleton = 1;

-- name: UpdateAdminPasswordHash :exec
INSERT INTO admin_credentials (singleton, password_hash)
VALUES (1, ?)
ON CONFLICT(singleton) DO UPDATE SET password_hash = excluded.password_hash;

-- name: CreateAPIToken :exec
INSERT INTO api_tokens (id, name, token_hash, token_prefix)
VALUES (?, ?, ?, ?);

-- name: ListAPITokens :many
SELECT id, name, token_prefix, created_at, last_used_at, revoked_at
FROM api_tokens
ORDER BY created_at DESC;

-- name: GetActiveAPITokenByHash :one
SELECT id, name, token_prefix, created_at, last_used_at, revoked_at
FROM api_tokens
WHERE token_hash = ?
  AND revoked_at IS NULL;

-- name: TouchAPIToken :exec
UPDATE api_tokens
SET last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: RevokeAPIToken :exec
UPDATE api_tokens
SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: GetSSHKeyByFingerprint :one
SELECT id, name, public_key, fingerprint_sha256, created_at, last_used_at
FROM ssh_keys
WHERE fingerprint_sha256 = ?;

-- name: ListSSHKeys :many
SELECT id, name, public_key, fingerprint_sha256, created_at, last_used_at
FROM ssh_keys
ORDER BY created_at DESC;

-- name: ListAppSettings :many
SELECT key, value, updated_at
FROM app_settings
ORDER BY key;

-- name: UpsertAppSetting :exec
INSERT INTO app_settings (key, value)
VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now');

-- name: DeleteAppSetting :exec
DELETE FROM app_settings
WHERE key = ?;

-- name: CreateSSHKey :exec
INSERT INTO ssh_keys (id, name, public_key, fingerprint_sha256)
VALUES (?, ?, ?, ?);

-- name: TouchSSHKey :exec
UPDATE ssh_keys
SET last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: DeleteSSHKey :exec
DELETE FROM ssh_keys
WHERE id = ?;

-- name: CreateSource :exec
INSERT INTO sources (id, name, description, last_seen_at)
VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

-- name: GetSourceByName :one
SELECT id, name, description, created_at, last_seen_at
FROM sources
WHERE name = ?;

-- name: TouchSource :exec
UPDATE sources
SET last_seen_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: CreatePool :exec
INSERT INTO pools (id, source_id, name, last_seen_at)
VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

-- name: GetPoolByName :one
SELECT id, source_id, name, created_at, last_seen_at
FROM pools
WHERE source_id = ? AND name = ?;

-- name: TouchPool :exec
UPDATE pools
SET last_seen_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: CreateDataset :exec
INSERT INTO datasets (id, source_id, pool_id, path, display_name, last_seen_at)
VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

-- name: GetDatasetByPath :one
SELECT id, source_id, pool_id, path, display_name, created_at, last_seen_at
FROM datasets
WHERE source_id = ? AND pool_id = ? AND path = ?;

-- name: TouchDataset :exec
UPDATE datasets
SET last_seen_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: GetSnapshotByName :one
SELECT id, dataset_id, name, parent_snapshot_id, manifest_object_key, status,
       stream_validation_status, chain_validation_status,
       logical_bytes, stored_bytes, stream_sha256, stream_from_guid, stream_to_guid, chunk_count, created_at, upload_started_at,
       upload_completed_at, failure_reason
FROM snapshots
WHERE dataset_id = ? AND name = ?;

-- name: GetCommittedSnapshotByToGUID :one
SELECT s.id, s.name, s.status, s.stream_to_guid
FROM snapshots s
JOIN datasets d ON d.id = s.dataset_id
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
WHERE src.name = ?
  AND p.name = ?
  AND d.path = ?
  AND s.stream_to_guid = ?
  AND s.status = 'committed';

-- name: InvalidateSnapshotForDeletion :exec
UPDATE snapshots
SET status = 'deleting',
    stream_validation_status = 'failed',
    chain_validation_status = 'failed',
    failure_reason = 'deletion in progress'
WHERE id = ?;

-- name: InvalidateDatasetSnapshotsForDeletion :exec
UPDATE snapshots
SET status = 'deleting',
    stream_validation_status = 'failed',
    chain_validation_status = 'failed',
    failure_reason = 'deletion in progress'
WHERE dataset_id = ?;

-- name: DeleteSnapshot :exec
DELETE FROM snapshots
WHERE id = ?;

-- name: DeleteDataset :exec
DELETE FROM datasets
WHERE id = ?;

-- name: DeleteUploadSessionsBySnapshot :exec
DELETE FROM upload_sessions
WHERE snapshot_id = ?;

-- name: ListSnapshotObjectKeys :many
SELECT snapshot_chunks.object_key
FROM snapshot_chunks
WHERE snapshot_chunks.snapshot_id = ?
UNION
SELECT manifest_object_key AS object_key
FROM snapshots
WHERE snapshots.id = ?
  AND snapshots.manifest_object_key IS NOT NULL
  AND snapshots.manifest_object_key != '';

-- name: ListDatasetObjectKeys :many
SELECT sc.object_key
FROM snapshot_chunks sc
JOIN snapshots s ON s.id = sc.snapshot_id
WHERE s.dataset_id = ?
UNION
SELECT s.manifest_object_key AS object_key
FROM snapshots s
WHERE s.dataset_id = ?
  AND s.manifest_object_key IS NOT NULL
  AND s.manifest_object_key != '';

-- name: CreatePendingSnapshot :exec
INSERT INTO snapshots (id, dataset_id, name, parent_snapshot_id, status, upload_started_at)
VALUES (?, ?, ?, ?, 'uploading', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

-- name: CreateUploadSession :exec
INSERT INTO upload_sessions (id, dataset_id, snapshot_id, target_snapshot_name, base_snapshot_id, status, chunk_size_bytes)
VALUES (?, ?, ?, ?, ?, 'uploading', ?);

-- name: UpdateUploadProgress :exec
UPDATE upload_sessions
SET status = ?, current_chunk_index = ?, chunks_completed = ?, bytes_received = ?, last_heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: UpdateUploadStatus :exec
UPDATE upload_sessions
SET status = ?, last_heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: SetUploadManifestObjectKey :exec
UPDATE upload_sessions
SET status = 'committing_catalog', manifest_object_key = ?, last_heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: CompleteUploadSession :exec
UPDATE upload_sessions
SET status = 'complete', bytes_received = ?, chunks_completed = ?, stream_sha256 = ?, manifest_object_key = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: ListStaleUploadSessions :many
SELECT id, dataset_id, snapshot_id, target_snapshot_name, base_snapshot_id, status,
       chunk_size_bytes, current_chunk_index, chunks_completed, bytes_received, stream_sha256,
       manifest_object_key, started_at, last_heartbeat_at, completed_at, failure_reason
FROM upload_sessions
WHERE status IN ('pending', 'uploading', 'writing_chunk', 'committing_manifest', 'committing_catalog')
  AND last_heartbeat_at < ?
ORDER BY last_heartbeat_at ASC
LIMIT ?;

-- name: ListAbandonedUploadSessions :many
SELECT id, dataset_id, snapshot_id, target_snapshot_name, base_snapshot_id, status,
       chunk_size_bytes, current_chunk_index, chunks_completed, bytes_received, stream_sha256,
       manifest_object_key, started_at, last_heartbeat_at, completed_at, failure_reason
FROM upload_sessions
WHERE status IN ('failed', 'aborted')
  AND coalesce(completed_at, last_heartbeat_at, started_at) < ?
ORDER BY coalesce(completed_at, last_heartbeat_at, started_at) ASC
LIMIT ?;

-- name: DeleteUploadSession :exec
DELETE FROM upload_sessions
WHERE id = ?;

-- name: FailUploadSession :exec
UPDATE upload_sessions
SET status = 'failed', failure_reason = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: CreateSnapshotChunk :exec
INSERT INTO snapshot_chunks (id, snapshot_id, upload_session_id, chunk_index, object_key, size_bytes, zfs_offset_start, zfs_offset_end, sha256, status, completed_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

-- name: ListSnapshotChunks :many
SELECT id, snapshot_id, upload_session_id, chunk_index, object_key, size_bytes, zfs_offset_start, zfs_offset_end, sha256, status, created_at, completed_at
FROM snapshot_chunks
WHERE snapshot_id = ?
ORDER BY chunk_index ASC;

-- name: ListUploadSessionChunks :many
SELECT id, snapshot_id, upload_session_id, chunk_index, object_key, size_bytes, zfs_offset_start, zfs_offset_end, sha256, status, created_at, completed_at
FROM snapshot_chunks
WHERE upload_session_id = ?
ORDER BY chunk_index ASC;

-- name: CommitSnapshot :exec
UPDATE snapshots
SET status = 'committed', manifest_object_key = ?, logical_bytes = ?, stored_bytes = ?, stream_sha256 = ?, stream_from_guid = ?, stream_to_guid = ?, chunk_count = ?, upload_completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: FailSnapshot :exec
UPDATE snapshots
SET status = 'failed', failure_reason = ?
WHERE id = ?;

-- name: CreateOperation :exec
INSERT INTO operations (id, type, status, source_id, pool_id, dataset_id, snapshot_id, upload_session_id, summary)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpdateOperationStatus :exec
UPDATE operations
SET status = ?, summary = ?, failure_reason = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE id = ?;

-- name: MarkOperationRunning :exec
UPDATE operations
SET status = 'running', summary = ?, failure_reason = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), completed_at = NULL
WHERE id = ?;

-- name: ListPendingCleanupOperations :many
SELECT id, dataset_id, snapshot_id
FROM operations
WHERE type = 'cleanup'
  AND status IN ('queued', 'running')
ORDER BY started_at ASC
LIMIT ?;

-- name: FailOperationByUploadSession :exec
UPDATE operations
SET status = 'failed', summary = ?, failure_reason = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE upload_session_id = ?
  AND status IN ('queued', 'running');

-- name: CreateCatalogBackup :exec
INSERT INTO catalog_backups (id, operation_id, object_key, metadata_object_key, size_bytes, checksum_sha256, started_at, completed_at, status, failure_reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetLatestCatalogBackup :one
SELECT id, operation_id, object_key, metadata_object_key, size_bytes, checksum_sha256, started_at, completed_at, status, failure_reason
FROM catalog_backups
ORDER BY started_at DESC
LIMIT 1;

-- name: ListCatalogBackupsOlderThan :many
SELECT id, operation_id, object_key, metadata_object_key, size_bytes, checksum_sha256, started_at, completed_at, status, failure_reason
FROM catalog_backups
WHERE started_at < ?
ORDER BY started_at ASC
LIMIT ?;

-- name: DeleteCatalogBackup :exec
DELETE FROM catalog_backups
WHERE id = ?;

-- name: GetLatestOperations :many
SELECT id, type, status, started_at, updated_at, completed_at, summary, failure_reason
FROM operations
WHERE type != 'catalog_backup'
ORDER BY started_at DESC
LIMIT ?;

-- name: ListActiveUploads :many
SELECT u.id AS upload_session_id,
       src.name AS source_name,
       p.name AS pool_name,
       d.path AS dataset_path,
       u.target_snapshot_name,
       u.status,
       u.bytes_received,
       u.chunks_completed,
       u.started_at,
       u.last_heartbeat_at
FROM upload_sessions u
JOIN datasets d ON d.id = u.dataset_id
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
WHERE u.status IN ('pending', 'uploading', 'writing_chunk', 'committing_manifest', 'committing_catalog')
ORDER BY u.last_heartbeat_at DESC;

-- name: ListSnapshotsDueForValidation :many
SELECT s.id
FROM snapshots s
WHERE s.status = 'committed'
  AND (
    s.chain_validation_status != 'succeeded'
    OR NOT EXISTS (
      SELECT 1
      FROM validation_jobs v
      WHERE v.snapshot_id = s.id
        AND v.type = 'restore_check'
        AND v.status = 'succeeded'
        AND v.completed_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-24 hours')
    )
  )
ORDER BY coalesce(s.upload_completed_at, s.created_at) ASC
LIMIT ?;

-- name: CreateValidationJob :exec
INSERT INTO validation_jobs (id, snapshot_id, type, executor, status, started_at, result_summary)
VALUES (?, ?, ?, ?, 'running', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?);

-- name: CompleteValidationJob :exec
UPDATE validation_jobs
SET status = 'succeeded', completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), result_summary = ?
WHERE id = ?;

-- name: FailValidationJob :exec
UPDATE validation_jobs
SET status = 'failed', completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), result_summary = ?
WHERE id = ?;

-- name: UpdateSnapshotStreamValidationStatus :exec
UPDATE snapshots
SET stream_validation_status = ?
WHERE id = ?;

-- name: UpdateSnapshotChainValidationStatus :exec
UPDATE snapshots
SET chain_validation_status = ?
WHERE id = ?;

-- name: CreateValidationOperation :exec
INSERT INTO operations (id, type, status, snapshot_id, validation_job_id, summary)
VALUES (?, 'validation', 'running', ?, ?, ?);

-- name: CountFailedValidations :one
SELECT count(*) AS count
FROM snapshots
WHERE status = 'committed'
  AND chain_validation_status = 'failed';

-- name: ListLatestValidationJobs :many
SELECT v.id,
       v.snapshot_id,
       v.type,
       v.executor,
       v.status,
       v.started_at,
       v.completed_at,
       v.result_summary,
       s.name AS snapshot_name,
       d.path AS dataset_path,
       src.name AS source_name,
       p.name AS pool_name
FROM validation_jobs v
JOIN snapshots s ON s.id = v.snapshot_id
JOIN datasets d ON d.id = s.dataset_id
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
ORDER BY coalesce(v.completed_at, v.started_at, '') DESC
LIMIT ?;

-- name: CountCommittedSnapshots :one
SELECT count(*) AS count
FROM snapshots
WHERE status = 'committed';

-- name: CountSources :one
SELECT count(*) AS count
FROM sources;

-- name: CountDatasets :one
SELECT count(*) AS count
FROM datasets;

-- name: CountFailedUploads :one
SELECT count(*) AS count
FROM upload_sessions
WHERE status = 'failed';

-- name: ListDatasetSummaries :many
SELECT d.id AS dataset_id,
       src.name AS source_name,
       p.name AS pool_name,
       d.path AS dataset_path,
       count(CASE WHEN s.status = 'committed' THEN 1 END) AS snapshot_count,
       CAST(coalesce((
         SELECT latest.name
         FROM snapshots latest
         WHERE latest.dataset_id = d.id
         ORDER BY coalesce(latest.upload_completed_at, latest.created_at) DESC
         LIMIT 1
       ), '') AS TEXT) AS latest_snapshot_name,
       CAST(coalesce((
         SELECT valid.name
         FROM snapshots valid
         WHERE valid.dataset_id = d.id
           AND valid.status = 'committed'
           AND valid.chain_validation_status = 'succeeded'
         ORDER BY coalesce(valid.upload_completed_at, valid.created_at) DESC
         LIMIT 1
       ), '') AS TEXT) AS latest_chain_valid_snapshot_name,
       CAST(coalesce(sum(CASE WHEN s.status = 'committed' AND s.chain_validation_status = 'failed' THEN 1 ELSE 0 END), 0) AS INTEGER) AS failed_validation_count,
       CAST(coalesce(sum(CASE WHEN s.status = 'committed' THEN s.stored_bytes ELSE 0 END), 0) AS INTEGER) AS stored_bytes,
       CAST(coalesce(max(s.upload_completed_at), '') AS TEXT) AS latest_upload_at
FROM datasets d
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
LEFT JOIN snapshots s ON s.dataset_id = d.id
GROUP BY d.id, src.name, p.name, d.path
ORDER BY latest_upload_at DESC, source_name ASC, pool_name ASC, dataset_path ASC;

-- name: GetDatasetDetail :one
SELECT d.id AS dataset_id,
       src.name AS source_name,
       p.name AS pool_name,
       d.path AS dataset_path,
       d.created_at,
       d.last_seen_at
FROM datasets d
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
WHERE d.id = ?;

-- name: ListCommittedSnapshotsForIdentity :many
SELECT s.id,
       s.name,
       parent.name AS parent_snapshot_name,
       s.manifest_object_key,
       s.status,
       s.stream_validation_status,
       s.chain_validation_status,
       s.stream_sha256,
       s.stream_from_guid,
       s.stream_to_guid,
       s.chunk_count,
       s.upload_completed_at
FROM snapshots s
JOIN datasets d ON d.id = s.dataset_id
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
LEFT JOIN snapshots parent ON parent.id = s.parent_snapshot_id
WHERE src.name = ?
  AND p.name = ?
  AND d.path = ?
  AND s.status = 'committed'
  AND s.manifest_object_key IS NOT NULL
ORDER BY s.created_at ASC;

-- name: ListDatasetSnapshots :many
SELECT id, name, parent_snapshot_id, manifest_object_key, status,
       stream_validation_status, chain_validation_status,
       stored_bytes, stream_sha256, stream_from_guid, stream_to_guid, chunk_count, upload_started_at, upload_completed_at, failure_reason
FROM snapshots
WHERE dataset_id = ?
ORDER BY created_at DESC;

-- name: GetSnapshotDetail :one
SELECT s.id AS snapshot_id,
       s.name AS snapshot_name,
       s.parent_snapshot_id,
       s.manifest_object_key,
       s.status,
       s.stream_validation_status,
       s.chain_validation_status,
       s.logical_bytes,
       s.stored_bytes,
       s.stream_sha256,
       s.stream_from_guid,
       s.stream_to_guid,
       s.chunk_count,
       s.upload_started_at,
       s.upload_completed_at,
       s.failure_reason,
       d.id AS dataset_id,
       d.path AS dataset_path,
       src.name AS source_name,
       p.name AS pool_name
FROM snapshots s
JOIN datasets d ON d.id = s.dataset_id
JOIN sources src ON src.id = d.source_id
JOIN pools p ON p.id = d.pool_id
WHERE s.id = ?;

-- name: ListSnapshotRestoreChain :many
WITH RECURSIVE chain(depth, id, name, parent_snapshot_id, manifest_object_key, status) AS (
  SELECT 0, s.id, s.name, s.parent_snapshot_id, s.manifest_object_key, s.status
  FROM snapshots s
  WHERE s.id = ?
  UNION ALL
  SELECT chain.depth + 1, parent.id, parent.name, parent.parent_snapshot_id, parent.manifest_object_key, parent.status
  FROM snapshots parent
  JOIN chain ON chain.parent_snapshot_id = parent.id
)
SELECT id, name, parent_snapshot_id, manifest_object_key, status
FROM chain
ORDER BY depth DESC;

-- name: ListCommittedChildSnapshots :many
SELECT id, name
FROM snapshots
WHERE parent_snapshot_id = ?
  AND status = 'committed'
ORDER BY created_at ASC;

-- name: CountCommittedDescendants :one
WITH RECURSIVE descendants(id) AS (
  SELECT s.id
  FROM snapshots s
  WHERE s.parent_snapshot_id = ?
    AND s.status = 'committed'
  UNION ALL
  SELECT child.id
  FROM snapshots child
  JOIN descendants ON child.parent_snapshot_id = descendants.id
  WHERE child.status = 'committed'
)
SELECT count(*) AS count
FROM descendants;

-- name: SumStoredBytes :one
SELECT CAST(coalesce(sum(stored_bytes), 0) AS INTEGER) AS stored_bytes
FROM snapshots
WHERE status = 'committed';
