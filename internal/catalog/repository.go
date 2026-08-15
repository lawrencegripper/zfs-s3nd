package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

type Repository struct {
	database db.DBTX
	q        *db.Queries
}

func NewRepository(database db.DBTX) *Repository {
	return &Repository{database: database, q: db.New(database)}
}

type txBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func (r *Repository) withTx(ctx context.Context, fn func(*db.Queries) error) error {
	beginner, ok := r.database.(txBeginner)
	if !ok {
		return fn(r.q)
	}
	tx, err := beginner.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(r.q.WithTx(tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type DatasetIdentity struct {
	Source                   string
	Pool                     string
	Dataset                  string
	Snapshot                 string
	BaseSnapshot             string
	MaxIncrementalChainDepth int64
}

type ResolvedDataset struct {
	SourceID  string
	PoolID    string
	DatasetID string
}

type StartedUpload struct {
	SourceID        string
	PoolID          string
	DatasetID       string
	SnapshotID      string
	UploadSessionID string
	OperationID     string
	BaseSnapshotID  sql.NullString
}

type ChunkRecord struct {
	ID          string
	Index       int64
	ObjectKey   string
	SizeBytes   int64
	OffsetStart int64
	OffsetEnd   int64
	SHA256      string
}

type ReconcileResult struct {
	StaleUploadsFailed int
	OperationID        string
}

type AbandonedUpload struct {
	ID                string
	SnapshotID        sql.NullString
	ManifestObjectKey sql.NullString
}

type SnapshotRef struct {
	ID   string
	Name string
}

func (r *Repository) ResolveDataset(ctx context.Context, sourceName, poolName, datasetPath string) (ResolvedDataset, error) {
	source, err := r.q.GetSourceByName(ctx, sourceName)
	if errors.Is(err, sql.ErrNoRows) {
		source.ID = NewID("src")
		if err := r.q.CreateSource(ctx, db.CreateSourceParams{ID: source.ID, Name: sourceName, Description: ""}); err != nil {
			return ResolvedDataset{}, fmt.Errorf("create source: %w", err)
		}
	} else if err != nil {
		return ResolvedDataset{}, fmt.Errorf("get source: %w", err)
	} else if err := r.q.TouchSource(ctx, source.ID); err != nil {
		return ResolvedDataset{}, fmt.Errorf("touch source: %w", err)
	}

	pool, err := r.q.GetPoolByName(ctx, db.GetPoolByNameParams{SourceID: source.ID, Name: poolName})
	if errors.Is(err, sql.ErrNoRows) {
		pool.ID = NewID("pool")
		if err := r.q.CreatePool(ctx, db.CreatePoolParams{ID: pool.ID, SourceID: source.ID, Name: poolName}); err != nil {
			return ResolvedDataset{}, fmt.Errorf("create pool: %w", err)
		}
	} else if err != nil {
		return ResolvedDataset{}, fmt.Errorf("get pool: %w", err)
	} else if err := r.q.TouchPool(ctx, pool.ID); err != nil {
		return ResolvedDataset{}, fmt.Errorf("touch pool: %w", err)
	}

	dataset, err := r.q.GetDatasetByPath(ctx, db.GetDatasetByPathParams{SourceID: source.ID, PoolID: pool.ID, Path: datasetPath})
	if errors.Is(err, sql.ErrNoRows) {
		dataset.ID = NewID("ds")
		if err := r.q.CreateDataset(ctx, db.CreateDatasetParams{ID: dataset.ID, SourceID: source.ID, PoolID: pool.ID, Path: datasetPath, DisplayName: datasetPath}); err != nil {
			return ResolvedDataset{}, fmt.Errorf("create dataset: %w", err)
		}
	} else if err != nil {
		return ResolvedDataset{}, fmt.Errorf("get dataset: %w", err)
	} else if err := r.q.TouchDataset(ctx, dataset.ID); err != nil {
		return ResolvedDataset{}, fmt.Errorf("touch dataset: %w", err)
	}

	return ResolvedDataset{SourceID: source.ID, PoolID: pool.ID, DatasetID: dataset.ID}, nil
}

func (r *Repository) StartUpload(ctx context.Context, identity DatasetIdentity, chunkSize int64) (StartedUpload, error) {
	var started StartedUpload
	err := r.withTx(ctx, func(q *db.Queries) error {
		txRepo := &Repository{q: q}
		var err error
		started, err = txRepo.startUpload(ctx, identity, chunkSize)
		return err
	})
	return started, err
}

func (r *Repository) startUpload(ctx context.Context, identity DatasetIdentity, chunkSize int64) (StartedUpload, error) {
	resolved, err := r.ResolveDataset(ctx, identity.Source, identity.Pool, identity.Dataset)
	if err != nil {
		return StartedUpload{}, err
	}

	var baseID sql.NullString
	if identity.BaseSnapshot != "" {
		base, err := r.q.GetSnapshotByName(ctx, db.GetSnapshotByNameParams{DatasetID: resolved.DatasetID, Name: identity.BaseSnapshot})
		if err != nil {
			return StartedUpload{}, fmt.Errorf("base snapshot %q not found: %w", identity.BaseSnapshot, err)
		}
		if base.Status != "committed" {
			return StartedUpload{}, fmt.Errorf("base snapshot %q is %s, not committed", identity.BaseSnapshot, base.Status)
		}
		if identity.MaxIncrementalChainDepth > 0 {
			depth, err := r.ChainDepth(ctx, base.ID)
			if err != nil {
				return StartedUpload{}, fmt.Errorf("check base snapshot chain depth: %w", err)
			}
			incomingDepth := depth + 1
			if incomingDepth > identity.MaxIncrementalChainDepth {
				return StartedUpload{}, fmt.Errorf("incremental chain depth would be %d, exceeding MAX_INCREMENTAL_CHAIN_DEPTH=%d; send a full snapshot to create a new anchor", incomingDepth, identity.MaxIncrementalChainDepth)
			}
		}
		baseID = sql.NullString{String: base.ID, Valid: true}
	}

	if existing, err := r.q.GetSnapshotByName(ctx, db.GetSnapshotByNameParams{DatasetID: resolved.DatasetID, Name: identity.Snapshot}); err == nil {
		if existing.Status != "failed" && existing.Status != "aborted" {
			return StartedUpload{}, fmt.Errorf("snapshot %q already exists", identity.Snapshot)
		}
		if err := r.q.DeleteSnapshot(ctx, existing.ID); err != nil {
			return StartedUpload{}, fmt.Errorf("delete previous %s snapshot %q before retry: %w", existing.Status, identity.Snapshot, err)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return StartedUpload{}, fmt.Errorf("check snapshot: %w", err)
	}

	started := StartedUpload{
		SourceID:        resolved.SourceID,
		PoolID:          resolved.PoolID,
		DatasetID:       resolved.DatasetID,
		SnapshotID:      NewID("snap"),
		UploadSessionID: NewID("upload"),
		OperationID:     NewID("op"),
		BaseSnapshotID:  baseID,
	}
	if err := r.q.CreatePendingSnapshot(ctx, db.CreatePendingSnapshotParams{ID: started.SnapshotID, DatasetID: resolved.DatasetID, Name: identity.Snapshot, ParentSnapshotID: baseID}); err != nil {
		return StartedUpload{}, fmt.Errorf("create snapshot: %w", err)
	}
	if err := r.q.CreateUploadSession(ctx, db.CreateUploadSessionParams{ID: started.UploadSessionID, DatasetID: resolved.DatasetID, SnapshotID: sql.NullString{String: started.SnapshotID, Valid: true}, TargetSnapshotName: identity.Snapshot, BaseSnapshotID: baseID, ChunkSizeBytes: chunkSize}); err != nil {
		return StartedUpload{}, fmt.Errorf("create upload session: %w", err)
	}
	if err := r.q.CreateOperation(ctx, db.CreateOperationParams{ID: started.OperationID, Type: "upload", Status: "running", SourceID: sql.NullString{String: resolved.SourceID, Valid: true}, PoolID: sql.NullString{String: resolved.PoolID, Valid: true}, DatasetID: sql.NullString{String: resolved.DatasetID, Valid: true}, SnapshotID: sql.NullString{String: started.SnapshotID, Valid: true}, UploadSessionID: sql.NullString{String: started.UploadSessionID, Valid: true}, Summary: sql.NullString{String: "upload started", Valid: true}}); err != nil {
		return StartedUpload{}, fmt.Errorf("create operation: %w", err)
	}
	return started, nil
}

func (r *Repository) AddVerifiedChunk(ctx context.Context, snapshotID, uploadID string, chunk ChunkRecord) error {
	if chunk.ID == "" {
		chunk.ID = NewID("chunk")
	}
	return r.q.CreateSnapshotChunk(ctx, db.CreateSnapshotChunkParams{ID: chunk.ID, SnapshotID: snapshotID, UploadSessionID: uploadID, ChunkIndex: chunk.Index, ObjectKey: chunk.ObjectKey, SizeBytes: chunk.SizeBytes, ZfsOffsetStart: chunk.OffsetStart, ZfsOffsetEnd: chunk.OffsetEnd, Sha256: chunk.SHA256})
}

func (r *Repository) UpdateUploadProgress(ctx context.Context, uploadID string, currentIndex, completed, bytesReceived int64) error {
	return r.q.UpdateUploadProgress(ctx, db.UpdateUploadProgressParams{Status: "writing_chunk", CurrentChunkIndex: currentIndex, ChunksCompleted: completed, BytesReceived: bytesReceived, ID: uploadID})
}

func (r *Repository) UpdateUploadStatus(ctx context.Context, uploadID, status string) error {
	return r.q.UpdateUploadStatus(ctx, db.UpdateUploadStatusParams{Status: status, ID: uploadID})
}

func (r *Repository) SetUploadManifestObjectKey(ctx context.Context, uploadID, manifestKey string) error {
	return r.q.SetUploadManifestObjectKey(ctx, db.SetUploadManifestObjectKeyParams{ManifestObjectKey: sql.NullString{String: manifestKey, Valid: true}, ID: uploadID})
}

func (r *Repository) CommitUpload(ctx context.Context, snapshotID, uploadID, manifestKey string, logicalBytes, storedBytes int64, streamSHA256, streamFromGUID, streamToGUID string, chunkCount int64, operationID string) error {
	return r.withTx(ctx, func(q *db.Queries) error {
		if err := q.CommitSnapshot(ctx, db.CommitSnapshotParams{ManifestObjectKey: sql.NullString{String: manifestKey, Valid: true}, LogicalBytes: sql.NullInt64{Int64: logicalBytes, Valid: true}, StoredBytes: storedBytes, StreamSha256: sql.NullString{String: streamSHA256, Valid: true}, StreamFromGuid: streamFromGUID, StreamToGuid: streamToGUID, ChunkCount: chunkCount, ID: snapshotID}); err != nil {
			return fmt.Errorf("commit snapshot: %w", err)
		}
		if err := q.CompleteUploadSession(ctx, db.CompleteUploadSessionParams{BytesReceived: logicalBytes, ChunksCompleted: chunkCount, StreamSha256: sql.NullString{String: streamSHA256, Valid: true}, ManifestObjectKey: sql.NullString{String: manifestKey, Valid: true}, ID: uploadID}); err != nil {
			return fmt.Errorf("complete upload: %w", err)
		}
		return q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: "upload committed", Valid: true}, FailureReason: sql.NullString{}, ID: operationID})
	})
}

func (r *Repository) FailUpload(ctx context.Context, snapshotID, uploadID, operationID string, reason error) error {
	msg := reason.Error()
	return r.withTx(ctx, func(q *db.Queries) error {
		if err := q.FailSnapshot(ctx, db.FailSnapshotParams{FailureReason: sql.NullString{String: msg, Valid: true}, ID: snapshotID}); err != nil {
			return fmt.Errorf("fail snapshot: %w", err)
		}
		if err := q.FailUploadSession(ctx, db.FailUploadSessionParams{FailureReason: sql.NullString{String: msg, Valid: true}, ID: uploadID}); err != nil {
			return fmt.Errorf("fail upload session: %w", err)
		}
		if err := q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "failed", Summary: sql.NullString{String: "upload failed", Valid: true}, FailureReason: sql.NullString{String: msg, Valid: true}, ID: operationID}); err != nil {
			return fmt.Errorf("fail upload operation: %w", err)
		}
		return nil
	})
}

func (r *Repository) ListSnapshotChunks(ctx context.Context, snapshotID string) ([]db.SnapshotChunk, error) {
	return r.q.ListSnapshotChunks(ctx, snapshotID)
}

func (r *Repository) RestoreChain(ctx context.Context, snapshotID string) ([]db.ListSnapshotRestoreChainRow, error) {
	return r.q.ListSnapshotRestoreChain(ctx, snapshotID)
}

func (r *Repository) ChainDepth(ctx context.Context, snapshotID string) (int64, error) {
	chain, err := r.RestoreChain(ctx, snapshotID)
	if err != nil {
		return 0, err
	}
	return int64(len(chain)), nil
}

func (r *Repository) NextCommittedChild(ctx context.Context, snapshotID string) (SnapshotRef, int64, bool, error) {
	children, err := r.q.ListCommittedChildSnapshots(ctx, sql.NullString{String: snapshotID, Valid: true})
	if err != nil {
		return SnapshotRef{}, 0, false, err
	}
	if len(children) == 0 {
		return SnapshotRef{}, 0, false, nil
	}
	total, err := r.q.CountCommittedDescendants(ctx, sql.NullString{String: snapshotID, Valid: true})
	if err != nil {
		return SnapshotRef{}, 0, false, err
	}
	remainingAfterNext := total - 1
	if remainingAfterNext < 0 {
		remainingAfterNext = 0
	}
	return SnapshotRef{ID: children[0].ID, Name: children[0].Name}, remainingAfterNext, true, nil
}

func (r *Repository) ListUploadSessionChunks(ctx context.Context, uploadID string) ([]db.SnapshotChunk, error) {
	return r.q.ListUploadSessionChunks(ctx, uploadID)
}

func (r *Repository) ListAbandonedUploadSessions(ctx context.Context, olderThan time.Time, limit int64) ([]AbandonedUpload, error) {
	if limit <= 0 {
		limit = 100
	}
	uploads, err := r.q.ListAbandonedUploadSessions(ctx, db.ListAbandonedUploadSessionsParams{CompletedAt: sql.NullString{String: FormatTime(olderThan), Valid: true}, Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]AbandonedUpload, 0, len(uploads))
	for _, upload := range uploads {
		result = append(result, AbandonedUpload{ID: upload.ID, SnapshotID: upload.SnapshotID, ManifestObjectKey: upload.ManifestObjectKey})
	}
	return result, nil
}

func (r *Repository) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return r.q.DeleteSnapshot(ctx, snapshotID)
}

func (r *Repository) DeleteUploadSession(ctx context.Context, uploadID string) error {
	return r.q.DeleteUploadSession(ctx, uploadID)
}

type CatalogBackupRecord struct {
	ID                string
	OperationID       string
	ObjectKey         string
	MetadataObjectKey string
	SizeBytes         int64
	ChecksumSHA256    string
	StartedAt         time.Time
	CompletedAt       time.Time
	Status            string
	FailureReason     string
}

type CatalogBackupRef struct {
	ID                string
	ObjectKey         string
	MetadataObjectKey string
}

func (r *Repository) CreateCatalogBackupOperation(ctx context.Context) (string, error) {
	operationID := NewID("op")
	if err := r.q.CreateOperation(ctx, db.CreateOperationParams{ID: operationID, Type: "catalog_backup", Status: "running", Summary: sql.NullString{String: "catalog backup running", Valid: true}}); err != nil {
		return "", err
	}
	return operationID, nil
}

func (r *Repository) RecordCatalogBackup(ctx context.Context, record CatalogBackupRecord) error {
	if record.ID == "" {
		record.ID = NewID("catbak")
	}
	return createCatalogBackup(ctx, r.q, record)
}

func createCatalogBackup(ctx context.Context, q *db.Queries, record CatalogBackupRecord) error {
	return q.CreateCatalogBackup(ctx, db.CreateCatalogBackupParams{
		ID:                record.ID,
		OperationID:       sql.NullString{String: record.OperationID, Valid: record.OperationID != ""},
		ObjectKey:         record.ObjectKey,
		MetadataObjectKey: sql.NullString{String: record.MetadataObjectKey, Valid: record.MetadataObjectKey != ""},
		SizeBytes:         record.SizeBytes,
		ChecksumSha256:    record.ChecksumSHA256,
		StartedAt:         FormatTime(record.StartedAt),
		CompletedAt:       sql.NullString{String: FormatTime(record.CompletedAt), Valid: !record.CompletedAt.IsZero()},
		Status:            record.Status,
		FailureReason:     sql.NullString{String: record.FailureReason, Valid: record.FailureReason != ""},
	})
}

func (r *Repository) RecordSucceededCatalogBackup(ctx context.Context, record CatalogBackupRecord, summary string) error {
	if record.ID == "" {
		record.ID = NewID("catbak")
	}
	return r.withTx(ctx, func(q *db.Queries) error {
		if err := createCatalogBackup(ctx, q, record); err != nil {
			return err
		}
		return q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: summary, Valid: true}, FailureReason: sql.NullString{}, ID: record.OperationID})
	})
}

func (r *Repository) CompleteCatalogBackupOperation(ctx context.Context, operationID, summary string) error {
	return r.q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: summary, Valid: true}, FailureReason: sql.NullString{}, ID: operationID})
}

func (r *Repository) FailCatalogBackupOperation(ctx context.Context, operationID string, reason error) error {
	return r.q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "failed", Summary: sql.NullString{String: "catalog backup failed", Valid: true}, FailureReason: sql.NullString{String: reason.Error(), Valid: true}, ID: operationID})
}

func (r *Repository) ListCatalogBackupsOlderThan(ctx context.Context, cutoff time.Time, limit int64) ([]CatalogBackupRef, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.q.ListCatalogBackupsOlderThan(ctx, db.ListCatalogBackupsOlderThanParams{StartedAt: FormatTime(cutoff), Limit: limit})
	if err != nil {
		return nil, err
	}
	refs := make([]CatalogBackupRef, 0, len(rows))
	for _, row := range rows {
		refs = append(refs, CatalogBackupRef{ID: row.ID, ObjectKey: row.ObjectKey, MetadataObjectKey: row.MetadataObjectKey.String})
	}
	return refs, nil
}

func (r *Repository) DeleteCatalogBackup(ctx context.Context, id string) error {
	return r.q.DeleteCatalogBackup(ctx, id)
}

func (r *Repository) ReconcileStaleUploads(ctx context.Context, staleBefore time.Time, limit int64) (ReconcileResult, error) {
	if limit <= 0 {
		limit = 100
	}
	cutoff := FormatTime(staleBefore)
	stale, err := r.q.ListStaleUploadSessions(ctx, db.ListStaleUploadSessionsParams{LastHeartbeatAt: cutoff, Limit: limit})
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("list stale upload sessions: %w", err)
	}
	if len(stale) == 0 {
		return ReconcileResult{}, nil
	}

	operationID := NewID("op")
	if err := r.q.CreateOperation(ctx, db.CreateOperationParams{ID: operationID, Type: "reconciliation", Status: "running", Summary: sql.NullString{String: "startup reconciliation running", Valid: true}}); err != nil {
		return ReconcileResult{}, fmt.Errorf("create reconciliation operation: %w", err)
	}

	for _, upload := range stale {
		reason := fmt.Sprintf("stale upload session %s last heartbeat %s before startup cutoff %s", upload.ID, upload.LastHeartbeatAt, cutoff)
		if upload.SnapshotID.Valid {
			if err := r.q.FailSnapshot(ctx, db.FailSnapshotParams{FailureReason: sql.NullString{String: reason, Valid: true}, ID: upload.SnapshotID.String}); err != nil {
				return ReconcileResult{}, fmt.Errorf("fail stale snapshot %s: %w", upload.SnapshotID.String, err)
			}
		}
		if err := r.q.FailUploadSession(ctx, db.FailUploadSessionParams{FailureReason: sql.NullString{String: reason, Valid: true}, ID: upload.ID}); err != nil {
			return ReconcileResult{}, fmt.Errorf("fail stale upload %s: %w", upload.ID, err)
		}
		if err := r.q.FailOperationByUploadSession(ctx, db.FailOperationByUploadSessionParams{Summary: sql.NullString{String: "upload failed by startup reconciliation", Valid: true}, FailureReason: sql.NullString{String: reason, Valid: true}, UploadSessionID: sql.NullString{String: upload.ID, Valid: true}}); err != nil {
			return ReconcileResult{}, fmt.Errorf("fail upload operation for stale upload %s: %w", upload.ID, err)
		}
	}

	summary := fmt.Sprintf("marked %d stale upload(s) failed", len(stale))
	if err := r.q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: summary, Valid: true}, FailureReason: sql.NullString{}, ID: operationID}); err != nil {
		return ReconcileResult{}, fmt.Errorf("complete reconciliation operation: %w", err)
	}
	return ReconcileResult{StaleUploadsFailed: len(stale), OperationID: operationID}, nil
}

func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func NewID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
