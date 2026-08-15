package deletion

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

type Deleter struct {
	DB    db.DBTX
	Store storage.Store
}

type Result struct {
	ObjectsDeleted int
}

type txBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func withTx(ctx context.Context, database db.DBTX, fn func(*db.Queries) error) error {
	beginner, ok := database.(txBeginner)
	if !ok {
		return fn(db.New(database))
	}
	tx, err := beginner.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(db.New(tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (d Deleter) DeleteSnapshot(ctx context.Context, snapshotID string) (Result, error) {
	return d.DeleteSnapshotWithOperation(ctx, snapshotID, "")
}

func (d Deleter) DeleteSnapshotWithOperation(ctx context.Context, snapshotID, operationID string) (Result, error) {
	if d.DB == nil {
		return Result{}, fmt.Errorf("db is required")
	}
	if d.Store == nil {
		return Result{}, fmt.Errorf("store is required")
	}
	q := db.New(d.DB)
	descendants, err := q.CountCommittedDescendants(ctx, sql.NullString{String: snapshotID, Valid: true})
	if err != nil {
		return Result{}, fmt.Errorf("count committed snapshot descendants: %w", err)
	}
	if descendants > 0 {
		return Result{}, fmt.Errorf("snapshot %s has %d committed descendant(s); delete the dependent chain or dataset instead", snapshotID, descendants)
	}
	keys, err := q.ListSnapshotObjectKeys(ctx, db.ListSnapshotObjectKeysParams{SnapshotID: snapshotID, ID: snapshotID})
	if err != nil {
		return Result{}, fmt.Errorf("list snapshot objects: %w", err)
	}
	if err := q.InvalidateSnapshotForDeletion(ctx, snapshotID); err != nil {
		return Result{}, fmt.Errorf("invalidate snapshot before deleting objects: %w", err)
	}
	result, err := deleteObjects(ctx, d.Store, keys)
	if err != nil {
		return result, err
	}
	if err := withTx(ctx, d.DB, func(q *db.Queries) error {
		if err := q.DeleteUploadSessionsBySnapshot(ctx, sql.NullString{String: snapshotID, Valid: true}); err != nil {
			return fmt.Errorf("delete upload sessions for snapshot: %w", err)
		}
		if err := q.DeleteSnapshot(ctx, snapshotID); err != nil {
			return fmt.Errorf("delete snapshot catalog rows: %w", err)
		}
		if operationID != "" {
			summary := fmt.Sprintf("cleanup succeeded; %d objects removed", result.ObjectsDeleted)
			if err := q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: summary, Valid: true}, FailureReason: sql.NullString{}, ID: operationID}); err != nil {
				return fmt.Errorf("mark cleanup succeeded: %w", err)
			}
		}
		return nil
	}); err != nil {
		return result, err
	}
	return result, nil
}

func (d Deleter) DeleteDataset(ctx context.Context, datasetID string) (Result, error) {
	return d.DeleteDatasetWithOperation(ctx, datasetID, "")
}

func (d Deleter) DeleteDatasetWithOperation(ctx context.Context, datasetID, operationID string) (Result, error) {
	if d.DB == nil {
		return Result{}, fmt.Errorf("db is required")
	}
	if d.Store == nil {
		return Result{}, fmt.Errorf("store is required")
	}
	q := db.New(d.DB)
	keys, err := q.ListDatasetObjectKeys(ctx, db.ListDatasetObjectKeysParams{DatasetID: datasetID, DatasetID_2: datasetID})
	if err != nil {
		return Result{}, fmt.Errorf("list dataset objects: %w", err)
	}
	if err := q.InvalidateDatasetSnapshotsForDeletion(ctx, datasetID); err != nil {
		return Result{}, fmt.Errorf("invalidate dataset snapshots before deleting objects: %w", err)
	}
	result, err := deleteObjects(ctx, d.Store, keys)
	if err != nil {
		return result, err
	}
	if err := withTx(ctx, d.DB, func(q *db.Queries) error {
		if err := q.DeleteDataset(ctx, datasetID); err != nil {
			return fmt.Errorf("delete dataset catalog rows: %w", err)
		}
		if operationID != "" {
			summary := fmt.Sprintf("cleanup succeeded; %d objects removed", result.ObjectsDeleted)
			if err := q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: summary, Valid: true}, FailureReason: sql.NullString{}, ID: operationID}); err != nil {
				return fmt.Errorf("mark cleanup succeeded: %w", err)
			}
		}
		return nil
	}); err != nil {
		return result, err
	}
	return result, nil
}

func deleteObjects(ctx context.Context, store storage.Store, keys []string) (Result, error) {
	seen := make(map[string]struct{}, len(keys))
	var result Result
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		if err := store.Delete(ctx, key); err != nil {
			return result, fmt.Errorf("delete object %s: %w", key, err)
		}
		seen[key] = struct{}{}
		result.ObjectsDeleted++
	}
	return result, nil
}
