package deletion

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

type Worker struct {
	DB     db.DBTX
	Store  storage.Store
	Logger *slog.Logger
	Limit  int64
}

type WorkerResult struct {
	OperationsProcessed int
	OperationsFailed    int
	ObjectsDeleted      int
}

func (w Worker) RunOnce(ctx context.Context) (WorkerResult, error) {
	if w.DB == nil {
		return WorkerResult{}, fmt.Errorf("db is required")
	}
	if w.Store == nil {
		return WorkerResult{}, fmt.Errorf("store is required")
	}
	logger := w.Logger
	if logger == nil {
		logger = slog.Default()
	}
	limit := w.Limit
	if limit <= 0 {
		limit = 10
	}
	q := db.New(w.DB)
	operations, err := q.ListPendingCleanupOperations(ctx, limit)
	if err != nil {
		return WorkerResult{}, fmt.Errorf("list pending cleanup operations: %w", err)
	}

	var result WorkerResult
	for _, op := range operations {
		objectsDeleted, err := w.runOperation(ctx, q, op)
		if err != nil {
			result.OperationsFailed++
			logger.Error("cleanup operation failed", "operation_id", op.ID, "error", err)
			continue
		}
		result.OperationsProcessed++
		result.ObjectsDeleted += objectsDeleted
		logger.Info("cleanup operation succeeded", "operation_id", op.ID, "objects_deleted", objectsDeleted)
	}
	return result, nil
}

func (w Worker) runOperation(ctx context.Context, q *db.Queries, op db.ListPendingCleanupOperationsRow) (int, error) {
	if err := q.MarkOperationRunning(ctx, db.MarkOperationRunningParams{Summary: sql.NullString{String: "cleanup running", Valid: true}, ID: op.ID}); err != nil {
		return 0, fmt.Errorf("mark cleanup running: %w", err)
	}

	var (
		result Result
		err    error
	)
	deleter := Deleter{DB: w.DB, Store: w.Store}
	switch {
	case op.SnapshotID.Valid && op.SnapshotID.String != "":
		result, err = deleter.DeleteSnapshotWithOperation(ctx, op.SnapshotID.String, op.ID)
	case op.DatasetID.Valid && op.DatasetID.String != "":
		result, err = deleter.DeleteDatasetWithOperation(ctx, op.DatasetID.String, op.ID)
	default:
		err = fmt.Errorf("cleanup operation has no dataset_id or snapshot_id")
	}
	if err != nil {
		_ = q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "failed", Summary: sql.NullString{String: "cleanup failed", Valid: true}, FailureReason: sql.NullString{String: err.Error(), Valid: true}, ID: op.ID})
		return result.ObjectsDeleted, err
	}
	return result.ObjectsDeleted, nil
}

func StartWorker(ctx context.Context, logger *slog.Logger, worker Worker, interval time.Duration) {
	if interval <= 0 {
		return
	}
	if worker.Logger == nil {
		worker.Logger = logger
	}
	if worker.Logger == nil {
		worker.Logger = slog.Default()
	}
	worker.Logger.Info("starting cleanup worker", "interval", interval.String())
	go func() {
		// Run once on startup so queued work resumes quickly after deploy/restart.
		result, err := worker.RunOnce(ctx)
		logWorkerResult(worker.Logger, result, err)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := worker.RunOnce(ctx)
				logWorkerResult(worker.Logger, result, err)
			}
		}
	}()
}

func logWorkerResult(logger *slog.Logger, result WorkerResult, err error) {
	if err != nil {
		logger.Error("cleanup worker failed", "error", err)
		return
	}
	if result.OperationsProcessed > 0 || result.OperationsFailed > 0 || result.ObjectsDeleted > 0 {
		logger.Info("cleanup worker completed", "operations_processed", result.OperationsProcessed, "operations_failed", result.OperationsFailed, "objects_deleted", result.ObjectsDeleted)
	}
}
