package catalogbackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

const DefaultRetention = 30 * 24 * time.Hour

type Runner struct {
	Catalog   *catalog.Catalog
	Repo      *catalog.Repository
	Store     storage.Store
	Keys      storage.KeyBuilder
	TempDir   string
	Now       func() time.Time
	Retention time.Duration
}

type Result struct {
	BackupID          string `json:"backup_id"`
	OperationID       string `json:"operation_id"`
	ObjectKey         string `json:"object_key"`
	MetadataObjectKey string `json:"metadata_object_key"`
	SizeBytes         int64  `json:"size_bytes"`
	ChecksumSHA256    string `json:"checksum_sha256"`
	StartedAt         string `json:"started_at"`
	CompletedAt       string `json:"completed_at"`
}

func (r Runner) Run(ctx context.Context) (Result, error) {
	if r.Catalog == nil {
		return Result{}, fmt.Errorf("catalog is required")
	}
	if r.Repo == nil {
		return Result{}, fmt.Errorf("repo is required")
	}
	if r.Store == nil {
		return Result{}, fmt.Errorf("store is required")
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Retention == 0 {
		r.Retention = DefaultRetention
	}
	started := r.Now().UTC()
	backupID := catalog.NewID("catbak")

	operationID, err := r.Repo.CreateCatalogBackupOperation(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("create catalog backup operation: %w", err)
	}
	fail := func(cause error) (Result, error) {
		_ = r.Repo.FailCatalogBackupOperation(ctx, operationID, cause)
		return Result{}, cause
	}

	tempDir := r.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return fail(fmt.Errorf("create temp dir: %w", err))
	}
	tmpPath := filepath.Join(tempDir, backupID+".sqlite")
	defer os.Remove(tmpPath)

	if _, err := r.Catalog.DB().ExecContext(ctx, `VACUUM INTO ?`, tmpPath); err != nil {
		return fail(fmt.Errorf("vacuum catalog into backup: %w", err))
	}
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return fail(fmt.Errorf("read catalog backup: %w", err))
	}
	sha := sha256.Sum256(data)
	shaHex := hex.EncodeToString(sha[:])

	backupKey := r.backupObjectKey(started, backupID, ".sqlite")
	info, err := r.Store.PutBytes(ctx, backupKey, data)
	if err != nil {
		return fail(fmt.Errorf("upload catalog backup: %w", err))
	}
	if info.Size != int64(len(data)) {
		return fail(fmt.Errorf("catalog backup persisted size mismatch: got %d want %d", info.Size, len(data)))
	}
	if info.SHA256 != "" && info.SHA256 != shaHex {
		return fail(fmt.Errorf("catalog backup checksum mismatch: got %s want %s", info.SHA256, shaHex))
	}

	completed := r.Now().UTC()
	metadataKey := r.backupObjectKey(started, backupID, ".json")
	result := Result{
		BackupID:          backupID,
		OperationID:       operationID,
		ObjectKey:         backupKey,
		MetadataObjectKey: metadataKey,
		SizeBytes:         int64(len(data)),
		ChecksumSHA256:    shaHex,
		StartedAt:         catalog.FormatTime(started),
		CompletedAt:       catalog.FormatTime(completed),
	}
	metadataBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fail(fmt.Errorf("marshal catalog backup metadata: %w", err))
	}
	metadataInfo, err := r.Store.PutBytes(ctx, metadataKey, metadataBytes)
	if err != nil {
		return fail(fmt.Errorf("upload catalog backup metadata: %w", err))
	}
	if metadataInfo.Size != int64(len(metadataBytes)) {
		return fail(fmt.Errorf("catalog backup metadata persisted size mismatch: got %d want %d", metadataInfo.Size, len(metadataBytes)))
	}

	if err := r.Repo.RecordSucceededCatalogBackup(ctx, catalog.CatalogBackupRecord{
		ID:                backupID,
		OperationID:       operationID,
		ObjectKey:         backupKey,
		MetadataObjectKey: metadataKey,
		SizeBytes:         result.SizeBytes,
		ChecksumSHA256:    shaHex,
		StartedAt:         started,
		CompletedAt:       completed,
		Status:            "succeeded",
	}, "catalog backup succeeded"); err != nil {
		return fail(fmt.Errorf("record catalog backup success: %w", err))
	}
	if r.Retention > 0 {
		if err := r.cleanupExpired(ctx, completed.Add(-r.Retention)); err != nil {
			return Result{}, fmt.Errorf("cleanup expired catalog backups: %w", err)
		}
	}
	return result, nil
}

func (r Runner) cleanupExpired(ctx context.Context, cutoff time.Time) error {
	for {
		backups, err := r.Repo.ListCatalogBackupsOlderThan(ctx, cutoff, 100)
		if err != nil {
			return fmt.Errorf("list expired catalog backups: %w", err)
		}
		if len(backups) == 0 {
			return nil
		}
		for _, backup := range backups {
			if backup.ObjectKey != "" {
				if err := r.Store.Delete(ctx, backup.ObjectKey); err != nil {
					return fmt.Errorf("delete expired catalog backup %s object %s: %w", backup.ID, backup.ObjectKey, err)
				}
			}
			if backup.MetadataObjectKey != "" {
				if err := r.Store.Delete(ctx, backup.MetadataObjectKey); err != nil {
					return fmt.Errorf("delete expired catalog backup %s metadata %s: %w", backup.ID, backup.MetadataObjectKey, err)
				}
			}
			if err := r.Repo.DeleteCatalogBackup(ctx, backup.ID); err != nil {
				return fmt.Errorf("delete expired catalog backup row %s: %w", backup.ID, err)
			}
		}
	}
}

func (r Runner) backupObjectKey(t time.Time, backupID, suffix string) string {
	prefix := strings.Trim(r.Keys.CatalogBackupPrefix(), "/")
	return fmt.Sprintf("%s/%04d/%02d/%02d/%s-%s%s", prefix, t.Year(), t.Month(), t.Day(), t.Format("20060102T150405Z"), backupID, suffix)
}
