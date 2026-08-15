package cleanup

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

type AbandonedUploadCleaner struct {
	Repo  *catalog.Repository
	Store storage.Store
}

type AbandonedUploadCleanupResult struct {
	UploadsDeleted   int
	SnapshotsDeleted int
	ObjectsDeleted   int
}

func (c AbandonedUploadCleaner) Cleanup(ctx context.Context, olderThan time.Time, limit int64) (AbandonedUploadCleanupResult, error) {
	if c.Repo == nil {
		return AbandonedUploadCleanupResult{}, fmt.Errorf("repo is required")
	}
	if c.Store == nil {
		return AbandonedUploadCleanupResult{}, fmt.Errorf("store is required")
	}

	uploads, err := c.Repo.ListAbandonedUploadSessions(ctx, olderThan, limit)
	if err != nil {
		return AbandonedUploadCleanupResult{}, fmt.Errorf("list abandoned uploads: %w", err)
	}

	var result AbandonedUploadCleanupResult
	for _, upload := range uploads {
		chunks, err := c.Repo.ListUploadSessionChunks(ctx, upload.ID)
		if err != nil {
			return result, fmt.Errorf("list chunks for abandoned upload %s: %w", upload.ID, err)
		}

		seen := make(map[string]struct{}, len(chunks)+1)
		for _, chunk := range chunks {
			if _, ok := seen[chunk.ObjectKey]; ok {
				continue
			}
			if err := c.Store.Delete(ctx, chunk.ObjectKey); err != nil {
				return result, fmt.Errorf("delete abandoned chunk %s: %w", chunk.ObjectKey, err)
			}
			seen[chunk.ObjectKey] = struct{}{}
			result.ObjectsDeleted++
		}

		if upload.ManifestObjectKey.Valid && upload.ManifestObjectKey.String != "" {
			if _, ok := seen[upload.ManifestObjectKey.String]; !ok {
				if err := c.Store.Delete(ctx, upload.ManifestObjectKey.String); err != nil {
					return result, fmt.Errorf("delete abandoned manifest %s: %w", upload.ManifestObjectKey.String, err)
				}
				result.ObjectsDeleted++
			}
		}

		if upload.SnapshotID.Valid {
			if err := c.Repo.DeleteSnapshot(ctx, upload.SnapshotID.String); err != nil {
				return result, fmt.Errorf("delete abandoned snapshot %s: %w", upload.SnapshotID.String, err)
			}
			result.SnapshotsDeleted++
		}
		if err := c.Repo.DeleteUploadSession(ctx, upload.ID); err != nil {
			return result, fmt.Errorf("delete abandoned upload session %s: %w", upload.ID, err)
		}
		result.UploadsDeleted++
	}
	return result, nil
}
