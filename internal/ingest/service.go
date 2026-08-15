package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/manifest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

type Service struct {
	Repo                         *catalog.Repository
	Store                        storage.Store
	Keys                         storage.KeyBuilder
	ChunkSize                    int64
	MaxIncrementalChainDepth     int64
	MaxIncrementalChainDepthFunc func() int64

	// PutRetryMaxElapsed bounds retries for transient object storage Put failures.
	// Defaults to 5 minutes.
	PutRetryMaxElapsed time.Duration
	// PutRetryInitialBackoff controls the first retry delay. Defaults to 100ms.
	PutRetryInitialBackoff time.Duration

	// BeforeCatalogCommit is a test hook used to inject catalog commit failures
	// after chunks and manifest have been persisted.
	BeforeCatalogCommit func(context.Context, StartedCommit) error
}

type StartedCommit struct {
	SnapshotID      string
	UploadSessionID string
	OperationID     string
	ManifestKey     string
}

type Request struct {
	Source                   string
	Pool                     string
	Dataset                  string
	Snapshot                 string
	BaseSnapshot             string
	Raw                      bool
	Compressed               bool
	FromGUID                 string
	ToGUID                   string
	MaxIncrementalChainDepth int64
}

type Result struct {
	SnapshotID      string
	UploadSessionID string
	OperationID     string
	ManifestKey     string
	StreamSHA256    string
	BytesReceived   int64
	Chunks          []manifest.Chunk
}

const (
	defaultPutRetryMaxElapsed     = 5 * time.Minute
	defaultPutRetryInitialBackoff = 100 * time.Millisecond
	maxPutRetryBackoff            = 30 * time.Second
)

func (s Service) EffectiveMaxIncrementalChainDepth() int64 {
	if s.MaxIncrementalChainDepthFunc != nil {
		return s.MaxIncrementalChainDepthFunc()
	}
	return s.MaxIncrementalChainDepth
}

func (s Service) Receive(ctx context.Context, req Request, r io.Reader) (Result, error) {
	if s.Repo == nil {
		return Result{}, fmt.Errorf("repo is required")
	}
	if s.Store == nil {
		return Result{}, fmt.Errorf("store is required")
	}
	chunkSize := s.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 64 * 1024 * 1024
	}

	maxChainDepth := req.MaxIncrementalChainDepth
	if maxChainDepth == 0 {
		maxChainDepth = s.EffectiveMaxIncrementalChainDepth()
	}
	started, err := s.Repo.StartUpload(ctx, catalog.DatasetIdentity{Source: req.Source, Pool: req.Pool, Dataset: req.Dataset, Snapshot: req.Snapshot, BaseSnapshot: req.BaseSnapshot, MaxIncrementalChainDepth: maxChainDepth}, chunkSize)
	if err != nil {
		return Result{}, err
	}

	result := Result{SnapshotID: started.SnapshotID, UploadSessionID: started.UploadSessionID, OperationID: started.OperationID}
	fail := func(cause error) (Result, error) {
		failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := s.Repo.FailUpload(failureCtx, started.SnapshotID, started.UploadSessionID, started.OperationID, cause); err != nil {
			return Result{}, errors.Join(cause, fmt.Errorf("record upload failure: %w", err))
		}
		return Result{}, cause
	}

	if chunkSize > int64(int(chunkSize)) {
		return Result{}, fmt.Errorf("chunk size too large: %d", chunkSize)
	}
	buffer := make([]byte, int(chunkSize))
	streamHash := sha256.New()
	var chunks []manifest.Chunk
	var offset int64
	for index := int64(0); ; index++ {
		n, readErr := io.ReadFull(r, buffer)
		if readErr == io.EOF {
			if ctx.Err() != nil {
				return fail(fmt.Errorf("read chunk %d: %w", index, ctx.Err()))
			}
			break
		}
		if readErr == io.ErrUnexpectedEOF {
			if ctx.Err() != nil {
				return fail(fmt.Errorf("read chunk %d: %w", index, ctx.Err()))
			}
			// Last partial chunk.
		} else if readErr != nil {
			return fail(fmt.Errorf("read chunk %d: %w", index, readErr))
		}
		if n == 0 {
			break
		}

		chunkData := append([]byte(nil), buffer[:n]...)
		_, _ = streamHash.Write(chunkData)
		key := s.Keys.ChunkKey(req.Source, req.Pool, req.Dataset, req.Snapshot, index)
		info, err := s.putBytesWithRetry(ctx, key, chunkData)
		if err != nil {
			return fail(fmt.Errorf("put chunk %d: %w", index, err))
		}
		if info.Size != int64(n) {
			return fail(fmt.Errorf("chunk %d persisted size mismatch: got %d want %d", index, info.Size, n))
		}
		sha := sha256.Sum256(chunkData)
		shaHex := hex.EncodeToString(sha[:])
		if info.SHA256 != "" && info.SHA256 != shaHex {
			return fail(fmt.Errorf("chunk %d sha mismatch: got %s want %s", index, info.SHA256, shaHex))
		}

		chunk := manifest.Chunk{Index: index, ObjectKey: key, SizeBytes: int64(n), OffsetStart: offset, OffsetEnd: offset + int64(n), SHA256: shaHex}
		if err := s.Repo.AddVerifiedChunk(ctx, started.SnapshotID, started.UploadSessionID, catalog.ChunkRecord{Index: chunk.Index, ObjectKey: chunk.ObjectKey, SizeBytes: chunk.SizeBytes, OffsetStart: chunk.OffsetStart, OffsetEnd: chunk.OffsetEnd, SHA256: chunk.SHA256}); err != nil {
			return fail(fmt.Errorf("record chunk %d: %w", index, err))
		}
		chunks = append(chunks, chunk)
		offset += int64(n)
		if err := s.Repo.UpdateUploadProgress(ctx, started.UploadSessionID, index+1, int64(len(chunks)), offset); err != nil {
			return fail(fmt.Errorf("update progress: %w", err))
		}
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if len(chunks) == 0 {
		return fail(fmt.Errorf("empty upload stream"))
	}

	if err := s.Repo.UpdateUploadStatus(ctx, started.UploadSessionID, "committing_manifest"); err != nil {
		return fail(fmt.Errorf("mark committing manifest: %w", err))
	}

	streamSHA := hex.EncodeToString(streamHash.Sum(nil))
	mani := manifest.New(
		manifest.Identity{Source: req.Source, Pool: req.Pool, Dataset: req.Dataset, Snapshot: req.Snapshot},
		manifest.Lineage{BaseSnapshot: req.BaseSnapshot},
		manifest.Stream{Raw: req.Raw, Compressed: req.Compressed, FromGUID: req.FromGUID, ToGUID: req.ToGUID, SizeBytes: offset, SHA256: streamSHA, ChunkSize: chunkSize},
		chunks,
	)
	manifestBytes, err := mani.MarshalCanonical()
	if err != nil {
		return fail(fmt.Errorf("build manifest: %w", err))
	}
	manifestKey := s.Keys.ManifestKey(req.Source, req.Pool, req.Dataset, req.Snapshot)
	manifestInfo, err := s.putBytesWithRetry(ctx, manifestKey, manifestBytes)
	if err != nil {
		return fail(fmt.Errorf("put manifest: %w", err))
	}
	if manifestInfo.Size != int64(len(manifestBytes)) {
		return fail(fmt.Errorf("manifest persisted size mismatch: got %d want %d", manifestInfo.Size, len(manifestBytes)))
	}

	if err := s.Repo.SetUploadManifestObjectKey(ctx, started.UploadSessionID, manifestKey); err != nil {
		return fail(fmt.Errorf("mark committing catalog: %w", err))
	}
	if s.BeforeCatalogCommit != nil {
		if err := s.BeforeCatalogCommit(ctx, StartedCommit{SnapshotID: started.SnapshotID, UploadSessionID: started.UploadSessionID, OperationID: started.OperationID, ManifestKey: manifestKey}); err != nil {
			return fail(fmt.Errorf("before catalog commit: %w", err))
		}
	}
	if err := s.Repo.CommitUpload(ctx, started.SnapshotID, started.UploadSessionID, manifestKey, offset, offset, streamSHA, req.FromGUID, req.ToGUID, int64(len(chunks)), started.OperationID); err != nil {
		return fail(err)
	}

	result.ManifestKey = manifestKey
	result.StreamSHA256 = streamSHA
	result.BytesReceived = offset
	result.Chunks = chunks
	return result, nil
}

func (s Service) putBytesWithRetry(ctx context.Context, key string, data []byte) (storage.ObjectInfo, error) {
	maxElapsed := s.PutRetryMaxElapsed
	if maxElapsed <= 0 {
		maxElapsed = defaultPutRetryMaxElapsed
	}
	backoff := s.PutRetryInitialBackoff
	if backoff <= 0 {
		backoff = defaultPutRetryInitialBackoff
	}

	started := time.Now()
	var lastErr error
	for attempt := 1; ; attempt++ {
		info, err := s.Store.PutBytes(ctx, key, data)
		if err == nil {
			return info, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return storage.ObjectInfo{}, ctx.Err()
		}
		remaining := maxElapsed - time.Since(started)
		if remaining <= 0 {
			return storage.ObjectInfo{}, fmt.Errorf("object put failed after %d attempts over %s: %w", attempt, maxElapsed, lastErr)
		}
		delay := backoff
		if delay > maxPutRetryBackoff {
			delay = maxPutRetryBackoff
		}
		if delay > remaining {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return storage.ObjectInfo{}, ctx.Err()
		case <-timer.C:
		}
		if backoff < maxPutRetryBackoff {
			backoff *= 2
			if backoff > maxPutRetryBackoff {
				backoff = maxPutRetryBackoff
			}
		}
	}
}
