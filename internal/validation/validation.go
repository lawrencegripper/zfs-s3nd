package validation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"github.com/lawrencegripper/zfs-s3nd/internal/manifest"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
)

type StreamGUIDs struct {
	FromGUID string
	ToGUID   string
}

type StreamChecker interface {
	Check(ctx context.Context, manifestKey string, stream io.Reader) (StreamGUIDs, error)
}

type ZstreamdumpChecker struct {
	Path string
}

func (c ZstreamdumpChecker) Check(ctx context.Context, manifestKey string, stream io.Reader) (StreamGUIDs, error) {
	path := c.Path
	if path == "" {
		path = "zstreamdump"
	}
	cmd := exec.CommandContext(ctx, path)
	cmd.Stdin = stream
	output, err := cmd.CombinedOutput()
	if err != nil {
		return StreamGUIDs{}, fmt.Errorf("zstreamdump %s: %w: %s", manifestKey, err, strings.TrimSpace(string(output)))
	}
	if zstreamdumpBeginRecords(string(output)) == 0 {
		return StreamGUIDs{}, fmt.Errorf("zstreamdump %s found no DRR_BEGIN records", manifestKey)
	}
	return ParseZstreamdumpGUIDs(string(output)), nil
}

type Validator struct {
	DB      db.DBTX
	Store   storage.Store
	Checker StreamChecker
}

type chainItem struct {
	row      db.ListSnapshotRestoreChainRow
	manifest manifest.Manifest
	guids    StreamGUIDs
}

func (v Validator) ValidateChain(ctx context.Context, snapshotID string) error {
	if v.DB == nil {
		return fmt.Errorf("db is required")
	}
	if v.Store == nil {
		return fmt.Errorf("store is required")
	}
	if v.Checker == nil {
		return fmt.Errorf("stream checker is required")
	}

	rows, err := db.New(v.DB).ListSnapshotRestoreChain(ctx, snapshotID)
	if err != nil {
		return fmt.Errorf("list restore chain: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("snapshot %s not found", snapshotID)
	}
	if rows[len(rows)-1].ID != snapshotID {
		return fmt.Errorf("restore chain does not end at target snapshot %s", snapshotID)
	}

	items := make([]chainItem, len(rows))
	for i, row := range rows {
		if row.Status != "committed" {
			return fmt.Errorf("snapshot %s is %s, not committed", row.Name, row.Status)
		}
		if !row.ManifestObjectKey.Valid || row.ManifestObjectKey.String == "" {
			return fmt.Errorf("snapshot %s has no manifest", row.Name)
		}
		if i == 0 {
			if row.ParentSnapshotID.Valid {
				return fmt.Errorf("snapshot %s references missing ancestor %s", row.Name, row.ParentSnapshotID.String)
			}
		} else {
			previous := rows[i-1]
			if !row.ParentSnapshotID.Valid {
				return fmt.Errorf("snapshot %s is incremental in chain but has no parent", row.Name)
			}
			if row.ParentSnapshotID.String != previous.ID {
				return fmt.Errorf("snapshot %s parent %s does not match previous snapshot %s", row.Name, row.ParentSnapshotID.String, previous.ID)
			}
		}

		mani, err := v.verifyManifestAndObjects(ctx, row.ID, row.Name, row.ManifestObjectKey.String)
		if err != nil {
			return err
		}
		if mani.Identity.Snapshot != row.Name {
			return fmt.Errorf("snapshot %s manifest identity snapshot is %s", row.Name, mani.Identity.Snapshot)
		}
		if i == 0 {
			if mani.Lineage.BaseSnapshot != "" {
				return fmt.Errorf("full snapshot %s manifest unexpectedly has base %s", row.Name, mani.Lineage.BaseSnapshot)
			}
		} else {
			previous := rows[i-1]
			if mani.Lineage.BaseSnapshot != previous.Name {
				return fmt.Errorf("snapshot %s manifest base %s does not match previous snapshot %s", row.Name, mani.Lineage.BaseSnapshot, previous.Name)
			}
		}

		guids, err := v.checkStream(ctx, row.ManifestObjectKey.String, mani)
		if err != nil {
			return fmt.Errorf("stream check %s: %w", row.Name, err)
		}
		items[i] = chainItem{row: row, manifest: mani, guids: guids}
	}

	for i := 1; i < len(items); i++ {
		previous := items[i-1]
		current := items[i]
		if previous.guids.ToGUID == "" && current.guids.FromGUID == "" {
			continue
		}
		if previous.guids.ToGUID == "" {
			return fmt.Errorf("snapshot %s fromguid %s cannot be verified because previous snapshot %s has no toguid", current.row.Name, current.guids.FromGUID, previous.row.Name)
		}
		if current.guids.FromGUID == "" {
			return fmt.Errorf("incremental snapshot %s has no fromguid to compare with previous snapshot %s", current.row.Name, previous.row.Name)
		}
		if current.guids.FromGUID != previous.guids.ToGUID {
			return fmt.Errorf("incremental snapshot %s fromguid %s does not match previous toguid %s", current.row.Name, current.guids.FromGUID, previous.guids.ToGUID)
		}
	}
	return nil
}

func (v Validator) ValidateSnapshot(ctx context.Context, snapshotID string) error {
	if v.DB == nil {
		return fmt.Errorf("db is required")
	}
	if v.Store == nil {
		return fmt.Errorf("store is required")
	}
	if v.Checker == nil {
		return fmt.Errorf("stream checker is required")
	}

	q := db.New(v.DB)
	snapshot, err := q.GetSnapshotDetail(ctx, snapshotID)
	if err != nil {
		return fmt.Errorf("get snapshot: %w", err)
	}
	if snapshot.Status != "committed" {
		return fmt.Errorf("snapshot %s is %s, not committed", snapshot.SnapshotName, snapshot.Status)
	}
	if !snapshot.ManifestObjectKey.Valid || snapshot.ManifestObjectKey.String == "" {
		return fmt.Errorf("snapshot %s has no manifest", snapshot.SnapshotName)
	}

	mani, err := v.verifyManifestAndObjects(ctx, snapshot.SnapshotID, snapshot.SnapshotName, snapshot.ManifestObjectKey.String)
	if err != nil {
		return err
	}
	if mani.Identity.Source != snapshot.SourceName || mani.Identity.Pool != snapshot.PoolName || mani.Identity.Dataset != snapshot.DatasetPath || mani.Identity.Snapshot != snapshot.SnapshotName {
		return fmt.Errorf("snapshot %s manifest identity mismatch", snapshot.SnapshotName)
	}

	guids, err := v.checkStream(ctx, snapshot.ManifestObjectKey.String, mani)
	if err != nil {
		return fmt.Errorf("stream check %s: %w", snapshot.SnapshotName, err)
	}
	if snapshot.StreamFromGuid != "" && guids.FromGUID != "" && snapshot.StreamFromGuid != guids.FromGUID {
		return fmt.Errorf("snapshot %s catalog fromguid %s does not match stream fromguid %s", snapshot.SnapshotName, snapshot.StreamFromGuid, guids.FromGUID)
	}
	if snapshot.StreamToGuid != "" && guids.ToGUID != "" && snapshot.StreamToGuid != guids.ToGUID {
		return fmt.Errorf("snapshot %s catalog toguid %s does not match stream toguid %s", snapshot.SnapshotName, snapshot.StreamToGuid, guids.ToGUID)
	}

	if !snapshot.ParentSnapshotID.Valid {
		if mani.Lineage.BaseSnapshot != "" {
			return fmt.Errorf("full snapshot %s manifest unexpectedly has base %s", snapshot.SnapshotName, mani.Lineage.BaseSnapshot)
		}
		return nil
	}

	parent, err := q.GetSnapshotDetail(ctx, snapshot.ParentSnapshotID.String)
	if err != nil {
		return fmt.Errorf("get parent snapshot: %w", err)
	}
	if parent.Status != "committed" {
		return fmt.Errorf("parent snapshot %s is %s, not committed", parent.SnapshotName, parent.Status)
	}
	if mani.Lineage.BaseSnapshot != parent.SnapshotName {
		return fmt.Errorf("snapshot %s manifest base %s does not match parent snapshot %s", snapshot.SnapshotName, mani.Lineage.BaseSnapshot, parent.SnapshotName)
	}
	if parent.StreamToGuid == "" && guids.FromGUID != "" {
		return fmt.Errorf("snapshot %s fromguid %s cannot be verified because parent snapshot %s has no toguid", snapshot.SnapshotName, guids.FromGUID, parent.SnapshotName)
	}
	if guids.FromGUID == "" && parent.StreamToGuid != "" {
		return fmt.Errorf("incremental snapshot %s has no fromguid to compare with parent snapshot %s", snapshot.SnapshotName, parent.SnapshotName)
	}
	if parent.StreamToGuid != "" && guids.FromGUID != "" && guids.FromGUID != parent.StreamToGuid {
		return fmt.Errorf("incremental snapshot %s fromguid %s does not match parent toguid %s", snapshot.SnapshotName, guids.FromGUID, parent.StreamToGuid)
	}
	return nil
}

func (v Validator) verifyManifestAndObjects(ctx context.Context, snapshotID, snapshotName, manifestKey string) (manifest.Manifest, error) {
	manifestBytes, err := v.Store.GetBytes(ctx, manifestKey)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("read manifest for snapshot %s: %w", snapshotName, err)
	}
	mani, err := manifest.Unmarshal(manifestBytes)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("parse manifest for snapshot %s: %w", snapshotName, err)
	}

	chunks, err := db.New(v.DB).ListSnapshotChunks(ctx, snapshotID)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("list catalog chunks for snapshot %s: %w", snapshotName, err)
	}
	if len(chunks) != len(mani.Chunks) {
		return manifest.Manifest{}, fmt.Errorf("snapshot %s chunk count mismatch: catalog has %d manifest has %d", snapshotName, len(chunks), len(mani.Chunks))
	}
	for i, chunk := range mani.Chunks {
		catalogChunk := chunks[i]
		if catalogChunk.ChunkIndex != chunk.Index || catalogChunk.ObjectKey != chunk.ObjectKey || catalogChunk.SizeBytes != chunk.SizeBytes || catalogChunk.Sha256 != chunk.SHA256 {
			return manifest.Manifest{}, fmt.Errorf("snapshot %s catalog chunk %d does not match manifest", snapshotName, chunk.Index)
		}
		if err := v.verifyChunkMetadata(ctx, snapshotName, chunk); err != nil {
			return manifest.Manifest{}, err
		}
	}
	return mani, nil
}

func (v Validator) verifyChunkMetadata(ctx context.Context, snapshotName string, chunk manifest.Chunk) error {
	info, err := v.Store.Head(ctx, chunk.ObjectKey)
	if err != nil {
		return fmt.Errorf("snapshot %s chunk %d head %s: %w", snapshotName, chunk.Index, chunk.ObjectKey, err)
	}
	if info.Size != chunk.SizeBytes {
		return fmt.Errorf("snapshot %s chunk %d size mismatch: got %d want %d", snapshotName, chunk.Index, info.Size, chunk.SizeBytes)
	}
	if info.SHA256 != "" && info.SHA256 != chunk.SHA256 {
		return fmt.Errorf("snapshot %s chunk %d sha256 metadata mismatch: got %s want %s", snapshotName, chunk.Index, info.SHA256, chunk.SHA256)
	}
	return nil
}

func (v Validator) checkStream(ctx context.Context, manifestKey string, mani manifest.Manifest) (StreamGUIDs, error) {
	reader, writer := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- v.writeManifestStream(ctx, mani, writer)
	}()
	guids, checkErr := v.Checker.Check(ctx, manifestKey, reader)
	_ = reader.Close()
	if err := <-writeErr; err != nil && checkErr == nil {
		return StreamGUIDs{}, err
	}
	if checkErr != nil {
		return StreamGUIDs{}, checkErr
	}
	return guids, nil
}

func (v Validator) writeManifestStream(ctx context.Context, mani manifest.Manifest, writer *io.PipeWriter) error {
	streamHash := sha256.New()
	var total int64
	for _, chunk := range mani.Chunks {
		reader, err := v.Store.GetReader(ctx, chunk.ObjectKey)
		if err != nil {
			_ = writer.CloseWithError(err)
			return err
		}
		chunkHash := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(writer, streamHash, chunkHash), reader)
		closeErr := reader.Close()
		if copyErr != nil {
			_ = writer.CloseWithError(copyErr)
			return copyErr
		}
		if closeErr != nil {
			_ = writer.CloseWithError(closeErr)
			return closeErr
		}
		if written != chunk.SizeBytes {
			err := fmt.Errorf("chunk %d streamed size mismatch: got %d want %d", chunk.Index, written, chunk.SizeBytes)
			_ = writer.CloseWithError(err)
			return err
		}
		gotChunkSHA := hex.EncodeToString(chunkHash.Sum(nil))
		if gotChunkSHA != chunk.SHA256 {
			err := fmt.Errorf("chunk %d sha256 mismatch: got %s want %s", chunk.Index, gotChunkSHA, chunk.SHA256)
			_ = writer.CloseWithError(err)
			return err
		}
		total += written
	}
	if total != mani.Stream.SizeBytes {
		err := fmt.Errorf("stream size mismatch: got %d want %d", total, mani.Stream.SizeBytes)
		_ = writer.CloseWithError(err)
		return err
	}
	got := hex.EncodeToString(streamHash.Sum(nil))
	if got != mani.Stream.SHA256 {
		err := fmt.Errorf("stream sha256 mismatch: got %s want %s", got, mani.Stream.SHA256)
		_ = writer.CloseWithError(err)
		return err
	}
	return writer.Close()
}

var guidLineRE = regexp.MustCompile(`(?i)\b(fromguid|toguid)\b\s*=\s*([0-9a-fx]+)`)
var beginRecordsRE = regexp.MustCompile(`(?m)Total DRR_BEGIN records = ([0-9]+)`)

func zstreamdumpBeginRecords(output string) int64 {
	match := beginRecordsRE.FindStringSubmatch(output)
	if len(match) != 2 {
		return 0
	}
	count, _ := strconv.ParseInt(match[1], 10, 64)
	return count
}

func ParseZstreamdumpGUIDs(output string) StreamGUIDs {
	var guids StreamGUIDs
	for _, match := range guidLineRE.FindAllStringSubmatch(output, -1) {
		kind := strings.ToLower(match[1])
		value := canonicalGUID(match[2])
		switch kind {
		case "fromguid":
			guids.FromGUID = value
		case "toguid":
			guids.ToGUID = value
		}
	}
	return guids
}

func canonicalGUID(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	hexDigits := strings.TrimPrefix(value, "0x")
	guid, err := strconv.ParseUint(hexDigits, 16, 64)
	if err != nil {
		return value
	}
	if guid == 0 {
		return "0"
	}
	return fmt.Sprintf("0x%x", guid)
}

type Runner struct {
	DB       db.DBTX
	Store    storage.Store
	Checker  StreamChecker
	Executor string
}

type RunDueResult struct {
	Checked   int `json:"checked"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

type ReconcileStaleResult struct {
	JobsFailed int64 `json:"jobs_failed"`
}

func ReconcileStaleJobs(ctx context.Context, database db.DBTX, olderThan time.Time) (ReconcileStaleResult, error) {
	cutoff := catalog.FormatTime(olderThan)
	if _, err := database.ExecContext(ctx, `UPDATE snapshots
SET stream_validation_status = 'failed'
WHERE stream_validation_status = 'running'
  AND id IN (SELECT snapshot_id FROM validation_jobs WHERE type = 'stream_check' AND status = 'running' AND started_at < ? AND snapshot_id IS NOT NULL)`, cutoff); err != nil {
		return ReconcileStaleResult{}, fmt.Errorf("mark stale stream validations failed: %w", err)
	}
	if _, err := database.ExecContext(ctx, `UPDATE snapshots
SET chain_validation_status = 'failed'
WHERE chain_validation_status = 'running'
  AND id IN (SELECT snapshot_id FROM validation_jobs WHERE type = 'restore_check' AND status = 'running' AND started_at < ? AND snapshot_id IS NOT NULL)`, cutoff); err != nil {
		return ReconcileStaleResult{}, fmt.Errorf("mark stale chain validations failed: %w", err)
	}
	result, err := database.ExecContext(ctx, `UPDATE validation_jobs
SET status = 'failed', completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), result_summary = 'validation job failed by startup reconciliation'
WHERE status = 'running' AND started_at < ?`, cutoff)
	if err != nil {
		return ReconcileStaleResult{}, fmt.Errorf("fail stale validation jobs: %w", err)
	}
	failed, _ := result.RowsAffected()
	if _, err := database.ExecContext(ctx, `UPDATE operations
SET status = 'failed', summary = 'validation failed by startup reconciliation', failure_reason = 'stale validation job', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE type = 'validation' AND status = 'running' AND started_at < ?`, cutoff); err != nil {
		return ReconcileStaleResult{}, fmt.Errorf("fail stale validation operations: %w", err)
	}
	return ReconcileStaleResult{JobsFailed: failed}, nil
}

func (r Runner) RunDue(ctx context.Context, limit int64) (RunDueResult, error) {
	if limit <= 0 {
		limit = 25
	}
	if r.Executor == "" {
		r.Executor = "local"
	}
	if r.Checker == nil {
		r.Checker = ZstreamdumpChecker{}
	}
	q := db.New(r.DB)
	snapshotIDs, err := q.ListSnapshotsDueForValidation(ctx, limit)
	if err != nil {
		return RunDueResult{}, fmt.Errorf("list snapshots due for validation: %w", err)
	}
	var result RunDueResult
	for _, snapshotID := range snapshotIDs {
		result.Checked++
		if err := r.runOne(ctx, q, snapshotID); err != nil {
			result.Failed++
			continue
		}
		result.Succeeded++
	}
	return result, nil
}

func (r Runner) RunSnapshot(ctx context.Context, snapshotID string) error {
	return r.runSnapshotMode(ctx, snapshotID, false)
}

func (r Runner) RunChain(ctx context.Context, snapshotID string) error {
	return r.runSnapshotMode(ctx, snapshotID, true)
}

func (r Runner) runSnapshotMode(ctx context.Context, snapshotID string, fullChain bool) error {
	if r.Executor == "" {
		r.Executor = "local"
	}
	if r.Checker == nil {
		r.Checker = ZstreamdumpChecker{}
	}
	return r.runOneMode(ctx, db.New(r.DB), snapshotID, fullChain)
}

type txBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func withQueriesTx(ctx context.Context, database db.DBTX, fallback *db.Queries, fn func(*db.Queries) error) error {
	beginner, ok := database.(txBeginner)
	if !ok {
		return fn(fallback)
	}
	tx, err := beginner.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(fallback.WithTx(tx)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (r Runner) runOne(ctx context.Context, q *db.Queries, snapshotID string) error {
	return r.runOneMode(ctx, q, snapshotID, true)
}

func (r Runner) runOneMode(ctx context.Context, q *db.Queries, snapshotID string, fullChain bool) error {
	jobID := catalog.NewID("val")
	operationID := catalog.NewID("op")
	validationType := "stream_check"
	if fullChain {
		validationType = "restore_check"
	}
	if err := withQueriesTx(ctx, r.DB, q, func(q *db.Queries) error {
		if err := q.CreateValidationJob(ctx, db.CreateValidationJobParams{ID: jobID, SnapshotID: sql.NullString{String: snapshotID, Valid: true}, Type: validationType, Executor: r.Executor, ResultSummary: sql.NullString{String: "validation running", Valid: true}}); err != nil {
			return fmt.Errorf("create validation job: %w", err)
		}
		if err := q.CreateValidationOperation(ctx, db.CreateValidationOperationParams{ID: operationID, SnapshotID: sql.NullString{String: snapshotID, Valid: true}, ValidationJobID: sql.NullString{String: jobID, Valid: true}, Summary: sql.NullString{String: "validation running", Valid: true}}); err != nil {
			return fmt.Errorf("create validation operation: %w", err)
		}
		if err := updateSnapshotValidationModeStatus(ctx, q, snapshotID, fullChain, "running"); err != nil {
			return fmt.Errorf("mark snapshot validation running: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	validator := Validator{DB: r.DB, Store: r.Store, Checker: r.Checker}
	var err error
	if fullChain {
		err = validator.ValidateChain(ctx, snapshotID)
	} else {
		err = validator.ValidateSnapshot(ctx, snapshotID)
	}
	if err != nil {
		summary := err.Error()
		_ = withQueriesTx(ctx, r.DB, q, func(q *db.Queries) error {
			if err := q.FailValidationJob(ctx, db.FailValidationJobParams{ResultSummary: sql.NullString{String: summary, Valid: true}, ID: jobID}); err != nil {
				return err
			}
			if err := updateSnapshotValidationModeStatus(ctx, q, snapshotID, fullChain, "failed"); err != nil {
				return err
			}
			return q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "failed", Summary: sql.NullString{String: "validation failed", Valid: true}, FailureReason: sql.NullString{String: summary, Valid: true}, ID: operationID})
		})
		return err
	}

	summary := "restore chain validation succeeded"
	if !fullChain {
		summary = "snapshot stream validation succeeded"
	}
	if err := withQueriesTx(ctx, r.DB, q, func(q *db.Queries) error {
		if err := q.CompleteValidationJob(ctx, db.CompleteValidationJobParams{ResultSummary: sql.NullString{String: summary, Valid: true}, ID: jobID}); err != nil {
			return fmt.Errorf("complete validation job: %w", err)
		}
		if err := updateSnapshotValidationModeStatus(ctx, q, snapshotID, fullChain, "succeeded"); err != nil {
			return fmt.Errorf("mark snapshot validation succeeded: %w", err)
		}
		if err := q.UpdateOperationStatus(ctx, db.UpdateOperationStatusParams{Status: "succeeded", Summary: sql.NullString{String: summary, Valid: true}, FailureReason: sql.NullString{}, ID: operationID}); err != nil {
			return fmt.Errorf("complete validation operation: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func updateSnapshotValidationModeStatus(ctx context.Context, q *db.Queries, snapshotID string, fullChain bool, status string) error {
	if fullChain {
		return q.UpdateSnapshotChainValidationStatus(ctx, db.UpdateSnapshotChainValidationStatusParams{ChainValidationStatus: status, ID: snapshotID})
	}
	return q.UpdateSnapshotStreamValidationStatus(ctx, db.UpdateSnapshotStreamValidationStatusParams{StreamValidationStatus: status, ID: snapshotID})
}
