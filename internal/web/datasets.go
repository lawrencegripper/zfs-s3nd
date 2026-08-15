package web

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"net/http"
)

type datasetsData struct {
	Datasets  []db.ListDatasetSummariesRow
	Message   string
	Error     string
	CSRFToken string `json:"-"`
}

type datasetDetailData struct {
	Dataset         db.GetDatasetDetailRow
	Snapshots       []db.ListDatasetSnapshotsRow
	RestoreSnapshot string
	RestoreCommands []string
	Message         string
	Error           string
	CSRFToken       string `json:"-"`
}

type snapshotDetailData struct {
	Snapshot        db.GetSnapshotDetailRow
	RestoreCommands []string
	Message         string
	Error           string
	CSRFToken       string `json:"-"`
}

func (s *Server) datasets(w http.ResponseWriter, r *http.Request) {
	rows, err := db.New(s.catalog.DB()).ListDatasetSummaries(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("list datasets: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := datasetsTemplate.Execute(w, datasetsData{Datasets: rows, Message: r.URL.Query().Get("message"), Error: r.URL.Query().Get("error"), CSRFToken: s.csrfToken(r)}); err != nil {
		s.logger.Error("render datasets", "error", err)
	}
}

func (s *Server) datasetDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := db.New(s.catalog.DB())
	dataset, err := q.GetDatasetDetail(r.Context(), id)
	if err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("get dataset: %v", err), http.StatusInternalServerError)
		return
	}
	snapshots, err := q.ListDatasetSnapshots(r.Context(), id)
	if err != nil {
		http.Error(w, fmt.Sprintf("list snapshots: %v", err), http.StatusInternalServerError)
		return
	}
	data := datasetDetailData{Dataset: dataset, Snapshots: snapshots, Message: r.URL.Query().Get("message"), Error: r.URL.Query().Get("error"), CSRFToken: s.csrfToken(r)}
	for _, snapshot := range snapshots {
		if snapshot.Status != "committed" || snapshot.ChainValidationStatus != "succeeded" {
			continue
		}
		chain, err := q.ListSnapshotRestoreChain(r.Context(), snapshot.ID)
		if err != nil {
			http.Error(w, fmt.Sprintf("build restore chain: %v", err), http.StatusInternalServerError)
			return
		}
		data.RestoreSnapshot = snapshot.Name
		data.RestoreCommands = restoreCommandsFor(dataset.PoolName, dataset.DatasetPath, chain, s.restoreSSHCommandPrefix)
		break
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := datasetDetailTemplate.Execute(w, data); err != nil {
		s.logger.Error("render dataset detail", "error", err)
	}
}

func (s *Server) snapshotDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := db.New(s.catalog.DB())
	snapshot, err := q.GetSnapshotDetail(r.Context(), id)
	if err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("get snapshot: %v", err), http.StatusInternalServerError)
		return
	}
	chain, err := q.ListSnapshotRestoreChain(r.Context(), id)
	if err != nil {
		http.Error(w, fmt.Sprintf("build restore chain: %v", err), http.StatusInternalServerError)
		return
	}
	data := snapshotDetailData{Snapshot: snapshot, RestoreCommands: restoreCommands(snapshot, chain, s.restoreSSHCommandPrefix), Message: r.URL.Query().Get("message"), Error: r.URL.Query().Get("error"), CSRFToken: s.csrfToken(r)}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := snapshotDetailTemplate.Execute(w, data); err != nil {
		s.logger.Error("render snapshot detail", "error", err)
	}
}

func (s *Server) validateDataset(w http.ResponseWriter, r *http.Request) {
	if s.validationRunner == nil {
		redirectTo(w, r, fmt.Sprintf("/datasets/%s?error=%s", r.PathValue("id"), urlQueryEscape("validation runner is not configured")))
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	q := db.New(s.catalog.DB())
	if _, err := q.GetDatasetDetail(r.Context(), id); err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("get dataset: %v", err), http.StatusInternalServerError)
		return
	}
	snapshots, err := q.ListDatasetSnapshots(r.Context(), id)
	if err != nil {
		http.Error(w, fmt.Sprintf("list snapshots: %v", err), http.StatusInternalServerError)
		return
	}
	ids := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Status == "committed" {
			ids = append(ids, snapshot.ID)
		}
	}
	if len(ids) == 0 {
		redirectTo(w, r, fmt.Sprintf("/datasets/%s?error=%s", id, urlQueryEscape("no committed snapshots to validate")))
		return
	}
	runner := *s.validationRunner
	go func() {
		ctx := context.Background()
		for _, snapshotID := range ids {
			if err := runner.RunChain(ctx, snapshotID); err != nil {
				s.logger.Warn("manual dataset validation failed", "dataset_id", id, "snapshot_id", snapshotID, "error", err)
				continue
			}
			s.logger.Info("manual dataset validation succeeded", "dataset_id", id, "snapshot_id", snapshotID)
		}
	}()
	redirectTo(w, r, "/activity?message="+urlQueryEscape(fmt.Sprintf("chain validation started for %d snapshot(s)", len(ids))))
}

func (s *Server) validateSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.validationRunner == nil {
		http.Error(w, "validation runner is not configured", http.StatusServiceUnavailable)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := db.New(s.catalog.DB()).GetSnapshotDetail(r.Context(), id); err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("get snapshot: %v", err), http.StatusInternalServerError)
		return
	}
	mode := r.FormValue("mode")
	if mode == "" {
		mode = "stream"
	}
	if mode != "stream" && mode != "chain" {
		redirectTo(w, r, fmt.Sprintf("/snapshots/%s?error=%s", id, urlQueryEscape("validation mode must be stream or chain")))
		return
	}
	runner := *s.validationRunner
	go func() {
		ctx := context.Background()
		var err error
		if mode == "chain" {
			err = runner.RunChain(ctx, id)
		} else {
			err = runner.RunSnapshot(ctx, id)
		}
		if err != nil {
			s.logger.Warn("manual snapshot validation failed", "snapshot_id", id, "mode", mode, "error", err)
			return
		}
		s.logger.Info("manual snapshot validation succeeded", "snapshot_id", id, "mode", mode)
	}()
	redirectTo(w, r, "/activity?message="+urlQueryEscape(fmt.Sprintf("%s validation started", mode)))
}

func (s *Server) deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		redirectDashboard(w, r, "", "object store is not configured")
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	snapshot, err := db.New(s.catalog.DB()).GetSnapshotDetail(r.Context(), id)
	if err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("get snapshot: %v", err), http.StatusInternalServerError)
		return
	}
	if _, err := s.queueSnapshotDeletion(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectTo(w, r, fmt.Sprintf("/datasets/%s?message=%s", snapshot.DatasetID, urlQueryEscape("snapshot deletion queued; objects and catalog rows will be removed by the cleanup worker")))
}

func (s *Server) deleteDataset(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		redirectDashboard(w, r, "", "object store is not configured")
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := s.queueDatasetDeletion(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectDashboard(w, r, "dataset deletion queued; objects and catalog rows will be removed by the cleanup worker", "")
}

func (s *Server) queueSnapshotDeletion(ctx context.Context, id string) (string, error) {
	if s.store == nil {
		return "", fmt.Errorf("object store is not configured")
	}
	q := db.New(s.catalog.DB())
	snapshot, err := q.GetSnapshotDetail(ctx, id)
	if err != nil {
		return "", fmt.Errorf("get snapshot: %w", err)
	}
	descendants, err := q.CountCommittedDescendants(ctx, sql.NullString{String: id, Valid: true})
	if err != nil {
		return "", fmt.Errorf("count committed snapshot descendants: %w", err)
	}
	if descendants > 0 {
		return "", fmt.Errorf("snapshot has %d committed descendant(s); delete the dependent chain or dataset instead", descendants)
	}
	opID := catalog.NewID("op")
	if err := q.CreateOperation(ctx, db.CreateOperationParams{ID: opID, Type: "cleanup", Status: "queued", DatasetID: sql.NullString{String: snapshot.DatasetID, Valid: true}, SnapshotID: sql.NullString{String: id, Valid: true}, Summary: sql.NullString{String: "snapshot deletion queued", Valid: true}}); err != nil {
		return "", fmt.Errorf("create deletion operation: %w", err)
	}
	return opID, nil
}

func (s *Server) queueDatasetDeletion(ctx context.Context, id string) (string, error) {
	if s.store == nil {
		return "", fmt.Errorf("object store is not configured")
	}
	if _, err := db.New(s.catalog.DB()).GetDatasetDetail(ctx, id); err != nil {
		return "", fmt.Errorf("get dataset: %w", err)
	}
	opID := catalog.NewID("op")
	if err := db.New(s.catalog.DB()).CreateOperation(ctx, db.CreateOperationParams{ID: opID, Type: "cleanup", Status: "queued", DatasetID: sql.NullString{String: id, Valid: true}, Summary: sql.NullString{String: "dataset deletion queued", Valid: true}}); err != nil {
		return "", fmt.Errorf("create deletion operation: %w", err)
	}
	return opID, nil
}
