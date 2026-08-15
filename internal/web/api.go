package web

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

func (s *Server) apiDashboard(w http.ResponseWriter, r *http.Request) {
	data, err := s.loadDashboard(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, data)
}

func (s *Server) apiDatasets(w http.ResponseWriter, r *http.Request) {
	rows, err := db.New(s.catalog.DB()).ListDatasetSummaries(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("list datasets: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (s *Server) apiDatasetDetail(w http.ResponseWriter, r *http.Request) {
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
	writeJSON(w, datasetDetailData{Dataset: dataset, Snapshots: snapshots})
}

func (s *Server) apiSnapshotDetail(w http.ResponseWriter, r *http.Request) {
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
	writeJSON(w, snapshotDetailData{Snapshot: snapshot, RestoreCommands: restoreCommands(snapshot, chain, s.restoreSSHCommandPrefix)})
}

func (s *Server) apiValidateSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.validationRunner == nil {
		http.Error(w, "validation runner is not configured", http.StatusServiceUnavailable)
		return
	}
	id := r.PathValue("id")
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "stream"
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
			s.logger.Warn("api snapshot validation failed", "snapshot_id", id, "mode", mode, "error", err)
		}
	}()
	writeJSON(w, map[string]string{"status": "queued", "snapshot_id": id, "mode": mode})
}

func (s *Server) apiDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	opID, err := s.queueSnapshotDeletion(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "queued", "operation_id": opID})
}

func (s *Server) apiDeleteDataset(w http.ResponseWriter, r *http.Request) {
	opID, err := s.queueDatasetDeletion(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "queued", "operation_id": opID})
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Server) runValidation(w http.ResponseWriter, r *http.Request) {
	if s.validationRunner == nil {
		http.Error(w, "validation runner is not configured", http.StatusServiceUnavailable)
		return
	}
	limit := int64(25)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	result, err := s.validationRunner.RunDue(r.Context(), limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("run validation: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
