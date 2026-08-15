package web

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

type dashboardData struct {
	SourceCount       int64
	DatasetCount      int64
	SnapshotCount     int64
	FailedUploads     int64
	FailedValidations int64
	StoredBytes       int64
	ActiveUploads     []activeUploadView
	Operations        []db.GetLatestOperationsRow
	ValidationJobs    []db.ListLatestValidationJobsRow
	SSHKeys           []db.SshKey
	APITokens         []db.ListAPITokensRow
	LatestBackup      *db.CatalogBackup
	Datasets          []db.ListDatasetSummariesRow
	Message           string
	Error             string
	AddKeyName        string
	AddKeyPublicKey   string
	NASKeygenScript   string
	FirstUploadScript string
	CSRFToken         string `json:"-"`
}

type activeUploadView struct {
	SourceName         string
	PoolName           string
	DatasetPath        string
	TargetSnapshotName string
	Status             string
	BytesReceived      int64
	ChunksCompleted    int64
	StartedAt          string
	LastHeartbeatAt    string
	Throughput         string
}

type activityData struct {
	ActiveUploads  []activeUploadView
	Operations     []db.GetLatestOperationsRow
	ValidationJobs []db.ListLatestValidationJobsRow
	LatestBackup   *db.CatalogBackup
	Message        string
	Error          string
	CSRFToken      string `json:"-"`
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	data, err := s.loadDashboard(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.Message = r.URL.Query().Get("message")
	data.Error = r.URL.Query().Get("error")
	data.CSRFToken = s.csrfToken(r)
	data.NASKeygenScript = nasKeygenScript()
	data.FirstUploadScript = firstUploadScript(s.uploadSSHCommandPrefix())

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := dashboardTemplate.Execute(w, data); err != nil {
		s.logger.Error("render dashboard", "error", err)
	}
}

func (s *Server) loadDashboard(ctx context.Context) (dashboardData, error) {
	q := db.New(s.catalog.DB())
	var data dashboardData
	var err error
	if data.SourceCount, err = q.CountSources(ctx); err != nil {
		return data, fmt.Errorf("count sources: %w", err)
	}
	if data.DatasetCount, err = q.CountDatasets(ctx); err != nil {
		return data, fmt.Errorf("count datasets: %w", err)
	}
	if data.SnapshotCount, err = q.CountCommittedSnapshots(ctx); err != nil {
		return data, fmt.Errorf("count snapshots: %w", err)
	}
	if data.FailedUploads, err = q.CountFailedUploads(ctx); err != nil {
		return data, fmt.Errorf("count failed uploads: %w", err)
	}
	if data.FailedValidations, err = q.CountFailedValidations(ctx); err != nil {
		return data, fmt.Errorf("count failed validations: %w", err)
	}
	stored, err := q.SumStoredBytes(ctx)
	if err != nil {
		return data, fmt.Errorf("sum stored bytes: %w", err)
	}
	data.StoredBytes = stored
	activeUploads, err := q.ListActiveUploads(ctx)
	if err != nil {
		return data, fmt.Errorf("active uploads: %w", err)
	}
	data.ActiveUploads = activeUploadViews(activeUploads, time.Now())
	data.Operations, err = q.GetLatestOperations(ctx, 20)
	if err != nil {
		return data, fmt.Errorf("latest operations: %w", err)
	}
	data.ValidationJobs, err = q.ListLatestValidationJobs(ctx, 10)
	if err != nil {
		return data, fmt.Errorf("latest validation jobs: %w", err)
	}
	data.SSHKeys, err = q.ListSSHKeys(ctx)
	if err != nil {
		return data, fmt.Errorf("list ssh keys: %w", err)
	}
	data.APITokens, err = q.ListAPITokens(ctx)
	if err != nil {
		return data, fmt.Errorf("list api tokens: %w", err)
	}
	data.Datasets, err = q.ListDatasetSummaries(ctx)
	if err != nil {
		return data, fmt.Errorf("list datasets: %w", err)
	}
	if len(data.Datasets) > 5 {
		data.Datasets = data.Datasets[:5]
	}
	latestBackup, err := q.GetLatestCatalogBackup(ctx)
	if err == nil {
		data.LatestBackup = &latestBackup
	} else if err != sql.ErrNoRows {
		return data, fmt.Errorf("latest catalog backup: %w", err)
	}
	return data, nil
}

func (s *Server) activity(w http.ResponseWriter, r *http.Request) {
	data, err := s.loadActivity(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.Message = r.URL.Query().Get("message")
	data.Error = r.URL.Query().Get("error")
	data.CSRFToken = s.csrfToken(r)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := activityTemplate.Execute(w, data); err != nil {
		s.logger.Error("render activity", "error", err)
	}
}

func (s *Server) loadActivity(ctx context.Context) (activityData, error) {
	q := db.New(s.catalog.DB())
	var data activityData
	activeUploads, err := q.ListActiveUploads(ctx)
	if err != nil {
		return data, fmt.Errorf("active uploads: %w", err)
	}
	data.ActiveUploads = activeUploadViews(activeUploads, time.Now())
	data.Operations, err = q.GetLatestOperations(ctx, 50)
	if err != nil {
		return data, fmt.Errorf("latest operations: %w", err)
	}
	data.ValidationJobs, err = q.ListLatestValidationJobs(ctx, 50)
	if err != nil {
		return data, fmt.Errorf("latest validation jobs: %w", err)
	}
	latestBackup, err := q.GetLatestCatalogBackup(ctx)
	if err == nil {
		data.LatestBackup = &latestBackup
	} else if err != sql.ErrNoRows {
		return data, fmt.Errorf("latest catalog backup: %w", err)
	}
	return data, nil
}
