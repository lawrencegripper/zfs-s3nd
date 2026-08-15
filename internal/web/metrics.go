package web

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	body, err := s.prometheusMetrics(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("metrics: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, body)
}

func (s *Server) prometheusMetrics(ctx context.Context) (string, error) {
	q := db.New(s.catalog.DB())
	var b strings.Builder
	writeGauge := func(name, help string, value interface{}) {
		_, _ = fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, value)
	}

	sourceCount, err := q.CountSources(ctx)
	if err != nil {
		return "", fmt.Errorf("count sources: %w", err)
	}
	writeGauge("zfs_s3end_sources", "Configured backup sources.", sourceCount)

	datasetCount, err := q.CountDatasets(ctx)
	if err != nil {
		return "", fmt.Errorf("count datasets: %w", err)
	}
	writeGauge("zfs_s3end_datasets", "Known datasets.", datasetCount)

	snapshotCount, err := q.CountCommittedSnapshots(ctx)
	if err != nil {
		return "", fmt.Errorf("count snapshots: %w", err)
	}
	writeGauge("zfs_s3end_committed_snapshots", "Committed snapshots.", snapshotCount)

	storedBytes, err := q.SumStoredBytes(ctx)
	if err != nil {
		return "", fmt.Errorf("sum stored bytes: %w", err)
	}
	writeGauge("zfs_s3end_stored_bytes", "Stored snapshot bytes.", storedBytes)

	failedUploads, err := q.CountFailedUploads(ctx)
	if err != nil {
		return "", fmt.Errorf("count failed uploads: %w", err)
	}
	writeGauge("zfs_s3end_failed_uploads", "Failed upload sessions.", failedUploads)

	failedValidations, err := q.CountFailedValidations(ctx)
	if err != nil {
		return "", fmt.Errorf("count failed validations: %w", err)
	}
	writeGauge("zfs_s3end_failed_chain_validations", "Failed chain validations.", failedValidations)

	activeUploads, err := q.ListActiveUploads(ctx)
	if err != nil {
		return "", fmt.Errorf("list active uploads: %w", err)
	}
	writeGauge("zfs_s3end_active_uploads", "Active upload sessions.", len(activeUploads))

	latestBackup, err := q.GetLatestCatalogBackup(ctx)
	if err == nil {
		_, _ = fmt.Fprintf(&b, "# HELP zfs_s3end_catalog_backup_status Latest catalog backup status.\n# TYPE zfs_s3end_catalog_backup_status gauge\nzfs_s3end_catalog_backup_status{status=%s} 1\n", promLabelValue(latestBackup.Status))
		if latestBackup.CompletedAt.Valid {
			if completedAt, err := time.Parse(time.RFC3339Nano, latestBackup.CompletedAt.String); err == nil {
				writeGauge("zfs_s3end_catalog_backup_last_completed_timestamp_seconds", "Last completed catalog backup time.", completedAt.Unix())
			}
		}
	} else if err != sql.ErrNoRows {
		return "", fmt.Errorf("latest catalog backup: %w", err)
	}

	return b.String(), nil
}

func promLabelValue(value string) string {
	return strconv.Quote(value)
}
