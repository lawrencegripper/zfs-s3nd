package web

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

func redirectTo(w http.ResponseWriter, r *http.Request, location string) {
	http.Redirect(w, r, location, http.StatusSeeOther)
}

func redirectDashboard(w http.ResponseWriter, r *http.Request, message, errorMessage string) {
	values := make([]string, 0, 2)
	if message != "" {
		values = append(values, "message="+urlQueryEscape(message))
	}
	if errorMessage != "" {
		values = append(values, "error="+urlQueryEscape(errorMessage))
	}
	location := "/"
	if len(values) > 0 {
		location += "?" + strings.Join(values, "&")
	}
	http.Redirect(w, r, location, http.StatusSeeOther)
}

func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}

func int64Value(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case []byte:
		parsed, _ := strconv.ParseInt(string(v), 10, 64)
		return parsed
	case string:
		parsed, _ := strconv.ParseInt(v, 10, 64)
		return parsed
	default:
		return 0
	}
}

func formatBytes(bytes int64) string {
	if bytes < 0 {
		bytes = 0
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	value := float64(bytes)
	unit := 0
	for value >= 1000 && unit < len(units)-1 {
		value /= 1000
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d B", bytes)
	}
	formatted := fmt.Sprintf("%.1f", value)
	formatted = strings.TrimSuffix(formatted, ".0")
	return formatted + " " + units[unit]
}

func formatNullBytes(bytes sql.NullInt64) string {
	if !bytes.Valid {
		return ""
	}
	return formatBytes(bytes.Int64)
}

func formatBitrate(bytes int64, elapsed time.Duration) string {
	if bytes <= 0 || elapsed <= 0 {
		return "0 Mbps"
	}
	bps := float64(bytes) * 8 / elapsed.Seconds()
	units := []string{"bps", "Kbps", "Mbps", "Gbps", "Tbps"}
	unit := 0
	for bps >= 1000 && unit < len(units)-1 {
		bps /= 1000
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%.0f %s", bps, units[unit])
	}
	formatted := fmt.Sprintf("%.1f", bps)
	formatted = strings.TrimSuffix(formatted, ".0")
	return formatted + " " + units[unit]
}

func activeUploadViews(rows []db.ListActiveUploadsRow, now time.Time) []activeUploadView {
	views := make([]activeUploadView, 0, len(rows))
	for _, row := range rows {
		startedAt, err := time.Parse(time.RFC3339Nano, row.StartedAt)
		throughput := "0 Mbps"
		if err == nil {
			throughput = formatBitrate(row.BytesReceived, now.Sub(startedAt))
		}
		views = append(views, activeUploadView{
			SourceName:         row.SourceName,
			PoolName:           row.PoolName,
			DatasetPath:        row.DatasetPath,
			TargetSnapshotName: row.TargetSnapshotName,
			Status:             row.Status,
			BytesReceived:      row.BytesReceived,
			ChunksCompleted:    row.ChunksCompleted,
			StartedAt:          row.StartedAt,
			LastHeartbeatAt:    row.LastHeartbeatAt,
			Throughput:         throughput,
		})
	}
	return views
}

func formatDisplayTime(value interface{}) string {
	raw := ""
	switch v := value.(type) {
	case sql.NullString:
		if !v.Valid {
			return ""
		}
		raw = v.String
	case string:
		raw = v
	case nil:
		return ""
	default:
		raw = fmt.Sprint(v)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	return parsed.UTC().Format("2006-01-02 15:04 UTC")
}

func humanLabel(value interface{}) string {
	raw := strings.TrimSpace(fmt.Sprint(value))
	if raw == "" {
		return ""
	}
	raw = strings.ReplaceAll(raw, "_", " ")
	return strings.ToUpper(raw[:1]) + raw[1:]
}

func statusBadgeClass(status interface{}) string {
	value := strings.ToLower(strings.TrimSpace(fmt.Sprint(status)))
	switch value {
	case "succeeded", "healthy", "chain-valid", "committed", "0":
		return "badge-green"
	case "failed", "error", "missing":
		return "badge-red"
	case "":
		return "badge-yellow"
	default:
		return "badge-yellow"
	}
}

func countBadgeClass(count int64) string {
	if count > 0 {
		return "badge-red"
	}
	return "badge-green"
}
