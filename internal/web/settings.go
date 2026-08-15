package web

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
)

type settingsData struct {
	SSHKeys            []db.SshKey
	APITokens          []db.ListAPITokensRow
	NASKeygenScript    string
	Message            string
	Error              string
	RuntimeSettings    appsettings.Snapshot
	HasRuntimeSettings bool
	CSRFToken          string `json:"-"`
}

func (s *Server) settings(w http.ResponseWriter, r *http.Request) {
	data, err := s.loadSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.Message = r.URL.Query().Get("message")
	data.Error = r.URL.Query().Get("error")
	data.CSRFToken = s.csrfToken(r)
	data.NASKeygenScript = nasKeygenScript()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := settingsTemplate.Execute(w, data); err != nil {
		s.logger.Error("render settings", "error", err)
	}
}

func (s *Server) loadSettings(ctx context.Context) (settingsData, error) {
	q := db.New(s.catalog.DB())
	var data settingsData
	var err error
	data.SSHKeys, err = q.ListSSHKeys(ctx)
	if err != nil {
		return data, fmt.Errorf("list ssh keys: %w", err)
	}
	data.APITokens, err = q.ListAPITokens(ctx)
	if err != nil {
		return data, fmt.Errorf("list api tokens: %w", err)
	}
	if s.settingsManager != nil {
		data.RuntimeSettings = s.settingsManager.Current()
		data.HasRuntimeSettings = true
	}
	return data, nil
}

func (s *Server) updateRuntimeSetting(w http.ResponseWriter, r *http.Request) {
	if s.settingsManager == nil {
		http.Error(w, "runtime settings are unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key := r.PathValue("key")
	if err := s.settingsManager.Set(r.Context(), key, strings.TrimSpace(r.FormValue("value"))); err != nil {
		redirectSettings(w, r, "", err.Error())
		return
	}
	redirectSettings(w, r, "setting updated", "")
}

func (s *Server) resetRuntimeSetting(w http.ResponseWriter, r *http.Request) {
	if s.settingsManager == nil {
		http.Error(w, "runtime settings are unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := s.settingsManager.Reset(r.Context(), r.PathValue("key")); err != nil {
		redirectSettings(w, r, "", err.Error())
		return
	}
	redirectSettings(w, r, "setting reset to default", "")
}

func redirectSettings(w http.ResponseWriter, r *http.Request, message, errorMessage string) {
	query := url.Values{}
	if message != "" {
		query.Set("message", message)
	}
	if errorMessage != "" {
		query.Set("error", errorMessage)
	}
	location := "/settings"
	if encoded := query.Encode(); encoded != "" {
		location += "?" + encoded
	}
	http.Redirect(w, r, location, http.StatusSeeOther)
}
