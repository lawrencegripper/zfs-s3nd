package web

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"time"
)

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status := http.StatusOK
	response := map[string]string{"status": "ok"}
	if err := s.catalog.Health(ctx); err != nil {
		status = http.StatusServiceUnavailable
		response["status"] = "error"
		response["sqlite"] = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	checks := s.readinessChecks(ctx)
	status := http.StatusOK
	response := map[string]string{"status": "ok"}
	for _, check := range checks {
		if check.OK {
			continue
		}
		status = http.StatusServiceUnavailable
		response["status"] = "error"
		response[check.Key] = check.Detail
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) readinessChecks(ctx context.Context) []statusCheck {
	checks := make([]statusCheck, 0, 3)

	dbCheck := statusCheck{Key: "sqlite", Name: "Catalog", Description: "SQLite is reachable."}
	if err := s.catalog.Health(ctx); err != nil {
		dbCheck.Detail = err.Error()
	} else {
		dbCheck.OK = true
		dbCheck.Detail = "OK"
	}
	checks = append(checks, dbCheck)

	storageCheck := statusCheck{Key: "storage", Name: "Storage", Description: "Write, read, delete."}
	if s.store == nil {
		storageCheck.Detail = "object store is not configured"
	} else {
		key := "@health/readyz"
		if _, err := s.store.PutBytes(ctx, key, []byte("ok")); err != nil {
			storageCheck.Detail = err.Error()
		} else if got, err := s.store.GetBytes(ctx, key); err != nil {
			storageCheck.Detail = err.Error()
		} else if string(got) != "ok" {
			storageCheck.Detail = "read-after-write mismatch"
		} else {
			storageCheck.OK = true
			storageCheck.Detail = "OK"
		}
		_ = s.store.Delete(ctx, key)
	}
	checks = append(checks, storageCheck)

	zstreamdumpCheck := statusCheck{Key: "zstreamdump", Name: "zstreamdump", Description: "Required for validation."}
	if _, err := exec.LookPath("zstreamdump"); err != nil {
		zstreamdumpCheck.Detail = err.Error()
	} else {
		zstreamdumpCheck.OK = true
		zstreamdumpCheck.Detail = "OK"
	}
	checks = append(checks, zstreamdumpCheck)

	return checks
}

type statusCheck struct {
	Key         string
	Name        string
	Description string
	OK          bool
	Detail      string
}

type statusData struct {
	Checks     []statusCheck
	Ready      bool
	ErrorCount int
	Message    string
	Error      string
	CSRFToken  string `json:"-"`
}

func (s *Server) statusPage(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	checks := s.readinessChecks(ctx)
	data := statusData{Checks: checks, Ready: true, Message: r.URL.Query().Get("message"), Error: r.URL.Query().Get("error"), CSRFToken: s.csrfToken(r)}
	for _, check := range checks {
		if !check.OK {
			data.Ready = false
			data.ErrorCount++
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := statusTemplate.Execute(w, data); err != nil {
		s.logger.Error("render status", "error", err)
	}
}
