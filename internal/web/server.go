package web

import (
	"context"
	"github.com/lawrencegripper/zfs-s3nd/internal/appsettings"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/storage"
	"github.com/lawrencegripper/zfs-s3nd/internal/validation"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Server struct {
	catalog                 *catalog.Catalog
	logger                  *slog.Logger
	adminPassword           string
	validationRunner        *validation.Runner
	store                   storage.Store
	restoreSSHCommandPrefix string
	settingsManager         *appsettings.Manager
	sessionKey              []byte
	disableAuth             bool
	loginMu                 sync.Mutex
	loginAttempts           map[string]loginAttempt
	http                    *http.Server
}

func New(addr string, cat *catalog.Catalog, logger *slog.Logger, adminPassword ...string) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	password := ""
	if len(adminPassword) > 0 {
		password = adminPassword[0]
	}
	s := &Server{catalog: cat, logger: logger, adminPassword: password, restoreSSHCommandPrefix: "ssh [named_source]@<ssh-host>", sessionKey: randomSessionKey(), loginAttempts: make(map[string]loginAttempt)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /login", s.loginPage)
	mux.HandleFunc("POST /login", s.login)
	mux.HandleFunc("POST /logout", s.logout)
	mux.HandleFunc("GET /setup", s.setupPage)
	mux.HandleFunc("POST /setup", s.setup)
	mux.HandleFunc("GET /", s.dashboard)
	mux.HandleFunc("GET /activity", s.activity)
	mux.HandleFunc("GET /status", s.statusPage)
	mux.HandleFunc("GET /settings", s.settings)
	mux.HandleFunc("POST /settings/runtime/{key}", s.updateRuntimeSetting)
	mux.HandleFunc("POST /settings/runtime/{key}/reset", s.resetRuntimeSetting)
	mux.HandleFunc("GET /datasets", s.datasets)
	mux.HandleFunc("GET /datasets/{id}", s.datasetDetail)
	mux.HandleFunc("POST /datasets/{id}/validate", s.validateDataset)
	mux.HandleFunc("GET /snapshots/{id}", s.snapshotDetail)
	mux.HandleFunc("POST /snapshots/{id}/validate", s.validateSnapshot)
	mux.HandleFunc("POST /snapshots/{id}/delete", s.deleteSnapshot)
	mux.HandleFunc("POST /datasets/{id}/delete", s.deleteDataset)
	mux.HandleFunc("POST /settings/api-tokens", s.addAPIToken)
	mux.HandleFunc("POST /settings/api-tokens/{id}/delete", s.deleteAPIToken)
	mux.HandleFunc("POST /settings/ssh-keys", s.addSSHKey)
	mux.HandleFunc("POST /settings/ssh-keys/github", s.addGitHubSSHKeys)
	mux.HandleFunc("POST /settings/ssh-keys/{id}/delete", s.deleteSSHKey)
	mux.HandleFunc("GET /api/v1/dashboard", s.apiDashboard)
	mux.HandleFunc("GET /api/v1/datasets", s.apiDatasets)
	mux.HandleFunc("GET /api/v1/datasets/{id}", s.apiDatasetDetail)
	mux.HandleFunc("DELETE /api/v1/datasets/{id}", s.apiDeleteDataset)
	mux.HandleFunc("GET /api/v1/snapshots/{id}", s.apiSnapshotDetail)
	mux.HandleFunc("POST /api/v1/snapshots/{id}/validate", s.apiValidateSnapshot)
	mux.HandleFunc("DELETE /api/v1/snapshots/{id}", s.apiDeleteSnapshot)
	mux.HandleFunc("GET /api/v1/ssh-keys", s.apiListSSHKeys)
	mux.HandleFunc("POST /api/v1/ssh-keys", s.apiAddSSHKey)
	mux.HandleFunc("DELETE /api/v1/ssh-keys/{id}", s.apiDeleteSSHKey)
	mux.HandleFunc("GET /api/v1/api-tokens", s.apiListAPITokens)
	mux.HandleFunc("POST /api/v1/api-tokens", s.apiAddAPIToken)
	mux.HandleFunc("DELETE /api/v1/api-tokens/{id}", s.apiDeleteAPIToken)
	mux.HandleFunc("POST /api/v1/admin/validation/run", s.runValidation)
	mux.HandleFunc("POST /admin/validation/run", s.runValidation)
	s.http = &http.Server{
		Addr:              addr,
		Handler:           requestLogger(logger, s.authMiddleware(mux)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.http.Handler.ServeHTTP(w, r)
}

func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) SetSettingsManager(manager *appsettings.Manager) {
	s.settingsManager = manager
}

func (s *Server) SetValidationRunner(runner validation.Runner) {
	s.validationRunner = &runner
}

func (s *Server) SetStore(store storage.Store) {
	s.store = store
}

func (s *Server) SetRestoreSSHCommandPrefix(prefix string) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return
	}
	s.restoreSSHCommandPrefix = prefix
}
