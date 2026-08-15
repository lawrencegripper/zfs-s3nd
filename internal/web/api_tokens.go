package web

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"net/http"
	"strings"
)

func generateAPIToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "zs3_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

func hashAPIToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func apiTokenPrefix(token string) string {
	if len(token) <= 12 {
		return token
	}
	return token[:12]
}

func (s *Server) addAPIToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		redirectDashboard(w, r, "", "api token name is required")
		return
	}
	token, row, err := s.createAPIToken(r.Context(), name)
	if err != nil {
		http.Error(w, fmt.Sprintf("create api token: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = apiTokenCreatedTemplate.Execute(w, map[string]string{"Name": row.Name, "Prefix": row.TokenPrefix, "Token": token})
}

func (s *Server) deleteAPIToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if err := db.New(s.catalog.DB()).RevokeAPIToken(r.Context(), id); err != nil {
		redirectDashboard(w, r, "", "could not revoke API token")
		return
	}
	redirectDashboard(w, r, "api token revoked", "")
}

func (s *Server) createAPIToken(ctx context.Context, name string) (string, db.ListAPITokensRow, error) {
	q := db.New(s.catalog.DB())
	name = strings.TrimSpace(name)
	if name == "" {
		return "", db.ListAPITokensRow{}, fmt.Errorf("api token name is required")
	}
	token, err := generateAPIToken()
	if err != nil {
		return "", db.ListAPITokensRow{}, err
	}
	id := catalog.NewID("tok")
	prefix := apiTokenPrefix(token)
	if err := q.CreateAPIToken(ctx, db.CreateAPITokenParams{ID: id, Name: name, TokenHash: hashAPIToken(token), TokenPrefix: prefix}); err != nil {
		return "", db.ListAPITokensRow{}, err
	}
	return token, db.ListAPITokensRow{ID: id, Name: name, TokenPrefix: prefix}, nil
}

func (s *Server) apiListAPITokens(w http.ResponseWriter, r *http.Request) {
	rows, err := db.New(s.catalog.DB()).ListAPITokens(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("list api tokens: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (s *Server) apiAddAPIToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	token, row, err := s.createAPIToken(r.Context(), strings.TrimSpace(req.Name))
	if err != nil {
		http.Error(w, fmt.Sprintf("create api token: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"token": token, "api_token": row})
}

func (s *Server) apiDeleteAPIToken(w http.ResponseWriter, r *http.Request) {
	if err := db.New(s.catalog.DB()).RevokeAPIToken(r.Context(), r.PathValue("id")); err != nil {
		http.Error(w, fmt.Sprintf("revoke api token: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "revoked"})
}
