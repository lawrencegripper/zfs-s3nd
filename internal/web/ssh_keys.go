package web

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"golang.org/x/crypto/ssh"
	"io"
	"net/http"
	"strings"
	"time"
)

var githubKeysBaseURL = "https://github.com"

func (s *Server) addSSHKey(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	publicKeyText := strings.TrimSpace(r.FormValue("public_key"))
	if _, err := s.createSSHKey(r.Context(), name, publicKeyText); err != nil {
		redirectDashboard(w, r, "", err.Error())
		return
	}
	redirectDashboard(w, r, "ssh key added", "")
}

func (s *Server) createSSHKey(ctx context.Context, name, publicKeyText string) (string, error) {
	if name == "" || publicKeyText == "" {
		return "", fmt.Errorf("name and public key are required")
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(publicKeyText))
	if err != nil {
		return "", fmt.Errorf("invalid SSH public key")
	}
	canonical := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub)))
	fingerprint := ssh.FingerprintSHA256(pub)
	q := db.New(s.catalog.DB())
	id := catalog.NewID("key")
	if err := q.CreateSSHKey(ctx, db.CreateSSHKeyParams{ID: id, Name: name, PublicKey: canonical, FingerprintSha256: fingerprint}); err != nil {
		return "", fmt.Errorf("could not add SSH key; it may already exist")
	}
	return id, nil
}

func (s *Server) addGitHubSSHKeys(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	username := strings.TrimSpace(r.FormValue("github_username"))
	added, skipped, err := s.importGitHubSSHKeys(r.Context(), username)
	if err != nil {
		redirectDashboard(w, r, "", err.Error())
		return
	}
	message := fmt.Sprintf("imported %d GitHub SSH key(s)", added)
	if skipped > 0 {
		message = fmt.Sprintf("%s; skipped %d existing key(s)", message, skipped)
	}
	redirectDashboard(w, r, message, "")
}

func (s *Server) importGitHubSSHKeys(ctx context.Context, username string) (int, int, error) {
	if !isSafeGitHubUsername(username) {
		return 0, 0, fmt.Errorf("GitHub username is invalid")
	}
	keys, err := fetchGitHubKeys(ctx, username)
	if err != nil {
		return 0, 0, err
	}
	if len(keys) == 0 {
		return 0, 0, fmt.Errorf("GitHub user %q has no public SSH keys", username)
	}
	added, skipped := 0, 0
	for i, key := range keys {
		name := fmt.Sprintf("github:%s:%d", username, i+1)
		if _, err := s.createSSHKey(ctx, name, key); err != nil {
			if strings.Contains(err.Error(), "already exist") {
				skipped++
				continue
			}
			return added, skipped, err
		}
		added++
	}
	if added == 0 && skipped > 0 {
		return added, skipped, fmt.Errorf("all GitHub SSH keys for %q already exist", username)
	}
	return added, skipped, nil
}

func fetchGitHubKeys(ctx context.Context, username string) ([]string, error) {
	url := strings.TrimRight(githubKeysBaseURL, "/") + "/" + username + ".keys"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch GitHub keys: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("GitHub user %q not found or has no public SSH keys", username)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetch GitHub keys: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return nil, fmt.Errorf("read GitHub keys: %w", err)
	}
	var keys []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("GitHub returned an invalid SSH key")
		}
		keys = append(keys, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))))
	}
	return keys, nil
}

func isSafeGitHubUsername(value string) bool {
	if value == "" || len(value) > 39 || strings.HasPrefix(value, "-") || strings.HasSuffix(value, "-") {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

func (s *Server) deleteSSHKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if err := db.New(s.catalog.DB()).DeleteSSHKey(r.Context(), id); err != nil {
		redirectDashboard(w, r, "", "could not delete SSH key")
		return
	}
	redirectDashboard(w, r, "ssh key deleted", "")
}

func (s *Server) apiListSSHKeys(w http.ResponseWriter, r *http.Request) {
	rows, err := db.New(s.catalog.DB()).ListSSHKeys(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("list ssh keys: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, rows)
}

func (s *Server) apiAddSSHKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	id, err := s.createSSHKey(r.Context(), strings.TrimSpace(req.Name), strings.TrimSpace(req.PublicKey))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"id": id})
}

func (s *Server) apiDeleteSSHKey(w http.ResponseWriter, r *http.Request) {
	if err := db.New(s.catalog.DB()).DeleteSSHKey(r.Context(), r.PathValue("id")); err != nil {
		http.Error(w, fmt.Sprintf("delete ssh key: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "deleted"})
}
