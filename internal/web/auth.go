package web

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/lawrencegripper/zfs-s3nd/internal/catalog/db"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type loginAttempt struct {
	Failures     int
	Last         time.Time
	BlockedUntil time.Time
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.disableAuth {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		configured, err := s.adminConfigured(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("check admin setup: %v", err), http.StatusInternalServerError)
			return
		}
		if !configured {
			if r.URL.Path == "/setup" {
				next.ServeHTTP(w, r)
				return
			}
			http.Redirect(w, r, "/setup", http.StatusSeeOther)
			return
		}
		if r.URL.Path == "/login" || r.URL.Path == "/setup" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/metrics" {
			key := "api:" + loginRateKey(r)
			if !s.allowLoginAttempt(key, time.Now()) {
				http.Error(w, "too many authentication attempts", http.StatusTooManyRequests)
				return
			}
			if token := bearerToken(r); token != "" && s.verifyAPIToken(r.Context(), token) {
				s.recordLoginSuccess(key)
				next.ServeHTTP(w, r)
				return
			}
			s.recordLoginFailure(key, time.Now())
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if token := bearerToken(r); token != "" {
			key := "api:" + loginRateKey(r)
			if !s.allowLoginAttempt(key, time.Now()) {
				http.Error(w, "too many authentication attempts", http.StatusTooManyRequests)
				return
			}
			if s.verifyAPIToken(r.Context(), token) {
				s.recordLoginSuccess(key)
				next.ServeHTTP(w, r)
				return
			}
			s.recordLoginFailure(key, time.Now())
			s.logger.Warn("api token authentication failed", "remote_addr", r.RemoteAddr, "path", r.URL.Path)
		}
		if cookie, err := r.Cookie("zfs_s3end_session"); err == nil && s.verifySessionCookie(cookie.Value) {
			if isUnsafeMethod(r.Method) && !s.verifyCSRFToken(r, cookie.Value) {
				http.Error(w, "invalid csrf token", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		if isBrowserRequest(r) {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func (s *Server) adminConfigured(ctx context.Context) (bool, error) {
	if s.adminPassword != "" {
		return true, nil
	}
	credentials, err := db.New(s.catalog.DB()).GetAdminCredentials(ctx)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return credentials.PasswordHash.Valid && credentials.PasswordHash.String != "", nil
}

func (s *Server) verifyAdminPassword(ctx context.Context, password string) bool {
	if s.adminPassword != "" {
		return subtle.ConstantTimeCompare([]byte(password), []byte(s.adminPassword)) == 1
	}
	credentials, err := db.New(s.catalog.DB()).GetAdminCredentials(ctx)
	if err != nil || !credentials.PasswordHash.Valid || credentials.PasswordHash.String == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(credentials.PasswordHash.String), []byte(password)) == nil
}

func (s *Server) setSessionCookie(w http.ResponseWriter, r *http.Request, remember bool) {
	duration := 24 * time.Hour
	if remember {
		duration = 30 * 24 * time.Hour
	}
	expires := time.Now().Add(duration)
	payload := fmt.Sprintf("admin:%d", expires.Unix())
	mac := hmac.New(sha256.New, s.sessionKey)
	_, _ = mac.Write([]byte(payload))
	value := payload + ":" + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	http.SetCookie(w, &http.Cookie{Name: "zfs_s3end_session", Value: value, Path: "/", Expires: expires, MaxAge: int(duration.Seconds()), HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: s.requestIsHTTPS(r)})
}

func (s *Server) verifySessionCookie(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 3 || parts[0] != "admin" {
		return false
	}
	expiry, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().Unix() > expiry {
		return false
	}
	payload := parts[0] + ":" + parts[1]
	mac := hmac.New(sha256.New, s.sessionKey)
	_, _ = mac.Write([]byte(payload))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(parts[2]), []byte(want))
}

func (s *Server) csrfTokenForSession(sessionValue string) string {
	mac := hmac.New(sha256.New, s.sessionKey)
	_, _ = mac.Write([]byte("csrf:" + sessionValue))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Server) csrfToken(r *http.Request) string {
	cookie, err := r.Cookie("zfs_s3end_session")
	if err != nil || !s.verifySessionCookie(cookie.Value) {
		return ""
	}
	return s.csrfTokenForSession(cookie.Value)
}

func (s *Server) verifyCSRFToken(r *http.Request, sessionValue string) bool {
	token := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
	if token == "" {
		_ = r.ParseForm()
		token = strings.TrimSpace(r.FormValue("csrf_token"))
	}
	if token == "" {
		return false
	}
	want := s.csrfTokenForSession(sessionValue)
	return hmac.Equal([]byte(token), []byte(want))
}

func isUnsafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
}

func bearerToken(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(header) > 7 && strings.EqualFold(header[:7], "Bearer ") {
		return strings.TrimSpace(header[7:])
	}
	return ""
}

func (s *Server) verifyAPIToken(ctx context.Context, token string) bool {
	row, err := db.New(s.catalog.DB()).GetActiveAPITokenByHash(ctx, hashAPIToken(token))
	if err != nil {
		return false
	}
	_ = db.New(s.catalog.DB()).TouchAPIToken(ctx, row.ID)
	return true
}

func loginRateKey(r *http.Request) string {
	remote := r.RemoteAddr
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
		remote = strings.TrimSpace(strings.Split(forwarded, ",")[0])
	}
	return remote
}

func (s *Server) allowLoginAttempt(key string, now time.Time) bool {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	attempt := s.loginAttempts[key]
	return attempt.BlockedUntil.IsZero() || now.After(attempt.BlockedUntil)
}

func (s *Server) recordLoginFailure(key string, now time.Time) {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	attempt := s.loginAttempts[key]
	if now.Sub(attempt.Last) > 15*time.Minute {
		attempt.Failures = 0
	}
	attempt.Failures++
	attempt.Last = now
	if attempt.Failures >= 5 {
		attempt.BlockedUntil = now.Add(time.Duration(attempt.Failures-4) * time.Minute)
	}
	s.loginAttempts[key] = attempt
}

func (s *Server) recordLoginSuccess(key string) {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	delete(s.loginAttempts, key)
}

func isBrowserRequest(r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		return false
	}
	accept := r.Header.Get("Accept")
	return accept == "" || strings.Contains(accept, "text/html")
}

func (s *Server) requestIsHTTPS(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func randomSessionKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Errorf("generate session key: %w", err))
	}
	return key
}

func (s *Server) loginPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = loginTemplate.Execute(w, map[string]string{"Error": r.URL.Query().Get("error")})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key := loginRateKey(r)
	if !s.allowLoginAttempt(key, time.Now()) {
		http.Error(w, "too many login attempts", http.StatusTooManyRequests)
		return
	}
	if !s.verifyAdminPassword(r.Context(), r.FormValue("password")) {
		s.recordLoginFailure(key, time.Now())
		s.logger.Warn("admin login failed", "remote_addr", r.RemoteAddr)
		http.Redirect(w, r, "/login?error="+urlQueryEscape("invalid password"), http.StatusSeeOther)
		return
	}
	s.recordLoginSuccess(key)
	s.setSessionCookie(w, r, r.FormValue("remember") != "")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "zfs_s3end_session", Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: s.requestIsHTTPS(r)})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) setupPage(w http.ResponseWriter, r *http.Request) {
	configured, err := s.adminConfigured(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("check admin setup: %v", err), http.StatusInternalServerError)
		return
	}
	if configured {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = setupTemplate.Execute(w, map[string]string{"Error": r.URL.Query().Get("error")})
}

func (s *Server) setup(w http.ResponseWriter, r *http.Request) {
	configured, err := s.adminConfigured(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("check admin setup: %v", err), http.StatusInternalServerError)
		return
	}
	if configured {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	password := r.FormValue("password")
	confirm := r.FormValue("confirm_password")
	if len(password) < 12 {
		http.Redirect(w, r, "/setup?error="+urlQueryEscape("password must be at least 12 characters"), http.StatusSeeOther)
		return
	}
	if password != confirm {
		http.Redirect(w, r, "/setup?error="+urlQueryEscape("passwords do not match"), http.StatusSeeOther)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, fmt.Sprintf("hash password: %v", err), http.StatusInternalServerError)
		return
	}
	q := db.New(s.catalog.DB())
	if err := q.UpdateAdminPasswordHash(r.Context(), sql.NullString{String: string(hash), Valid: true}); err != nil {
		http.Error(w, fmt.Sprintf("store admin password: %v", err), http.StatusInternalServerError)
		return
	}
	s.setSessionCookie(w, r, false)
	http.Redirect(w, r, "/?message="+urlQueryEscape("Admin password saved. Authorize an SSH key, then send your first snapshot."), http.StatusSeeOther)
}
