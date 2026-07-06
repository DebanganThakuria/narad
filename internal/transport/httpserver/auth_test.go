package httpserver

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

type staticUserStore struct{ users map[string]user.User }

func (s staticUserStore) GetUser(_ context.Context, name string) (user.User, error) {
	u, ok := s.users[name]
	if !ok {
		return user.User{}, errs.ErrNotFound
	}
	return u, nil
}

func (s staticUserStore) UsersVersion() uint64 { return 1 }

func newAuthTestHandler(t *testing.T) http.Handler {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth := security.New(staticUserStore{users: map[string]user.User{
		"alice": {Username: "alice", PasswordHash: hash},
	}}, log)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, ok := security.IdentityFrom(r.Context()); ok {
			w.Header().Set("X-Identity", id.Username)
		}
		w.WriteHeader(http.StatusOK)
	})
	return Auth(auth, log)(inner)
}

func TestAuthMiddlewareRejectsMissingAndWrongCredentials(t *testing.T) {
	h := newAuthTestHandler(t)

	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/topics", nil))
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("no credentials: status = %d, want 401", res.Code)
	}
	if res.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("401 response missing WWW-Authenticate")
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	req.SetBasicAuth("alice", "nope")
	res = httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("wrong password: status = %d, want 401", res.Code)
	}
}

func TestAuthMiddlewareAcceptsValidCredentialsAndSetsIdentity(t *testing.T) {
	h := newAuthTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	req.SetBasicAuth("alice", "pw")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if got := res.Header().Get("X-Identity"); got != "alice" {
		t.Fatalf("identity = %q, want alice", got)
	}
}

func TestAuthMiddlewareExemptsProbesAndMetrics(t *testing.T) {
	h := newAuthTestHandler(t)
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		res := httptest.NewRecorder()
		h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if res.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200 without credentials", path, res.Code)
		}
	}
}

func TestAuthMiddlewareNilAuthenticatorDisablesAuth(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := Auth(nil, log)(inner)

	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/topics", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with auth disabled", res.Code)
	}
}
