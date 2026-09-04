package httpserver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
)

// TestAuthIdentityOmitsPasswordHash asserts the identity handlers read
// from the request context carries no password hash: nothing past the
// auth middleware needs it, so it must not be reachable there.
func TestAuthIdentityOmitsPasswordHash(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth := security.New(staticUserStore{users: map[string]user.User{
		"alice": {
			Username: "alice", PasswordHash: hash,
			Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}},
		},
	}}, log)

	var seen user.User
	var found bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, found = security.IdentityFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := Auth(auth, log)(inner)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	req.SetBasicAuth("alice", "pw")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if !found {
		t.Fatal("identity missing from request context")
	}
	if seen.PasswordHash != nil {
		t.Fatal("identity in request context still carries the password hash")
	}
	if seen.Username != "alice" || !seen.Allowed(user.ActionProduce, "orders-eu") {
		t.Fatalf("identity lost fields needed for authorization: %+v", seen)
	}

	// The authenticator's own cached record must be untouched: a second
	// verify still succeeds from cache with the hash intact.
	if _, err := auth.Verify(req.Context(), "alice", "pw"); err != nil {
		t.Fatalf("cached Verify after identity copy: %v", err)
	}
}
