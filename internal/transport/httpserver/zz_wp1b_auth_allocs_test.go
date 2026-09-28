package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
)

func wp1bHash(t *testing.T, password string) []byte {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	return h
}

// TestWP1BAuthMiddlewareAllocs bounds what the auth middleware adds to
// an authenticated request on a warm cache: the context node carrying
// the identity and the request copy that carries the context. Parsing
// the Authorization header and attaching the identity must not
// allocate on their own (they used to cost three more: the base64
// decode, the credential string and the boxed user copy).
func TestWP1BAuthMiddlewareAllocs(t *testing.T) {
	if wp1bRaceEnabled {
		t.Skip("the race detector adds allocations")
	}
	auth := security.New(staticUserStore{users: map[string]user.User{
		"alice": {Username: "alice", PasswordHash: wp1bHash(t, "pw")},
	}}, newTestLogger())

	var seen string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, ok := security.IdentityFrom(r.Context()); ok {
			seen = id.Username
		}
	})
	h := Auth(auth, newTestLogger())(inner)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
	req.SetBasicAuth("alice", "pw")
	rw := &wp1bDiscardRW{h: http.Header{}}

	h.ServeHTTP(rw, req) // warm the verification cache (runs bcrypt once)
	if seen != "alice" {
		t.Fatalf("identity = %q, want alice (status %d)", seen, rw.status)
	}
	allocs := testing.AllocsPerRun(200, func() {
		h.ServeHTTP(rw, req)
	})
	if allocs > 2 {
		t.Fatalf("auth middleware allocates %.0f times per authenticated request, want at most 2", allocs)
	}
}
