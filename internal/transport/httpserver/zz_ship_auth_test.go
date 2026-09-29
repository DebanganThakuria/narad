package httpserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
)

// shipFailingUserStore is a user store whose reads fail as a closed or
// broken metastore's do.
type shipFailingUserStore struct{}

func (shipFailingUserStore) GetUser(context.Context, string) (user.User, error) {
	return user.User{}, errors.New("bolt: database not open")
}

func (shipFailingUserStore) UsersVersion() uint64 { return 1 }

// shipAuthServe runs one request with Basic credentials through h and
// reports whether the inner handler was reached.
func shipAuthServe(t *testing.T, h http.Handler, reached *bool, username, password string) *httptest.ResponseRecorder {
	t.Helper()
	*reached = false
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	req.SetBasicAuth(username, password)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	return res
}

// The middleware answers 429 once a username has used up its failed
// verifications (security.ErrThrottled, now read from AuthenticateBasic),
// before the handler, with the JSON error body every layer uses.
func TestShipAuthMiddlewareThrottledIs429(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	log := slog.New(slog.DiscardHandler)
	auth := security.New(staticUserStore{users: map[string]user.User{
		"alice": {Username: "alice", PasswordHash: hash},
	}}, log)
	reached := false
	h := Auth(auth, log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	// Each wrong password is a different one: a repeat is denied from the
	// negative cache without spending the throttle budget.
	var res *httptest.ResponseRecorder
	for i := 0; ; i++ {
		if i == 20 {
			t.Fatalf("20 distinct wrong passwords never throttled; last status %d", res.Code)
		}
		res = shipAuthServe(t, h, &reached, "alice", fmt.Sprintf("wrong-%d", i))
		if res.Code == http.StatusTooManyRequests {
			break
		}
		if res.Code != http.StatusUnauthorized {
			t.Fatalf("wrong password %d: status %d, want 401 until throttled", i, res.Code)
		}
	}
	if reached {
		t.Fatal("a throttled request reached the handler")
	}
	if got, want := res.Body.String(), `{"error":"too many failed authentication attempts"}`+"\n"; got != want {
		t.Fatalf("throttled body = %q, want %q", got, want)
	}
	if ct := res.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("throttled Content-Type = %q", ct)
	}
	if res.Header().Get("WWW-Authenticate") != "" {
		t.Fatal("a throttled response asked for credentials again")
	}
}

// A user store that cannot be read is the server's failure, not the
// caller's: 500 with a generic message, the store's error logged and
// never sent, and the handler not reached.
func TestShipAuthMiddlewareStoreFailureIs500(t *testing.T) {
	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, nil))
	auth := security.New(shipFailingUserStore{}, log)
	reached := false
	h := Auth(auth, log)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	res := shipAuthServe(t, h, &reached, "alice", "pw")
	if res.Code != http.StatusInternalServerError || reached {
		t.Fatalf("store failure: status %d, handler reached %v; want 500 before the handler", res.Code, reached)
	}
	if got, want := res.Body.String(), `{"error":"authentication unavailable"}`+"\n"; got != want {
		t.Fatalf("store failure body = %q, want %q", got, want)
	}
	if strings.Contains(res.Body.String(), "bolt") {
		t.Fatal("the store's error reached the client")
	}
	if out := logged.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "authentication store failure") ||
		!strings.Contains(out, "database not open") {
		t.Fatalf("log = %q, want an error record carrying the store's error", out)
	}
}
