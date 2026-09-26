package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

func wp1bBasic(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

// wp1bNetHTTPBasicAuth is the reference parse: what net/http's
// Request.BasicAuth makes of an Authorization header value.
func wp1bNetHTTPBasicAuth(header string) (username, password string, ok bool) {
	r := &http.Request{Header: http.Header{}}
	if header != "" {
		r.Header.Set("Authorization", header)
	}
	return r.BasicAuth()
}

var wp1bHeaderSeeds = []string{
	"",
	"Basic",
	"Basic ",
	wp1bBasic("alice", "pw"),
	"basic " + base64.StdEncoding.EncodeToString([]byte("alice:pw")),
	"BASIC " + base64.StdEncoding.EncodeToString([]byte("alice:pw")),
	"Bearer " + base64.StdEncoding.EncodeToString([]byte("alice:pw")),
	"Basic  " + base64.StdEncoding.EncodeToString([]byte("alice:pw")),
	"Basic !!!!",
	"Basic " + base64.StdEncoding.EncodeToString([]byte("no-colon")),
	"Basic " + base64.RawStdEncoding.EncodeToString([]byte("alice:p")),
	wp1bBasic("", ""),
	wp1bBasic("alice", "pa:ss:word"),
	wp1bBasic("ünï", "çødé"),
	wp1bBasic("alice", strings.Repeat("p", 72)),
	wp1bBasic(strings.Repeat("u", 64), strings.Repeat("p", 72)),
	wp1bBasic("alice", strings.Repeat("p", 300)), // past the stack buffer
	"Basic YWxp\r\nY2U6cHc=",
}

// TestWP1BParseBasicAuthMatchesNetHTTP pins parseBasicAuth to
// Request.BasicAuth: the middleware used to call the latter, and a
// header either accepts must mean the same credentials to both.
func TestWP1BParseBasicAuthMatchesNetHTTP(t *testing.T) {
	for _, h := range wp1bHeaderSeeds {
		wp1bCheckParse(t, h)
	}
}

func FuzzWP1BParseBasicAuth(f *testing.F) {
	for _, h := range wp1bHeaderSeeds {
		f.Add(h)
	}
	f.Fuzz(wp1bCheckParse)
}

func wp1bCheckParse(t *testing.T, header string) {
	wantU, wantP, wantOK := wp1bNetHTTPBasicAuth(header)
	var buf [basicAuthStackBytes]byte
	gotU, gotP, gotOK := parseBasicAuth(header, &buf)
	if gotOK != wantOK || string(gotU) != wantU || string(gotP) != wantP {
		t.Fatalf("parseBasicAuth(%q) = %q, %q, %v; net/http says %q, %q, %v",
			header, gotU, gotP, gotOK, wantU, wantP, wantOK)
	}
}

// TestWP1BCredTokenSameForBytes: the cache key must not depend on
// whether the password came from Verify (string) or AuthenticateBasic
// (bytes), or one form would miss the other's cache entries.
func TestWP1BCredTokenSameForBytes(t *testing.T) {
	a, _, _ := newTestAuthenticator(t)
	for _, pw := range []string{"", "s3cret", strings.Repeat("x", 72)} {
		mac := hmac.New(sha256.New, a.credKey)
		mac.Write([]byte(pw))
		var want [32]byte
		copy(want[:], mac.Sum(nil))
		if got := credToken(a, []byte(pw)); got != want {
			t.Fatalf("credToken([]byte(%q)) = %x, want %x", pw, got, want)
		}
	}
}

func wp1bAuthenticate(t *testing.T, a *Authenticator, username, password string) (user.User, error) {
	t.Helper()
	ctx, err := a.AuthenticateBasic(context.Background(), wp1bBasic(username, password))
	if err != nil {
		return user.User{}, err
	}
	id, ok := IdentityFrom(ctx)
	if !ok {
		t.Fatal("AuthenticateBasic succeeded without attaching an identity")
	}
	return id, nil
}

// TestWP1BAuthenticateBasicIdentity covers the identity the middleware
// attaches: no password hash, the user's grants, a missing or malformed
// header rejected like a wrong password, and cache hits that neither
// read the store nor run bcrypt.
func TestWP1BAuthenticateBasicIdentity(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{
		Username: "alice", PasswordHash: testHash(t, "pw"),
		Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}},
	})

	id, err := wp1bAuthenticate(t, a, "alice", "pw")
	if err != nil {
		t.Fatalf("AuthenticateBasic: %v", err)
	}
	if id.Username != "alice" || !id.Allowed(user.ActionProduce, "orders-eu") {
		t.Fatalf("identity lost fields authorization needs: %+v", id)
	}
	if id.PasswordHash != nil {
		t.Fatal("identity carries the password hash")
	}

	before := store.gets.Load()
	if _, err := wp1bAuthenticate(t, a, "alice", "pw"); err != nil {
		t.Fatalf("cached AuthenticateBasic: %v", err)
	}
	if got := store.gets.Load(); got != before {
		t.Fatalf("cache hit read the store: gets %d -> %d", before, got)
	}

	// Verify and AuthenticateBasic share one cache: a string-form verify
	// is a cache hit after a bytes-form one.
	rec, err := a.Verify(context.Background(), "alice", "pw")
	if err != nil || store.gets.Load() != before {
		t.Fatalf("Verify after AuthenticateBasic: err %v, gets %d -> %d", err, before, store.gets.Load())
	}
	if rec.PasswordHash != nil {
		t.Fatal("Verify returned the password hash")
	}

	for _, header := range []string{"", "Basic", "Bearer abc", "Basic !!!!", "Basic " + base64.StdEncoding.EncodeToString([]byte("alice"))} {
		if _, err := a.AuthenticateBasic(context.Background(), header); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("header %q: err = %v, want ErrUnauthorized", header, err)
		}
	}
	if _, err := wp1bAuthenticate(t, a, "alice", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong password: err = %v, want ErrUnauthorized", err)
	}
	if _, err := wp1bAuthenticate(t, a, "ghost", "pw"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown user: err = %v, want ErrUnauthorized", err)
	}
}

// TestWP1BSharedIdentityFollowsUsersVersion: the fast path hands every
// request the same cached identity, so a users-domain change must
// still take effect on the very next request, and an identity already
// attached to an in-flight request must never change under it.
func TestWP1BSharedIdentityFollowsUsersVersion(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	hash := testHash(t, "pw")
	store.put(user.User{Username: "alice", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}}})

	first, err := a.AuthenticateBasic(context.Background(), wp1bBasic("alice", "pw"))
	if err != nil {
		t.Fatalf("AuthenticateBasic: %v", err)
	}

	// Grants change: the next request sees them without re-running
	// bcrypt, and the earlier request's identity is untouched.
	store.put(user.User{Username: "alice", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"orders-*"}}}})
	id, err := wp1bAuthenticate(t, a, "alice", "pw")
	if err != nil {
		t.Fatalf("after grant change: %v", err)
	}
	if id.Allowed(user.ActionProduce, "orders-eu") || !id.Allowed(user.ActionConsume, "orders-eu") {
		t.Fatalf("identity after grant change has stale grants: %+v", id.Grants)
	}
	old, _ := IdentityFrom(first)
	if !old.Allowed(user.ActionProduce, "orders-eu") || old.Allowed(user.ActionConsume, "orders-eu") {
		t.Fatalf("in-flight identity changed under its request: %+v", old.Grants)
	}

	// Password change: the old password stops working immediately.
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "new")})
	if _, err := wp1bAuthenticate(t, a, "alice", "pw"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old password after change: err = %v, want ErrUnauthorized", err)
	}
	if _, err := wp1bAuthenticate(t, a, "alice", "new"); err != nil {
		t.Fatalf("new password: %v", err)
	}

	// Deletion: access ends on the next request.
	store.delete("alice")
	if _, err := wp1bAuthenticate(t, a, "alice", "new"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("deleted user: err = %v, want ErrUnauthorized", err)
	}
}

// TestWP1BAuthenticateBasicCacheHitAllocs: a cache hit allocates only
// the context node that carries the identity.
func TestWP1BAuthenticateBasicCacheHitAllocs(t *testing.T) {
	if wp1bRaceEnabled {
		t.Skip("the race detector adds allocations")
	}
	// Longer than 32 bytes, so a string conversion could not hide in a
	// stack temporary.
	const password = "a-password-longer-than-thirty-two-bytes"
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, password)})
	header := wp1bBasic("alice", password)
	if _, err := a.AuthenticateBasic(context.Background(), header); err != nil {
		t.Fatalf("AuthenticateBasic: %v", err)
	}
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := a.AuthenticateBasic(context.Background(), header); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("AuthenticateBasic cache hit allocates %.0f times, want 1", allocs)
	}
}
