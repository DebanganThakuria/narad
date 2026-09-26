package security

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

func wp1bBenchAuthenticator(b *testing.B) *Authenticator {
	b.Helper()
	store := newFakeStore()
	a := New(store, slog.New(slog.DiscardHandler))
	hash, err := testHashB("pw")
	if err != nil {
		b.Fatal(err)
	}
	store.put(user.User{Username: "alice", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders*"}}}})
	if _, err := a.Verify(context.Background(), "alice", "pw"); err != nil {
		b.Fatal(err)
	}
	return a
}

// BenchmarkWP1BAuthChain replays the middleware's per-request identity
// steps on a cache hit: Verify, WithIdentity, WithContext, IdentityFrom.
func BenchmarkWP1BAuthChain(b *testing.B) {
	a := wp1bBenchAuthenticator(b)
	r := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
	b.ReportAllocs()
	for b.Loop() {
		rec, err := a.Verify(r.Context(), "alice", "pw")
		if err != nil {
			b.Fatal(err)
		}
		authed := r.WithContext(WithIdentity(r.Context(), rec))
		if _, ok := IdentityFrom(authed.Context()); !ok {
			b.Fatal("no identity")
		}
	}
}

// BenchmarkWP1BVerifyParallel is the cache-hit fast path under
// concurrency.
func BenchmarkWP1BVerifyParallel(b *testing.B) {
	a := wp1bBenchAuthenticator(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := a.Verify(context.Background(), "alice", "pw"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWP1BAuthenticateBasic is the middleware's per-request auth
// on a cache hit: header parse, verify, identity attach.
func BenchmarkWP1BAuthenticateBasic(b *testing.B) {
	a := wp1bBenchAuthenticator(b)
	r := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
	r.SetBasicAuth("alice", "pw")
	header := r.Header.Get("Authorization")
	b.ReportAllocs()
	for b.Loop() {
		ctx, err := a.AuthenticateBasic(r.Context(), header)
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := IdentityFrom(ctx); !ok {
			b.Fatal("no identity")
		}
	}
}
