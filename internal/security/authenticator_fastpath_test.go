package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// TestCredTokenMatchesFreshHMAC pins the pooled comparator to the
// reference computation: the cache keys must not depend on which
// pooled state served the call, or on whether the state was reused.
func TestCredTokenMatchesFreshHMAC(t *testing.T) {
	a, _, _ := newTestAuthenticator(t)

	for _, pw := range []string{"", "s3cret", "a much longer passphrase with spaces", "s3cret"} {
		mac := hmac.New(sha256.New, a.credKey)
		mac.Write([]byte(pw))
		var want [32]byte
		copy(want[:], mac.Sum(nil))

		for i := range 3 {
			if got := a.credToken(pw); got != want {
				t.Fatalf("credToken(%q) call %d = %x, want %x", pw, i, got, want)
			}
		}
	}
}

// TestRepeatedWrongPasswordSkipsStoreRead asserts the negative cache is
// served from the read-locked fast path: a second identical wrong
// password for a cached user must not touch the store at all.
func TestRepeatedWrongPasswordSkipsStoreRead(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "right")})

	if _, err := a.Verify(context.Background(), "alice", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("first wrong password: err = %v, want ErrUnauthorized", err)
	}
	before := store.gets.Load()
	for i := range 3 {
		if _, err := a.Verify(context.Background(), "alice", "wrong"); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("repeat %d: err = %v, want ErrUnauthorized", i, err)
		}
	}
	if got := store.gets.Load(); got != before {
		t.Fatalf("negative cache hit read the store: gets %d -> %d", before, got)
	}
}

// TestNegativeCacheExpiresAfterTTL guards the fast path against serving
// a stale rejection forever: past negativeTTL the slow path re-checks.
func TestNegativeCacheExpiresAfterTTL(t *testing.T) {
	a, store, now := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "right")})

	if _, err := a.Verify(context.Background(), "alice", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	*now = now.Add(negativeTTL + time.Second)
	before := store.gets.Load()
	if _, err := a.Verify(context.Background(), "alice", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	if got := store.gets.Load(); got == before {
		t.Fatal("expired negative entry was served from cache without re-reading the store")
	}
}

// countingHandler counts slog records by message so tests can assert
// on log volume without parsing output.
type countingHandler struct {
	counts map[string]*atomic.Int64
}

func newCountingHandler(msgs ...string) *countingHandler {
	h := &countingHandler{counts: make(map[string]*atomic.Int64, len(msgs))}
	for _, m := range msgs {
		h.counts[m] = new(atomic.Int64)
	}
	return h
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	if c, ok := h.counts[r.Message]; ok {
		c.Add(1)
	}
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

// TestThrottleLogRateLimited drains a user's bucket, then hammers it:
// the "authentication throttled" audit line must appear once per refill
// interval, not once per rejected request.
func TestThrottleLogRateLimited(t *testing.T) {
	store := newFakeStore()
	handler := newCountingHandler("authentication throttled")
	a := New(store, slog.New(handler))
	now := time.Unix(1_700_000_000, 0)
	a.now = func() time.Time { return now }
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "right")})

	for i := range bucketCapacity {
		pw := string(rune('a' + i))
		if _, err := a.Verify(context.Background(), "alice", pw); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("drain %d: err = %v", i, err)
		}
	}
	for i := range 20 {
		pw := "burst" + string(rune('a'+i))
		if _, err := a.Verify(context.Background(), "alice", pw); !errors.Is(err, ErrThrottled) {
			t.Fatalf("burst %d: err = %v, want ErrThrottled", i, err)
		}
	}
	if got := handler.counts["authentication throttled"].Load(); got != 1 {
		t.Fatalf("throttled log lines within one interval = %d, want 1", got)
	}

	// Advance past the refill interval: the first attempt spends the
	// one earned token (a plain 401), the rest are throttled and must
	// log exactly once more.
	now = now.Add(bucketRefillEvery + time.Second)
	for i := range 5 {
		pw := "again" + string(rune('a'+i))
		_, _ = a.Verify(context.Background(), "alice", pw)
	}
	if got := handler.counts["authentication throttled"].Load(); got != 2 {
		t.Fatalf("throttled log lines after refill = %d, want 2", got)
	}
}

func BenchmarkCredToken(b *testing.B) {
	a := New(newFakeStore(), slog.New(slog.DiscardHandler))
	b.ReportAllocs()
	for b.Loop() {
		a.credToken("correct horse battery staple")
	}
}

func BenchmarkVerifyCacheHit(b *testing.B) {
	store := newFakeStore()
	a := New(store, slog.New(slog.DiscardHandler))
	hash, err := testHashB("pw")
	if err != nil {
		b.Fatal(err)
	}
	store.put(user.User{Username: "alice", PasswordHash: hash})
	if _, err := a.Verify(context.Background(), "alice", "pw"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := a.Verify(context.Background(), "alice", "pw"); err != nil {
			b.Fatal(err)
		}
	}
}
