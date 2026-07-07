package security

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

func TestNegCacheClearedOnPwChangeToCachedValue(t *testing.T) {
	store := newFakeStore()
	a := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1_700_000_000, 0)
	a.now = func() time.Time { return now }

	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "old")})
	if _, err := a.Verify(context.Background(), "alice", "old"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Verify(context.Background(), "alice", "future"); !errors.Is(err, ErrUnauthorized) {
		t.Fatal(err)
	}
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "future")})
	if _, err := a.Verify(context.Background(), "alice", "future"); err != nil {
		t.Fatalf("BUG: new password (was negatively cached) rejected: %v", err)
	}
}

func TestFastPathStaleAcrossHashSwapSameVersion(t *testing.T) {
	store := newFakeStore()
	a := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1_700_000_000, 0)
	a.now = func() time.Time { return now }
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "old")})
	if _, err := a.Verify(context.Background(), "alice", "old"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.users["alice"] = user.User{Username: "alice", PasswordHash: testHash(t, "new")}
	store.mu.Unlock()
	_, err := a.Verify(context.Background(), "alice", "old")
	t.Logf("old pw after silent hash swap (same version): err=%v accepted=%v", err, err == nil)
}

// Legit user lockout: attacker drains bucket with distinct wrong passwords,
// each cached negative. Does the legit user's CORRECT password stay locked
// out beyond one refill interval? And can attacker keep it drained forever
// at 1 attempt / <12s?
func TestLockoutSustainability(t *testing.T) {
	store := newFakeStore()
	a := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	now := time.Unix(1_700_000_000, 0)
	a.now = func() time.Time { return now }
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "right")})

	for i := range bucketCapacity {
		pw := "wrong" + string(rune('a'+i))
		a.Verify(context.Background(), "alice", pw)
	}
	// Attacker sends one fresh wrong pw every 12s indefinitely.
	locked := 0
	for round := range 20 {
		now = now.Add(bucketRefillEvery)
		pw := "atk" + string(rune('a'+round))
		if _, err := a.Verify(context.Background(), "alice", pw); errors.Is(err, ErrThrottled) {
			// attacker itself throttled
		}
		// legit user tries right now:
		if _, err := a.Verify(context.Background(), "alice", "right"); errors.Is(err, ErrThrottled) {
			locked++
		}
	}
	t.Logf("legit user throttled in %d/20 rounds under sustained 1-per-12s attack", locked)
}
