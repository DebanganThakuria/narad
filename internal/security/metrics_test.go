package security

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// holdVerifySlots takes every verification slot and returns the func
// that frees them.
func holdVerifySlots(t *testing.T, a *Authenticator) func() {
	t.Helper()
	for range maxConcurrentVerify {
		if err := a.verifyGate.acquire(context.Background(), false); err != nil {
			t.Fatal(err)
		}
	}
	return func() {
		for range maxConcurrentVerify {
			a.verifyGate.release()
		}
	}
}

// narad_auth_verify_queued counts an admitted verification until its
// bcrypt finishes, including one whose caller already left (the work
// still runs, detached from the caller).
func TestVerifyQueuedGaugeCountsAdmittedWorkUntilItFinishes(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "pw")})
	gauge := a.Collector()
	free := holdVerifySlots(t, a)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := a.Verify(ctx, "alice", "wrong")
		errc <- err
	}()
	waitUntil(t, "the verification to queue", func() bool { return testutil.ToFloat64(gauge) == 1 })
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller: %v, want context.Canceled", err)
	}
	if got := testutil.ToFloat64(gauge); got != 1 {
		t.Fatalf("narad_auth_verify_queued = %v after the caller left, want 1 (the work still runs)", got)
	}
	free()
	waitUntil(t, "the queue to drain", func() bool { return testutil.ToFloat64(gauge) == 0 })
}

// HashPassword and ComparePassword do not need a verification slot.
func TestPasswordWorkRunsWhileEveryVerifySlotIsHeld(t *testing.T) {
	a, _, _ := newTestAuthenticator(t)
	defer holdVerifySlots(t, a)()
	hash, err := a.HashPassword(context.Background(), "pw")
	if err != nil {
		t.Fatalf("HashPassword with every verification slot held: %v", err)
	}
	if err := a.ComparePassword(context.Background(), hash, "pw"); err != nil {
		t.Fatalf("ComparePassword with every verification slot held: %v", err)
	}
}
