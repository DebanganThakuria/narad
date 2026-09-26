package security

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// wp1bFillBcryptSlots occupies every bcrypt slot, as other users'
// verifications would, and returns the func that frees them.
func wp1bFillBcryptSlots(a *Authenticator) (free func()) {
	for range maxConcurrentVerify {
		a.verifySem <- struct{}{}
	}
	return func() {
		for range maxConcurrentVerify {
			<-a.verifySem
		}
	}
}

// wp1bWaitGets waits until the store has served n reads, which each
// Verify does just before it joins the shared bcrypt call.
func wp1bWaitGets(t *testing.T, store *fakeStore, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for store.gets.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("store reads = %d, want %d", store.gets.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
	// Leave the callers time to get from the store read into the call.
	time.Sleep(50 * time.Millisecond)
}

// TestWP1BSingleflightLeaderCancelDoesNotFailFollowers: the shared
// bcrypt call used to wait for a slot under the first caller's context,
// so when that client disconnected every follower sharing the call got
// context.Canceled (a 499) although their own clients were still there.
func TestWP1BSingleflightLeaderCancelDoesNotFailFollowers(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "pw")})
	free := wp1bFillBcryptSlots(a)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := a.Verify(leaderCtx, "alice", "pw")
		leaderErr <- err
	}()
	wp1bWaitGets(t, store, 1)

	const followers = 20
	followerErr := make(chan error, followers)
	for range followers {
		go func() {
			_, err := a.Verify(context.Background(), "alice", "pw")
			followerErr <- err
		}()
	}
	wp1bWaitGets(t, store, 1+followers)

	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader err = %v, want context.Canceled", err)
	}
	free()
	for range followers {
		if err := <-followerErr; err != nil {
			t.Fatalf("follower with a live context got %v, want success", err)
		}
	}
}

// TestWP1BSingleflightCallerCancelReturnsPromptly: a caller whose own
// client goes away stops waiting at once, even while the shared call
// it joined is still queued for a bcrypt slot, and the call carries on
// for the callers still waiting.
func TestWP1BSingleflightCallerCancelReturnsPromptly(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "pw")})
	free := wp1bFillBcryptSlots(a)
	defer func() {
		if free != nil {
			free()
		}
	}()

	leaderErr := make(chan error, 1)
	go func() {
		_, err := a.Verify(context.Background(), "alice", "pw")
		leaderErr <- err
	}()
	wp1bWaitGets(t, store, 1)

	ctx, cancel := context.WithCancel(context.Background())
	followerErr := make(chan error, 1)
	go func() {
		_, err := a.Verify(ctx, "alice", "pw")
		followerErr <- err
	}()
	wp1bWaitGets(t, store, 2)

	cancel()
	select {
	case err := <-followerErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled follower err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled follower still waiting on the shared bcrypt call")
	}

	free()
	free = nil
	if err := <-leaderErr; err != nil {
		t.Fatalf("leader err = %v, want success", err)
	}
}
