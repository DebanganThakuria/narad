package security

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// A request that reads a changed password record must not join a
// bcrypt call still checking the old hash: it would accept the old
// password and cache it as verified for the new hash.
func TestVerifyOldPasswordAfterChangeDoesNotJoinStaleFlight(t *testing.T) {
	a, store, _ := newTestAuthenticator(t)
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "old")})
	free := wp1bFillBcryptSlots(a)

	first := make(chan error, 1)
	go func() {
		_, err := a.Verify(context.Background(), "alice", "old")
		first <- err
	}()
	wp1bWaitGets(t, store, 1)

	// The password changes while the first call waits for a bcrypt slot.
	store.put(user.User{Username: "alice", PasswordHash: testHash(t, "new")})

	second := make(chan error, 1)
	go func() {
		_, err := a.Verify(context.Background(), "alice", "old")
		second <- err
	}()
	wp1bWaitGets(t, store, 2)
	free()

	if err := <-first; err != nil {
		t.Fatalf("Verify(old) started before the change = %v, want nil", err)
	}
	if err := <-second; !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Verify(old) started after the change = %v, want ErrUnauthorized", err)
	}
	if _, err := a.Verify(context.Background(), "alice", "old"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("later Verify(old) = %v, want ErrUnauthorized", err)
	}
	if _, err := a.Verify(context.Background(), "alice", "new"); err != nil {
		t.Fatalf("Verify(new) = %v, want nil", err)
	}
}
