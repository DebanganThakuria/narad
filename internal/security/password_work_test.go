package security

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// HashPassword and ComparePassword must wait for a bcrypt slot like
// Verify does: with every slot taken they block until one frees or the
// context ends, so a loop of password changes cannot pin every core.
func TestHashAndCompareRunUnderBcryptBound(t *testing.T) {
	a := New(newFakeStore(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	for range maxConcurrentVerify {
		a.verifySem <- struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := a.HashPassword(ctx, "pw"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("HashPassword with all slots taken: err = %v, want deadline exceeded", err)
	}
	if err := a.ComparePassword(ctx, []byte("$2a$04$x"), "pw"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ComparePassword with all slots taken: err = %v, want deadline exceeded", err)
	}

	<-a.verifySem // free one slot
	hash, err := a.HashPassword(context.Background(), "pw")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if bcrypt.CompareHashAndPassword(hash, []byte("pw")) != nil {
		t.Fatal("HashPassword produced a hash that does not verify")
	}
	if err := a.ComparePassword(context.Background(), hash, "pw"); err != nil {
		t.Fatalf("ComparePassword(match): %v", err)
	}
	if err := a.ComparePassword(context.Background(), hash, "nope"); err == nil {
		t.Fatal("ComparePassword(mismatch) = nil, want error")
	}
	// The slot was released after each call.
	select {
	case a.verifySem <- struct{}{}:
	default:
		t.Fatal("bcrypt slot leaked by HashPassword/ComparePassword")
	}
}
