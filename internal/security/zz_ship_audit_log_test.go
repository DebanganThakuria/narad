package security

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// A failed authentication is audited with the stored account's name.
// The log never carries the presented password, and an unknown user
// (whatever was typed as the name) is never logged at all.
func TestAuthenticationFailureAuditNamesTheStoredAccountOnly(t *testing.T) {
	var logs bytes.Buffer
	store := newFakeStore()
	a := New(store, slog.New(slog.NewTextHandler(&logs, nil)))
	store.put(user.User{Username: "orders-service", PasswordHash: testHash(t, "right-secret")})

	if _, err := a.Verify(context.Background(), "orders-service", "wrong-secret-XYZ"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Verify(wrong password) = %v, want ErrUnauthorized", err)
	}
	if _, err := a.Verify(context.Background(), "typed-a-secret-QRS", "anything"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Verify(unknown user) = %v, want ErrUnauthorized", err)
	}

	out := logs.String()
	if !strings.Contains(out, `msg="authentication failed"`) || !strings.Contains(out, "username=orders-service") {
		t.Fatalf("audit log = %q, want an authentication failed line naming orders-service", out)
	}
	for _, secret := range []string{"wrong-secret-XYZ", "right-secret", "typed-a-secret-QRS"} {
		if strings.Contains(out, secret) {
			t.Fatalf("audit log carries %q: %q", secret, out)
		}
	}
}
