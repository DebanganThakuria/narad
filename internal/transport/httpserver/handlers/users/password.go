package users

import (
	"context"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// bcrypt work goes through Deps.Passwords when wired (the authenticator,
// which bounds process-wide bcrypt concurrency) and inline otherwise
// (security disabled, tests). Callers validate the password length
// first: bcrypt rejects inputs over 72 bytes, and that must be the
// client's 400, never a hashing failure logged as a server error.

func bcryptHash(s *handlers.Set, ctx context.Context, password string) ([]byte, error) {
	if s.Deps.Passwords != nil {
		return s.Deps.Passwords.HashPassword(ctx, password)
	}
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// bcryptCompare returns nil when password matches hash.
func bcryptCompare(s *handlers.Set, ctx context.Context, hash []byte, password string) error {
	if s.Deps.Passwords != nil {
		return s.Deps.Passwords.ComparePassword(ctx, hash, password)
	}
	return bcrypt.CompareHashAndPassword(hash, []byte(password))
}
