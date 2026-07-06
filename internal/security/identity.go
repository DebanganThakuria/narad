package security

import (
	"context"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

type identityKey struct{}

// WithIdentity returns a context carrying the authenticated user.
func WithIdentity(ctx context.Context, u user.User) context.Context {
	return context.WithValue(ctx, identityKey{}, u)
}

// IdentityFrom returns the authenticated user, if any. ok is false when
// the request was not authenticated (security disabled or exempt path).
func IdentityFrom(ctx context.Context) (user.User, bool) {
	u, ok := ctx.Value(identityKey{}).(user.User)
	return u, ok
}
