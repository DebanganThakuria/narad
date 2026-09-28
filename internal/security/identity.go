package security

import (
	"context"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

type identityKey struct{}

// WithIdentity returns a context carrying the authenticated user. The
// stored copy has its PasswordHash blanked: authorization only needs the
// username, admin flag, and grants, and nothing downstream of the auth
// middleware should be able to reach the hash through a request context
// (handlers that need it re-read the store).
func WithIdentity(ctx context.Context, u user.User) context.Context {
	u.PasswordHash = nil
	return withIdentity(ctx, &u)
}

// withIdentity stores id itself, not a copy. The context holds a
// pointer so attaching the verification cache's identity boxes nothing;
// id must carry no PasswordHash and must never be modified, since every
// request of that user until the next users-domain version shares it.
func withIdentity(ctx context.Context, id *user.User) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom returns the authenticated user, if any. ok is false when
// the request was not authenticated (security disabled or exempt path).
// The result is a copy; its Grants are shared and must not be modified.
func IdentityFrom(ctx context.Context) (user.User, bool) {
	id, ok := ctx.Value(identityKey{}).(*user.User)
	if !ok || id == nil {
		return user.User{}, false
	}
	return *id, true
}
