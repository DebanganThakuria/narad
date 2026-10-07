package handlers

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
)

// RequireSecuredAdmin is RequireAdmin without the no-identity bypass
// (authz.go:32-36): no identity answers 403 "remotes require security",
// a non-admin 403 "admin privileges required". Remotes lend outbound
// credentials, so a node whose API has no authorization must not let
// anyone manage them.
func (s *Set) RequireSecuredAdmin(w http.ResponseWriter, r *http.Request) (user.User, bool) {
	caller, ok := security.IdentityFrom(r.Context())
	if !ok {
		s.WriteError(w, http.StatusForbidden, "remotes require security")
		return user.User{}, false
	}
	if !caller.IsAdmin() {
		s.WriteError(w, http.StatusForbidden, "admin privileges required")
		return user.User{}, false
	}
	return caller, true
}
