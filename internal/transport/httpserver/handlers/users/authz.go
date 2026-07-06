package users

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// requireAdmin returns the calling user when they hold the admin action,
// writing a 403 otherwise. When security is disabled there is no
// identity on the request; that is treated as full access (dev mode),
// and a synthetic root user is returned so no-escalation checks pass.
func requireAdmin(s *handlers.Set, w http.ResponseWriter, r *http.Request) (user.User, bool) {
	caller, ok := handlers.Identity(r)
	if !ok {
		return user.User{Root: true}, true
	}
	if caller.IsAdmin() {
		return caller, true
	}
	s.WriteError(w, http.StatusForbidden, "admin privileges required")
	return user.User{}, false
}
