package users

import (
	"encoding/json"
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// UpdateGrants handles PUT /v1/users/{username}/grants. Admin only; the
// new grants may not exceed the caller's own, the root account's grants
// are immutable, and a caller cannot edit their own grants (no
// self-escalation).
func UpdateGrants(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := requireAdmin(s, w, r)
		if !ok {
			return
		}
		username := r.PathValue("username")

		var req updateGrantsRequest
		if !s.DecodeJSON(w, r, &req) {
			return
		}
		if err := user.ValidateGrants(req.Grants); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if username == caller.Username {
			s.WriteError(w, http.StatusForbidden, "cannot modify your own grants")
			return
		}
		if !caller.CanDelegate(req.Grants) {
			s.WriteError(w, http.StatusForbidden, "cannot grant permissions you do not hold")
			return
		}

		target, err := s.Deps.Metastore.GetUser(r.Context(), username)
		if err != nil {
			s.WriteBrokerError(w, "update grants", err)
			return
		}
		if target.Root {
			s.WriteError(w, http.StatusForbidden, "the root admin's grants are immutable")
			return
		}

		target.Grants = req.Grants
		target.UpdatedAtMs = time.Now().UnixMilli()
		if !applyUserUpdate(s, w, r, target, "user.grants") {
			return
		}
		s.WriteJSON(w, http.StatusOK, toResponse(target))
	}
}

// UpdatePassword handles PUT /v1/users/{username}/password. A user may
// change their own password by supplying current_password; an admin may
// reset anyone's password without it. Grants are untouched.
func UpdatePassword(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := handlers.Identity(r)
		selfService := ok && caller.Username == r.PathValue("username")
		// Either the caller is an admin, or they are changing their own
		// password. Anything else is forbidden.
		if !selfService && !(ok && caller.IsAdmin()) {
			// When security is disabled there is no identity; treat that
			// as full access (dev mode) — consistent with the other
			// handlers' authorization posture.
			if ok {
				s.WriteError(w, http.StatusForbidden, "admin required")
				return
			}
		}
		username := r.PathValue("username")

		var req updatePasswordRequest
		if !s.DecodeJSON(w, r, &req) {
			return
		}
		if req.NewPassword == "" {
			s.WriteError(w, http.StatusBadRequest, "new_password required")
			return
		}

		target, err := s.Deps.Metastore.GetUser(r.Context(), username)
		if err != nil {
			s.WriteBrokerError(w, "update password", err)
			return
		}

		// Self-service (non-admin) must prove the current password.
		if selfService && !caller.IsAdmin() {
			if bcrypt.CompareHashAndPassword(target.PasswordHash, []byte(req.CurrentPassword)) != nil {
				s.WriteError(w, http.StatusForbidden, "current password is incorrect")
				return
			}
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
		if err != nil {
			s.WriteError(w, http.StatusInternalServerError, "hash password")
			return
		}
		target.PasswordHash = hash
		target.UpdatedAtMs = time.Now().UnixMilli()
		if !applyUserUpdate(s, w, r, target, "user.password") {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// applyUserUpdate forwards the update to the leader (or applies it
// locally on the leader) and audits it. It returns false and has
// already written an error response when the update failed.
func applyUserUpdate(s *handlers.Set, w http.ResponseWriter, r *http.Request, u user.User, event string) bool {
	body, err := json.Marshal(u)
	if err != nil {
		s.WriteError(w, http.StatusInternalServerError, "encode user")
		return false
	}
	if s.Deps.Router != nil && s.Deps.Router.RouteUpdateUser(r.Context(), w, r, u.Username, body) {
		return false // response already written by the forward
	}
	if err := s.Deps.Metastore.UpdateUser(r.Context(), u); err != nil {
		s.WriteBrokerError(w, "update user", err)
		return false
	}
	s.Audit(r, event, u.Username)
	return true
}
