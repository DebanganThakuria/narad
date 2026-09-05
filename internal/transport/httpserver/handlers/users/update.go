package users

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Updates are field-scoped: a grants update proposes only the new
// grants, a password update only the new hash. The handler reads the
// target from the LOCAL replica for its checks (existence, root guards,
// the current-password proof), and that replica may lag the leader by a
// few applies. If the whole record were sent through Raft, a password
// change issued on a lagging follower would carry the stale grants along
// and re-apply a revocation the leader had already committed. The FSM
// applies each op against the record as it currently stands, so the
// fields not being changed are never touched (see metastore.UserUpdate).

// UpdateGrants handles PUT /v1/users/{username}/grants. Admin only; the
// new grants may not exceed the caller's own, the root account's grants
// are immutable, and a caller cannot edit their own grants (no
// self-escalation).
func UpdateGrants(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := s.RequireAdmin(w, r)
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

		upd := metastore.UserUpdate{
			Field: metastore.UserUpdateGrants,
			User:  user.User{Username: username, Grants: req.Grants, UpdatedAtMs: time.Now().UnixMilli()},
		}
		committed, ok := applyUserUpdate(s, w, r, upd, "user.grants")
		if !ok {
			return
		}
		s.WriteJSON(w, http.StatusOK, toResponse(committed))
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
			// as full access (dev mode), consistent with the other
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

		// Only root may change the root password. Without this, any other
		// admin could reset it, log in as root, and inherit root's
		// exclusive powers (granting the admin action, root immutability),
		// defeating the whole point of reserving those to root. Matches the
		// root guards on Delete and UpdateGrants. Skipped when security is
		// disabled (ok == false, dev mode).
		if ok && target.Root && caller.Username != target.Username {
			s.WriteError(w, http.StatusForbidden, "only the root admin may change the root password")
			return
		}

		// Self-service (non-admin) must prove the current password.
		if selfService && !caller.IsAdmin() {
			if bcryptCompare(target.PasswordHash, req.CurrentPassword) != nil {
				s.WriteError(w, http.StatusForbidden, "current password is incorrect")
				return
			}
		}

		hash, err := bcryptHash(req.NewPassword)
		if err != nil {
			s.WriteError(w, http.StatusInternalServerError, "hash password")
			return
		}
		upd := metastore.UserUpdate{
			Field: metastore.UserUpdatePassword,
			User:  user.User{Username: username, PasswordHash: hash, UpdatedAtMs: time.Now().UnixMilli()},
		}
		if _, ok := applyUserUpdate(s, w, r, upd, "user.password"); !ok {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// applyUserUpdate forwards the update to the leader (or applies it
// locally on the leader), audits it, and returns the record as
// committed. It returns ok=false when the response has already been
// written: either by the forward (success or failure) or by the error
// path here.
func applyUserUpdate(s *handlers.Set, w http.ResponseWriter, r *http.Request, upd metastore.UserUpdate, event string) (user.User, bool) {
	body, err := json.Marshal(upd)
	if err != nil {
		s.WriteError(w, http.StatusInternalServerError, "encode user update")
		return user.User{}, false
	}
	if s.Deps.Router != nil && s.Deps.Router.RouteUpdateUser(r.Context(), w, r, upd.Username, body) {
		return user.User{}, false // response already written by the forward
	}
	if err := s.Deps.Metastore.ApplyUserUpdate(r.Context(), upd); err != nil {
		s.WriteBrokerError(w, "update user", err)
		return user.User{}, false
	}
	s.Audit(r, event, upd.Username)
	// The leader's replica reflects its own apply by the time Raft
	// returns, so this read is the committed record.
	committed, err := s.Deps.Metastore.GetUser(r.Context(), upd.Username)
	if err != nil {
		s.WriteBrokerError(w, "update user", err)
		return user.User{}, false
	}
	return committed, true
}
