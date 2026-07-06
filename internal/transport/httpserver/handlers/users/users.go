// Package users holds the HTTP handlers for the /v1/users surface:
// create, list, get, delete, and password/grant updates. All mutating
// routes require the admin action; reads are admin-only too, since a
// user list is sensitive. Writes are forwarded to the cluster leader
// (Raft), mirroring the topic write path.
package users

import (
	"encoding/json"
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// createUserRequest is the POST /v1/users body.
type createUserRequest struct {
	Username string       `json:"username"`
	Password string       `json:"password"`
	Grants   []user.Grant `json:"grants,omitempty"`
}

// updateGrantsRequest is the PUT /v1/users/{username}/grants body.
type updateGrantsRequest struct {
	Grants []user.Grant `json:"grants"`
}

// updatePasswordRequest is the PUT /v1/users/{username}/password body.
// CurrentPassword is required when a non-admin changes their own
// password; admins may reset any password without it.
type updatePasswordRequest struct {
	CurrentPassword string `json:"current_password,omitempty"`
	NewPassword     string `json:"new_password"`
}

// userResponse is the client-facing user shape — never includes the
// password hash.
type userResponse struct {
	Username    string       `json:"username"`
	Grants      []user.Grant `json:"grants,omitempty"`
	Root        bool         `json:"root,omitempty"`
	CreatedAtMs int64        `json:"created_at_ms,omitempty"`
	UpdatedAtMs int64        `json:"updated_at_ms,omitempty"`
}

func toResponse(u user.User) userResponse {
	return userResponse{
		Username:    u.Username,
		Grants:      u.Grants,
		Root:        u.Root,
		CreatedAtMs: u.CreatedAtMs,
		UpdatedAtMs: u.UpdatedAtMs,
	}
}

// Create handles POST /v1/users. Admin only; the new user's grants may
// not exceed the caller's own (no privilege escalation).
func Create(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := requireAdmin(s, w, r)
		if !ok {
			return
		}

		var req createUserRequest
		if !s.DecodeJSON(w, r, &req) {
			return
		}
		if err := user.ValidateUsername(req.Username); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.Password == "" {
			s.WriteError(w, http.StatusBadRequest, "password required")
			return
		}
		if err := user.ValidateGrants(req.Grants); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if !caller.CanDelegate(req.Grants) {
			s.WriteError(w, http.StatusForbidden, "cannot grant permissions you do not hold")
			return
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		if err != nil {
			s.WriteError(w, http.StatusInternalServerError, "hash password")
			return
		}
		now := time.Now().UnixMilli()
		u := user.User{
			Username:     req.Username,
			PasswordHash: hash,
			Grants:       req.Grants,
			CreatedAtMs:  now,
			UpdatedAtMs:  now,
		}
		body, err := json.Marshal(u)
		if err != nil {
			s.WriteError(w, http.StatusInternalServerError, "encode user")
			return
		}

		if s.Deps.Router != nil && s.Deps.Router.RouteCreateUser(r.Context(), w, r, body) {
			return
		}
		if err := s.Deps.Metastore.CreateUser(r.Context(), u); err != nil {
			s.WriteBrokerError(w, "create user", err)
			return
		}
		s.Audit(r, "user.create", req.Username)
		s.WriteJSON(w, http.StatusCreated, toResponse(u))
	}
}

// List handles GET /v1/users. Admin only.
func List(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := requireAdmin(s, w, r); !ok {
			return
		}
		users, err := s.Deps.Metastore.ListUsers(r.Context())
		if err != nil {
			s.WriteBrokerError(w, "list users", err)
			return
		}
		out := make([]userResponse, len(users))
		for i, u := range users {
			out[i] = toResponse(u)
		}
		s.WriteJSON(w, http.StatusOK, out)
	}
}

// Get handles GET /v1/users/{username}. Admin only.
func Get(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := requireAdmin(s, w, r); !ok {
			return
		}
		u, err := s.Deps.Metastore.GetUser(r.Context(), r.PathValue("username"))
		if err != nil {
			s.WriteBrokerError(w, "get user", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, toResponse(u))
	}
}

// Delete handles DELETE /v1/users/{username}. Admin only; the root
// account cannot be deleted.
func Delete(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := requireAdmin(s, w, r)
		if !ok {
			return
		}
		username := r.PathValue("username")
		target, err := s.Deps.Metastore.GetUser(r.Context(), username)
		if err != nil {
			s.WriteBrokerError(w, "delete user", err)
			return
		}
		if target.Root {
			s.WriteError(w, http.StatusForbidden, "the root admin cannot be deleted")
			return
		}
		if target.Username == caller.Username {
			s.WriteError(w, http.StatusForbidden, "cannot delete your own account")
			return
		}

		if s.Deps.Router != nil && s.Deps.Router.RouteDeleteUser(r.Context(), w, r, username) {
			return
		}
		if err := s.Deps.Metastore.DeleteUser(r.Context(), username); err != nil {
			s.WriteBrokerError(w, "delete user", err)
			return
		}
		s.Audit(r, "user.delete", username)
		w.WriteHeader(http.StatusNoContent)
	}
}
