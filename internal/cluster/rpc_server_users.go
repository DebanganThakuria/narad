package cluster

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// User writes are metastore-level (not broker), so these handlers call
// the store directly. They only ever run on the leader — followers
// forward here via the router — and the store's Raft apply enforces
// that: a stray forward to a non-leader surfaces as a 503.

func (s *RPCServer) handleCreateUser(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeUserRequest(payload, nodewire.OpCreateUser)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid create user request: "+err.Error())
	}
	var u user.User
	if err := decodeStrictJSON(req.Body, &u); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid json: "+err.Error())
	}
	if err := s.store.CreateUser(rpcRequestContext(), u); err != nil {
		return userError(err)
	}
	return jsonResponse(http.StatusCreated, redactUser(u))
}

// handleUpdateUser applies a forwarded user update. The body is a
// metastore.UserUpdate: field-scoped (password or grants) from current
// ingress nodes, or a bare user.User (Field empty) from an older build,
// which still applies as a whole-record replace. The response carries
// the record as committed, read back from the leader's own replica.
func (s *RPCServer) handleUpdateUser(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeUserRequest(payload, nodewire.OpUpdateUser)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid update user request: "+err.Error())
	}
	var upd metastore.UserUpdate
	if err := decodeStrictJSON(req.Body, &upd); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid json: "+err.Error())
	}
	if upd.Username == "" {
		upd.Username = req.Username
	}
	ctx := rpcRequestContext()
	if err := s.store.ApplyUserUpdate(ctx, upd); err != nil {
		return userError(err)
	}
	committed, err := s.store.GetUser(ctx, upd.Username)
	if err != nil {
		return userError(err)
	}
	return jsonResponse(http.StatusOK, redactUser(committed))
}

func (s *RPCServer) handleDeleteUser(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeUserRequest(payload, nodewire.OpDeleteUser)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid delete user request: "+err.Error())
	}
	if err := s.store.DeleteUser(rpcRequestContext(), req.Username); err != nil {
		return userError(err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

// userError maps a metastore user write failure onto an HTTP status.
func userError(err error) nodewire.Response {
	switch {
	case errors.Is(err, errs.ErrAlreadyExists):
		return errorResponse(http.StatusConflict, "user already exists")
	case errors.Is(err, errs.ErrNotFound):
		return errorResponse(http.StatusNotFound, "user not found")
	case errors.Is(err, metastore.ErrRootProtected):
		return errorResponse(http.StatusForbidden, "the root account is protected")
	case errors.Is(err, errs.ErrInvalidArgument):
		return errorResponse(http.StatusBadRequest, err.Error())
	default:
		return errorResponse(http.StatusServiceUnavailable, "user write failed: "+err.Error())
	}
}

// redactUser clears the password hash before a user record crosses any
// boundary back to a client.
func redactUser(u user.User) user.User {
	u.PasswordHash = nil
	return u
}
