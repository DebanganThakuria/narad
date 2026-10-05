package cluster

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// forgetResult is the body of a successful forget, on the leader and as
// relayed by a follower.
type forgetResult struct {
	ID    string `json:"id"`
	Voter bool   `json:"voter"`
}

// handleForgetServer runs on the leader (followers forward here): it
// removes a Raft server that has no member record (see
// metastore.Store.ForgetServer). The leader's own log line audits the
// removal; the node the admin called audits the request.
func (s *RPCServer) handleForgetServer(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeForgetServerRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid forget request: "+err.Error())
	}
	voter, err := s.store.ForgetServer(rpcRequestContext(), req.ID)
	if err != nil {
		return errorResponse(forgetErrorStatus(err), err.Error())
	}
	return jsonResponse(http.StatusOK, forgetResult{ID: req.ID, Voter: voter})
}

// forgetErrorStatus maps a ForgetServer refusal onto its status.
func forgetErrorStatus(err error) int {
	switch {
	case errors.Is(err, errs.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, metastore.ErrMemberRecordExists),
		errors.Is(err, metastore.ErrServerNamedByAssignment),
		errors.Is(err, metastore.ErrQuorumAtRisk):
		return http.StatusConflict
	case errors.Is(err, errs.ErrInvalidArgument):
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}
