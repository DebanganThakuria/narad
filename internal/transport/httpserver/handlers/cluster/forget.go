package cluster

import (
	"context"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// forgetRouter is the forwarding half of Forget, which the cluster
// router implements. It is asserted rather than added to
// handlers.Router, so routers that predate forget, and test fakes, need
// not implement it: without it the handler runs the forget locally,
// which only a leader can do.
type forgetRouter interface {
	RouteForgetServer(ctx context.Context, w http.ResponseWriter, r *http.Request, id string) bool
}

// forgetResponse is the 200 body of a forget.
type forgetResponse struct {
	ID    string `json:"id"`
	Voter bool   `json:"voter"`
}

// Forget handles POST /v1/cluster/members/{id}/forget: remove a Raft
// server, voter or non-voter, that has no member record, such as a
// joiner admitted by a 3.0.x leader that never registered. Admin only.
// It refuses a server with a member record (decommission it instead)
// and one a partition assignment names, so it never touches data, and a
// voter whose removal could leave the cluster without a quorum. The
// write runs on the leader; followers forward it.
func Forget(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		id := r.PathValue("id")
		if id == "" {
			s.WriteError(w, http.StatusBadRequest, "member id required")
			return
		}
		aw := handlers.NewAuditWriter(w)
		w = aw
		defer aw.Audit(s, r, "cluster.forget", id)

		if fr, ok := s.Deps.Router.(forgetRouter); ok && fr.RouteForgetServer(r.Context(), w, r, id) {
			return // forwarded to the leader; response already written
		}
		voter, err := s.Deps.Metastore.ForgetServer(r.Context(), id)
		switch {
		case errors.Is(err, metastore.ErrMemberRecordExists),
			errors.Is(err, metastore.ErrServerNamedByAssignment),
			errors.Is(err, metastore.ErrQuorumAtRisk):
			s.WriteError(w, http.StatusConflict, err.Error())
		case err != nil:
			s.WriteBrokerError(w, "forget", err)
		default:
			s.WriteJSON(w, http.StatusOK, forgetResponse{ID: id, Voter: voter})
		}
	}
}
