package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// abortForwarder is the optional router call that sends a move abort to
// the leader (cluster.Router implements it). It is not part of
// handlers.Router, so routers that predate it keep working.
type abortForwarder interface {
	ForwardAbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) (forwarded bool, err error)
}

// abortTimeout bounds the abort write. It runs detached from the client
// request, so a client that hangs up cannot leave an applied abort
// unaudited.
const abortTimeout = 10 * time.Second

// AbortView is the 202 body of a move abort.
type AbortView struct {
	Move MoveView `json:"move"`
	Note string   `json:"note"`
}

// AbortMove handles POST /v1/cluster/moves/{topic}/{partition}/abort:
// clear an in-flight move's target, so the partition stays with its
// owner. Admin only, audited as cluster.move.abort. ?target= names the
// destination the operator means; a move that now targets another node
// is refused with 409 rather than aborted. The abort is the leader's
// compare-and-set on the current target, so a move re-planned in between
// is never cleared by mistake. The controller may plan the partition
// again on a later pass.
func AbortMove(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		topicName := r.PathValue("topic")
		partition, err := strconv.Atoi(r.PathValue("partition"))
		if topicName == "" || err != nil || partition < 0 {
			s.WriteError(w, http.StatusBadRequest, "topic and a partition number are required")
			return
		}
		a, err := s.Deps.Metastore.GetAssignment(topicName, partition)
		if errors.Is(err, errs.ErrNotFound) {
			s.WriteError(w, http.StatusNotFound, fmt.Sprintf("%s/%d has no assignment", topicName, partition))
			return
		}
		if err != nil {
			s.WriteBrokerError(w, "get assignment", err)
			return
		}
		if a.TargetID == "" {
			s.WriteError(w, http.StatusConflict, fmt.Sprintf("no move is in flight for %s/%d", topicName, partition))
			return
		}
		if want := r.URL.Query().Get("target"); want != "" && want != a.TargetID {
			s.WriteError(w, http.StatusConflict, fmt.Sprintf("the move of %s/%d now targets %s, not %s; nothing was aborted", topicName, partition, a.TargetID, want))
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), abortTimeout)
		defer cancel()
		forwarded := false
		if fw, ok := s.Deps.Router.(abortForwarder); ok {
			forwarded, err = fw.ForwardAbortMove(ctx, topicName, partition, a.TargetID)
		}
		if !forwarded {
			err = s.Deps.Metastore.AbortMove(ctx, topicName, partition, a.TargetID)
		}
		if err != nil {
			if forwarded {
				s.WriteError(w, http.StatusServiceUnavailable, "abort move on the leader: "+err.Error())
				return
			}
			s.WriteBrokerError(w, "abort move", err)
			return
		}
		s.Audit(r, "cluster.move.abort", fmt.Sprintf("%s/%d %s->%s", topicName, partition, a.OwnerID, a.TargetID))
		members, _ := s.Deps.Metastore.ListMembers()
		byID := make(map[string]metastore.Member, len(members))
		for _, m := range members {
			byID[m.ID] = m
		}
		s.WriteJSON(w, http.StatusAccepted, AbortView{
			Move: moveView(a, byID),
			Note: "the move's target was cleared (unless the move was re-planned meanwhile); the partition stays with its owner, and the controller may plan a move for it again",
		})
	}
}
