package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// RouteDecommissionMember forwards a decommission (mark/clear draining) to
// the cluster leader. Like other metastore writes it must run on the leader;
// returns false when this node IS the leader (the handler then applies
// locally), true after forwarding, and writes a 503 when no leader is known.
func (rt *Router) RouteDecommissionMember(ctx context.Context, w http.ResponseWriter, _ *http.Request, id string, cancel bool) bool {
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return false
	}
	res, err := rt.peer.DecommissionMember(ctx, addr, id, cancel)
	return writeForwardResult(w, res, err)
}

// moveAborter is the peer call ForwardAbortMove uses; *PeerClient
// implements it.
type moveAborter interface {
	AbortMove(ctx context.Context, addr, topicName string, partition int, expectedTarget string) error
}

// ForwardAbortMove forwards an operator's move abort (clear the target
// iff it is still expectedTarget) to the cluster leader over the
// existing OpAbortMove, which 3.0.x leaders serve too. forwarded is false
// when this node is the leader, or no leader is known: the caller then
// applies it locally, where the metastore refuses it on a follower.
func (rt *Router) ForwardAbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) (forwarded bool, err error) {
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return false, nil
	}
	ab, ok := rt.peer.(moveAborter)
	if !ok {
		return false, nil
	}
	return true, ab.AbortMove(ctx, addr, topicName, partition, expectedTarget)
}

var _ moveAborter = (*PeerClient)(nil)

// LeaderAssignment reads the assignment of topicName/partition as the
// Raft leader has it: behind a Barrier from the local FSM when this node
// leads, else the leader's answer over the existing OpGetAssignment. A
// move abort answers from it, since the leader's compare-and-set changes
// nothing, and still succeeds, when the move flipped or was re-planned
// first. found is false for a partition with no assignment. A 3.0.x
// leader answers from its replica without marking a leader read; that
// replica has applied the abort it just served, so its answer is used.
func (rt *Router) LeaderAssignment(ctx context.Context, topicName string, partition int) (a metastore.Assignment, found bool, err error) {
	if rt.store.IsLeader() {
		if err := rt.store.Barrier(); err != nil {
			return metastore.Assignment{}, false, fmt.Errorf("barrier: %w", err)
		}
		a, err := rt.store.GetAssignment(topicName, partition)
		if errors.Is(err, errs.ErrNotFound) {
			return metastore.Assignment{}, false, nil
		}
		return a, err == nil, err
	}
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return metastore.Assignment{}, false, errors.New("no leader is known")
	}
	lr, ok := rt.peer.(leaderAssignmentReader)
	if !ok {
		return metastore.Assignment{}, false, errors.New("this node's peer client cannot read the leader's assignment")
	}
	a, found, _, err = lr.leaderAssignment(ctx, addr, topicName, partition)
	return a, found, err
}
