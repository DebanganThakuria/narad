package cluster

import (
	"context"
	"net/http"
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
