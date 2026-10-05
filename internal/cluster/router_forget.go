package cluster

import (
	"context"
	"net/http"
)

// forgetUnsupportedByLeader is the 501 a follower answers when the
// leader's release predates forget.
const forgetUnsupportedByLeader = "the leader runs a release that cannot forget a Raft server; upgrade it first"

// RouteForgetServer forwards a forget (remove a Raft server with no
// member record) to the cluster leader. Like other metastore writes it
// must run on the leader: it returns false when this node IS the leader
// (the handler then runs it locally), and true after forwarding. A
// leader that cannot be reached is a 503, and one whose release does not
// know the operation is a 501.
func (rt *Router) RouteForgetServer(ctx context.Context, w http.ResponseWriter, _ *http.Request, id string) bool {
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return false
	}
	res, err := rt.peer.ForgetServer(ctx, addr, id)
	if err == nil && isUnsupportedOp(res) {
		writePeerResponse(w, errorResponse(http.StatusNotImplemented, forgetUnsupportedByLeader))
		return true
	}
	return writeForwardResult(w, res, err)
}
