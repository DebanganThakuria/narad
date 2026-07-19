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
