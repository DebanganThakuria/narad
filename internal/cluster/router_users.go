package cluster

import (
	"context"
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// User writes go through Raft, so like topic writes they must execute on
// the leader. Each Route* returns false when this node is the leader (no
// member address for "self"), letting the HTTP handler apply the write to
// the local metastore; otherwise it forwards to the leader and returns
// true. A 503 is returned when no leader is currently known.

// RouteCreateUser forwards a user create to the cluster leader.
func (rt *Router) RouteCreateUser(ctx context.Context, w http.ResponseWriter, _ *http.Request, body []byte) bool {
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return false
	}
	res, err := rt.peer.CreateUser(ctx, addr, body)
	return writeForwardResult(w, res, err)
}

// RouteUpdateUser forwards a user update to the cluster leader.
func (rt *Router) RouteUpdateUser(ctx context.Context, w http.ResponseWriter, _ *http.Request, username string, body []byte) bool {
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return false
	}
	res, err := rt.peer.UpdateUser(ctx, addr, username, body)
	return writeForwardResult(w, res, err)
}

// RouteDeleteUser forwards a user delete to the cluster leader.
func (rt *Router) RouteDeleteUser(ctx context.Context, w http.ResponseWriter, _ *http.Request, username string) bool {
	addr := rt.leaderMemberAddr()
	if addr == "" {
		return false
	}
	res, err := rt.peer.DeleteUser(ctx, addr, username)
	return writeForwardResult(w, res, err)
}

// writeForwardResult renders a forwarded peer response, mapping a
// transport error to 502.
func writeForwardResult(w http.ResponseWriter, res nodewire.Response, err error) bool {
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}
