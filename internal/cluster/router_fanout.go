package cluster

// Fan-out routing: attach/detach are Raft metadata writes, forwarded
// to the cluster leader exactly like topic create/alter/delete; cursor
// stats are scattered across the parent partitions' owners and merged
// here for the list-children API.

import (
	"context"
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// RouteAttachChild forwards a fan-out attach to the cluster leader.
func (rt *Router) RouteAttachChild(ctx context.Context, w http.ResponseWriter, _ *http.Request, parent, child string, delayMs int64) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.AttachChild(ctx, memberAddr, parent, child, delayMs)
	if err != nil {
		writeLeaderForwardError(w, err)
		return true
	}
	writePeerResponse(w, res)
	return true
}

// RouteDetachChild forwards a fan-out detach to the cluster leader.
func (rt *Router) RouteDetachChild(ctx context.Context, w http.ResponseWriter, _ *http.Request, parent, child string) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.DetachChild(ctx, memberAddr, parent, child)
	if err != nil {
		writeLeaderForwardError(w, err)
		return true
	}
	writePeerResponse(w, res)
	return true
}

// CollectFanoutCursors merges the fan-out cursor stats of every parent
// partition owner: local stats are passed in by the caller, remote
// owners are queried once each. Unreachable owners are skipped rather
// than failing the listing — the caller reports the lag as incomplete
// (ok=false) instead.
func (rt *Router) CollectFanoutCursors(ctx context.Context, parent string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool) {
	assignments, err := rt.store.ListAssignments(parent)
	if err != nil {
		return local, false
	}
	remoteAddrs := map[string]struct{}{}
	complete := true
	for _, assignment := range assignments {
		if assignment.OwnerID == rt.selfID {
			continue
		}
		addr := rt.ownerAddr(parent, assignment.Partition)
		if addr == "" {
			complete = false
			continue
		}
		remoteAddrs[addr] = struct{}{}
	}

	merged := local
	for addr := range remoteAddrs {
		stats, err := rt.peer.FanoutCursors(ctx, addr, parent)
		if err != nil {
			complete = false
			continue
		}
		merged = append(merged, stats...)
	}
	return merged, complete
}
