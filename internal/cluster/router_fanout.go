package cluster

// Fan-out routing: attach/detach are Raft metadata writes, forwarded
// to the cluster leader exactly like topic create/alter/delete; cursor
// stats are scattered across the parent partitions' owners and merged
// here for the list-children API.

import (
	"context"
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// SetFanoutRunner gives the router the node's fan-out runner. Call
// before serving.
func (rt *Router) SetFanoutRunner(r *FanoutRunner) { rt.fanout = r }

// RouteAttachChild forwards a fan-out attach to the cluster leader.
func (rt *Router) RouteAttachChild(ctx context.Context, w http.ResponseWriter, _ *http.Request, parent, child string, delayMs int64) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.AttachChild(ctx, memberAddr, parent, child, delayMs, forwardActor(ctx))
	return rt.writeForwardedWrite(ctx, w, memberAddr, res, err)
}

// RouteDetachChild forwards a fan-out detach to the cluster leader.
func (rt *Router) RouteDetachChild(ctx context.Context, w http.ResponseWriter, _ *http.Request, parent, child string) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.DetachChild(ctx, memberAddr, parent, child, forwardActor(ctx))
	return rt.writeForwardedWrite(ctx, w, memberAddr, res, err)
}

// CollectFanoutCursors merges the fan-out cursor stats of every parent
// partition owner: local stats are passed in by the caller, remote
// owners are queried once each. Unreachable owners are skipped rather
// than failing the listing: the caller reports the lag as incomplete
// (ok=false) instead. The local stats gain this node's remote cursor
// state; a remote owner's come with its own.
func (rt *Router) CollectFanoutCursors(ctx context.Context, parent string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool) {
	local = rt.fanout.OverlayRemoteCursorStats(parent, local)
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

// collectOwnerCursorStats is CollectFanoutCursors for a caller without
// a route cache (the leader's unshipped check): owners resolved from the
// local replica's assignments and members, each remote one asked once.
func collectOwnerCursorStats(ctx context.Context, store *metastore.Store, peer peerClient, selfID, parent string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool, error) {
	assignments, err := store.ListAssignments(parent)
	if err != nil {
		return nil, false, err
	}
	complete := true
	addrs := map[string]struct{}{}
	for _, a := range assignments {
		if selfID == "" || a.OwnerID == selfID {
			continue
		}
		m, err := store.GetMember(a.OwnerID)
		if err != nil || m.Status == metastore.MemberDead || m.Addr == "" {
			complete = false
			continue
		}
		addrs[m.Addr] = struct{}{}
	}
	merged := local
	for addr := range addrs {
		if peer == nil {
			complete = false
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, defaultPeerRPCTimeout)
		stats, err := peer.FanoutCursors(rpcCtx, addr, parent)
		cancel()
		if err != nil {
			complete = false
			continue
		}
		merged = append(merged, stats...)
	}
	return merged, complete, nil
}
