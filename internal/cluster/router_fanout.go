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
// state; a remote owner's come with its own. Each partition's stats are
// kept only from the owner this node's assignments name for it
// (ownedCursorStats).
func (rt *Router) CollectFanoutCursors(ctx context.Context, parent string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool) {
	local = rt.fanout.OverlayRemoteCursorStats(parent, local)
	assignments, err := rt.store.ListAssignments(parent)
	if err != nil {
		return local, false
	}
	owned := map[int]bool{}
	byAddr := map[string]map[int]bool{}
	complete := true
	for _, assignment := range assignments {
		if assignment.OwnerID == rt.selfID {
			owned[assignment.Partition] = true
			continue
		}
		addr := rt.ownerAddr(parent, assignment.Partition)
		if addr == "" {
			complete = false
			continue
		}
		if byAddr[addr] == nil {
			byAddr[addr] = map[int]bool{}
		}
		byAddr[addr][assignment.Partition] = true
	}

	merged := local
	if rt.selfID != "" {
		merged = ownedCursorStats(local, owned)
	}
	for addr, partitions := range byAddr {
		stats, err := rt.peer.FanoutCursors(ctx, addr, parent)
		if err != nil {
			complete = false
			continue
		}
		merged = append(merged, ownedCursorStats(stats, partitions)...)
	}
	return merged, complete
}

// ownedCursorStats keeps the stats of the partitions in owned. A node
// reports every partition its own replica says it owns, so a node whose
// replica lags a move can report a partition it no longer owns and omit
// one it now owns; counting its report as is would let the duplicate
// stand in for the missing partition.
func ownedCursorStats(stats []topic.FanoutCursorStat, owned map[int]bool) []topic.FanoutCursorStat {
	out := make([]topic.FanoutCursorStat, 0, len(stats))
	for _, st := range stats {
		if owned[st.Partition] {
			out = append(out, st)
		}
	}
	return out
}

// collectOwnerCursorStats is CollectFanoutCursors for a caller without
// a route cache (the leader's unshipped check): owners resolved from the
// local replica's assignments and members, each remote one asked once,
// and each partition's stats kept only from the owner the assignments
// name (ownedCursorStats).
func collectOwnerCursorStats(ctx context.Context, store *metastore.Store, peer peerClient, selfID, parent string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool, error) {
	assignments, err := store.ListAssignments(parent)
	if err != nil {
		return nil, false, err
	}
	complete := true
	owned := map[int]bool{}
	byAddr := map[string]map[int]bool{}
	for _, a := range assignments {
		if selfID == "" || a.OwnerID == selfID {
			owned[a.Partition] = true
			continue
		}
		m, err := store.GetMember(a.OwnerID)
		if err != nil || m.Status == metastore.MemberDead || m.Addr == "" {
			complete = false
			continue
		}
		if byAddr[m.Addr] == nil {
			byAddr[m.Addr] = map[int]bool{}
		}
		byAddr[m.Addr][a.Partition] = true
	}
	merged := ownedCursorStats(local, owned)
	for addr, partitions := range byAddr {
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
		merged = append(merged, ownedCursorStats(stats, partitions)...)
	}
	return merged, complete, nil
}
