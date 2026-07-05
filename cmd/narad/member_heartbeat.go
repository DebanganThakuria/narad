package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// memberRegistrar forwards a member registration to another node, used
// when this node is not the metastore leader.
type memberRegistrar interface {
	RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
}

// runMemberHeartbeater re-registers this node's membership on every tick
// (and once immediately) so the controller keeps seeing it alive. It runs
// until ctx is cancelled; failures are logged at debug and retried on the
// next tick.
func runMemberHeartbeater(ctx context.Context, store *metastore.Store, member metastore.Member, interval time.Duration, registrar memberRegistrar, log *slog.Logger) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	send := func() {
		if err := registerMember(ctx, store, member, registrar); err != nil {
			log.Debug("member heartbeat failed", "member", member.ID, "err", err)
		}
	}

	send()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

// registerMember records the member in the local store; when that fails
// (this node is not the leader) it forwards the registration to the
// leader over peer RPC.
func registerMember(ctx context.Context, store *metastore.Store, member metastore.Member, registrar memberRegistrar) error {
	member.LastHeartbeat = time.Now().Unix()
	if err := store.RegisterMember(ctx, member); err == nil {
		return nil
	}

	leaderAddr := leaderMemberAddr(store)
	if leaderAddr == "" {
		return fmt.Errorf("register member: leader member address unavailable")
	}
	if registrar == nil {
		return fmt.Errorf("register member: peer registrar unavailable")
	}
	res, err := registrar.RegisterMember(ctx, leaderAddr, nodewire.MemberRequest{
		ID:            member.ID,
		Addr:          member.Addr,
		ClusterAddr:   member.ClusterAddr,
		Status:        string(member.Status),
		LastHeartbeat: member.LastHeartbeat,
	})
	if err != nil {
		return err
	}
	if res.Status < 200 || res.Status >= 300 {
		return fmt.Errorf("register member returned status %d", res.Status)
	}
	return nil
}

// leaderMemberAddr resolves the leader's member (HTTP/RPC) address. The
// store only knows the leader's raft cluster address, so the member entry
// is found either by leader ID or by matching cluster addresses.
func leaderMemberAddr(store *metastore.Store) string {
	leaderClusterAddr := store.LeaderAddr()
	if strings.TrimSpace(leaderClusterAddr) == "" {
		return ""
	}
	if member, err := store.GetMember(store.LeaderID()); err == nil && member.Status != metastore.MemberDead {
		return strings.TrimSpace(member.Addr)
	}
	members, err := store.ListMembers()
	if err != nil {
		return ""
	}
	for _, member := range members {
		if member.Status == metastore.MemberDead {
			continue
		}
		if memberMatchesLeaderClusterAddr(member, leaderClusterAddr) {
			return strings.TrimSpace(member.Addr)
		}
	}
	return ""
}

func memberMatchesLeaderClusterAddr(member metastore.Member, leaderClusterAddr string) bool {
	if strings.TrimSpace(member.ClusterAddr) != "" {
		return netaddr.ClusterAddrMatchesPeer(leaderClusterAddr, member.ClusterAddr)
	}
	return netaddr.ClusterAddrMatchesPeer(leaderClusterAddr, member.Addr)
}
