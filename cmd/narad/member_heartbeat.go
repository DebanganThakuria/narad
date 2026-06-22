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

type memberRegistrar interface {
	RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
}

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
