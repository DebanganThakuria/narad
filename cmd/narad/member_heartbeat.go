package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

const memberRegisterPath = "/internal/v1/members"

func runMemberHeartbeater(ctx context.Context, store *metastore.Store, member metastore.Member, interval time.Duration, client *http.Client, log *slog.Logger) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if client == nil {
		client = http.DefaultClient
	}

	send := func() {
		if err := registerMember(ctx, store, member, client); err != nil {
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

func registerMember(ctx context.Context, store *metastore.Store, member metastore.Member, client *http.Client) error {
	member.LastHeartbeat = time.Now().Unix()
	if err := store.RegisterMember(ctx, member); err == nil {
		return nil
	}

	leaderAddr := leaderMemberHTTPAddr(store)
	if leaderAddr == "" {
		return fmt.Errorf("register member: leader HTTP address unavailable")
	}
	return postMemberRegistration(ctx, client, leaderAddr, member)
}

func leaderMemberHTTPAddr(store *metastore.Store) string {
	leaderClusterAddr := store.LeaderAddr()
	if strings.TrimSpace(leaderClusterAddr) == "" {
		return ""
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

func postMemberRegistration(ctx context.Context, client *http.Client, addr string, member metastore.Member) error {
	body, err := json.Marshal(member)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, memberRegisterEndpoint(addr), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("register member returned status %d", resp.StatusCode)
	}
	return nil
}

func memberRegisterEndpoint(addr string) string {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr + memberRegisterPath
	}
	return "http://" + addr + memberRegisterPath
}
