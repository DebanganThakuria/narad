package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// memberRegistrar forwards a member registration to another node, used
// when this node is not the metastore leader.
type memberRegistrar interface {
	RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
}

// memberRegisterRetryInterval is how soon the heartbeater tries again
// while this process has not yet registered even once.
const memberRegisterRetryInterval = 250 * time.Millisecond

// errMemberRemoved reports that the leader refused the registration
// because decommission removed this member. Retrying cannot succeed.
var errMemberRemoved = errors.New("register member: member was removed from the cluster")

// localMember is the member record this node registers and refreshes
// with every heartbeat. It reports this binary's build and the newest
// Raft entry type it applies: the leader proposes a newer entry type
// only once every member reports one that knows it.
func localMember(id, addr, clusterAddr string) metastore.Member {
	return metastore.Member{
		ID:          id,
		Addr:        addr,
		ClusterAddr: clusterAddr,
		Status:      metastore.MemberAlive,
		Build:       versionString(),
		EntryTypes:  metastore.MaxEntryType,
	}
}

// runMemberHeartbeater re-registers this node's membership every interval
// (and once immediately) so the controller keeps seeing it alive. It runs
// until ctx is cancelled; failures are logged at debug and retried.
//
// Until the first registration succeeds it retries every
// memberRegisterRetryInterval instead of every interval. On a fresh or
// fully restarted cluster the first attempt runs before any leader
// exists and fails; at the full interval the member table then stayed
// empty for several seconds after /readyz went green, and a topic
// created in that window got no partition owners until the
// controller's next reconcile tick. A follower also needs the leader's
// own member record before it can forward, so a slow cadence cost it
// one more interval on top.
func runMemberHeartbeater(ctx context.Context, store *metastore.Store, member metastore.Member, interval time.Duration, registrar memberRegistrar, log *slog.Logger) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	heartbeatLoop(ctx, interval, min(memberRegisterRetryInterval, interval), func() error {
		err := registerMember(ctx, store, member, registrar)
		if err != nil {
			log.Debug("member heartbeat failed", "member", member.ID, "err", err)
		}
		return err
	})
}

// heartbeatLoop calls send at once and then every interval until ctx
// is cancelled. Until send first succeeds, a failure is retried after
// retry instead. errMemberRemoved is the exception: a removed member can
// never register again, and each attempt costs the leader a Raft entry
// that is only refused, so it keeps the full interval.
func heartbeatLoop(ctx context.Context, interval, retry time.Duration, send func() error) {
	registered := false
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		next := interval
		if err := send(); err == nil {
			registered = true
		} else if !registered && !errors.Is(err, errMemberRemoved) {
			next = retry
		}
		timer.Reset(next)
	}
}

// registerMember records the member in the local store; when that fails
// (this node is not the leader) it forwards the registration to the
// leader over peer RPC. The heartbeat timestamp is only meaningful on
// the leader's clock: the local write happens only when this node IS
// the leader, and a forwarded registration is re-stamped by the leader
// (handleRegisterMember), which ignores the value sent here. It is still
// sent for leaders that predate the re-stamping.
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
	return forwardMember(ctx, registrar, leaderAddr, member)
}

// forwardMember sends the registration to the leader at leaderAddr. A
// leader on 3.0.x refuses the frame carrying the build and entry types
// (a trailing-field refusal); the registration is sent again at once
// without them, so a rolling upgrade never costs a heartbeat window and
// the member is never marked dead for it. Nothing is remembered: every
// heartbeat to an older leader costs one refused frame, which never
// reaches Raft.
func forwardMember(ctx context.Context, registrar memberRegistrar, leaderAddr string, member metastore.Member) error {
	req := nodewire.MemberRequest{
		ID:            member.ID,
		Addr:          member.Addr,
		ClusterAddr:   member.ClusterAddr,
		Status:        string(member.Status),
		LastHeartbeat: member.LastHeartbeat,
		Build:         member.Build,
		EntryTypes:    member.EntryTypes,
	}
	res, err := registrar.RegisterMember(ctx, leaderAddr, req)
	if err == nil && (req.Build != "" || req.EntryTypes != 0) && cluster.IsTrailingFieldRefusal(res) {
		req.Build, req.EntryTypes = "", 0
		res, err = registrar.RegisterMember(ctx, leaderAddr, req)
	}
	if err != nil {
		return err
	}
	if res.Status == http.StatusGone {
		return errMemberRemoved
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
