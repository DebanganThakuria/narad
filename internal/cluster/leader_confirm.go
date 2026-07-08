package cluster

// Leader confirmation for the produce dispatcher's discard path. The
// dispatcher discards a WAL record — 202-acked, durable, undelivered —
// when its topic is deleted. Local absence used to be the whole signal,
// justified by "replicas only move forward"; a replica restored from an
// old Raft snapshot breaks exactly that argument: every topic created
// after the snapshot point reads as absent, and discarding on that view
// destroys accepted records. Discard therefore additionally requires the
// LEADER to confirm the topic is gone; every failure mode keeps the
// record for the next pass to retry.

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// leaderView is the slice of the metastore needed to locate the current
// leader for confirmation RPCs.
type leaderView interface {
	LeaderID() string
	GetMember(id string) (metastore.Member, error)
}

// topicFetcher fetches a topic record from a peer; peerClient satisfies it.
type topicFetcher interface {
	GetTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error)
}

func topicAbsentOnLeader(ctx context.Context, view leaderView, peer topicFetcher, selfID, topicName string, log *slog.Logger) bool {
	if selfID == "" {
		return true // no cluster identity: the local store is the authority
	}
	leaderID := view.LeaderID()
	if leaderID == "" {
		log.Warn("dispatcher: no leader known; keeping records for absent topic", "topic", topicName)
		return false
	}
	if leaderID == selfID {
		return true // leader state is quorum-supported
	}
	member, err := view.GetMember(leaderID)
	if err != nil || member.Addr == "" {
		log.Warn("dispatcher: leader member unresolvable; keeping records for absent topic",
			"leader", leaderID, "topic", topicName, "err", err)
		return false
	}
	rpcCtx, cancel := context.WithTimeout(ctx, fanoutEpochConfirmTimeout)
	defer cancel()
	res, err := peer.GetTopic(rpcCtx, member.Addr, topicName)
	if err != nil {
		log.Warn("dispatcher: leader absence check unreachable; keeping records for absent topic",
			"leader", leaderID, "topic", topicName, "err", err)
		return false
	}
	return res.Status == http.StatusNotFound
}
