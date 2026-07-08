package cluster

// Leader confirmation for destructive decisions. A stale local replica
// must never authorize destroying data: it can be restored from an old
// Raft snapshot and read as current. Confirmation therefore comes from
// the LEADER — and when this node IS the leader, from its own state only
// AFTER a Raft barrier: election guarantees a fresh leader's LOG is
// complete, not that its FSM has applied it, so a just-elected leader
// restored from an old snapshot legally serves stale reads until the
// replay finishes. (Observed live: a force-killed node rejoined, won the
// election, and tail-anchored fan-out cursors off its still-replaying
// FSM — skipping the delay backlog — because "self is leader" was
// treated as authority without a barrier.) Every failure mode returns
// not-confirmed; callers defer and retry.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const leaderConfirmRPCTimeout = 5 * time.Second

// leaderView is the slice of the metastore needed to obtain the leader's
// view of a topic; *metastore.Store satisfies it.
type leaderView interface {
	LeaderID() string
	GetMember(id string) (metastore.Member, error)
	Barrier() error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
}

// topicFetcher fetches a topic record from a peer; peerClient satisfies it.
type topicFetcher interface {
	GetTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error)
}

// leaderTopicView resolves the LEADER's record of topicName.
// ok=false: the leader's view could not be obtained (no leader, member
// unresolvable, unreachable, barrier failed, undecodable) — treat as
// unconfirmed. absent=true (with ok): the leader authoritatively says the
// topic does not exist. Otherwise t is the leader's record.
func leaderTopicView(ctx context.Context, view leaderView, peer topicFetcher, selfID, topicName string, log *slog.Logger) (t topic.Topic, absent, ok bool) {
	localView := func() (topic.Topic, bool, bool) {
		rec, err := view.GetTopic(ctx, topicName)
		switch {
		case err == nil:
			return rec, false, true
		case errors.Is(err, errs.ErrNotFound):
			return topic.Topic{}, true, true
		default:
			log.Warn("leader confirm: local topic read failed", "topic", topicName, "err", err)
			return topic.Topic{}, false, false
		}
	}
	if selfID == "" {
		// No cluster identity (single process): the local store is the
		// whole cluster, no barrier needed.
		return localView()
	}
	leaderID := view.LeaderID()
	if leaderID == "" {
		log.Warn("leader confirm: no leader known", "topic", topicName)
		return topic.Topic{}, false, false
	}
	if leaderID == selfID {
		// Leader-local reads are authoritative only once the FSM has
		// applied every committed entry.
		if err := view.Barrier(); err != nil {
			log.Warn("leader confirm: barrier failed", "topic", topicName, "err", err)
			return topic.Topic{}, false, false
		}
		return localView()
	}
	member, err := view.GetMember(leaderID)
	if err != nil || member.Addr == "" {
		log.Warn("leader confirm: leader member unresolvable", "leader", leaderID, "topic", topicName, "err", err)
		return topic.Topic{}, false, false
	}
	rpcCtx, cancel := context.WithTimeout(ctx, leaderConfirmRPCTimeout)
	defer cancel()
	res, err := peer.GetTopic(rpcCtx, member.Addr, topicName)
	if err != nil {
		log.Warn("leader confirm: leader unreachable", "leader", leaderID, "topic", topicName, "err", err)
		return topic.Topic{}, false, false
	}
	switch res.Status {
	case http.StatusOK:
		var rec topic.Topic
		if err := json.Unmarshal(res.Body, &rec); err != nil {
			log.Warn("leader confirm: leader response undecodable", "leader", leaderID, "topic", topicName, "err", err)
			return topic.Topic{}, false, false
		}
		return rec, false, true
	case http.StatusNotFound:
		return topic.Topic{}, true, true
	default:
		log.Warn("leader confirm: leader returned non-OK", "leader", leaderID, "status", res.Status, "topic", topicName)
		return topic.Topic{}, false, false
	}
}

// topicAbsentOnLeader reports whether the leader confirms topicName does
// not exist — the bar for discarding its accepted WAL records.
func topicAbsentOnLeader(ctx context.Context, view leaderView, peer topicFetcher, selfID, topicName string, log *slog.Logger) bool {
	_, absent, ok := leaderTopicView(ctx, view, peer, selfID, topicName, log)
	return ok && absent
}
