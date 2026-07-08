package cluster

// Leader confirmation for fan-out tail anchors. A cursor may only anchor
// at the parent's tail — skipping all earlier records and overwriting the
// shared offset file — under an attach epoch the LEADER agrees is the
// child's current attachment. Local FSM state is not sufficient: a
// freshly restarted replica restored from an old Raft snapshot presents
// dead epochs as live, and anchoring on one clobbers the real cursor's
// resume point, so the eventual catch-up re-anchors at a later tail and
// silently drops the window in between. Every failure mode here returns
// false: deferring a cursor is cheap (the reconciler respawns it every
// pass), destroying its state is not.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const fanoutEpochConfirmTimeout = 5 * time.Second

// fanoutLeaderView is the slice of the metastore needed to locate the
// current leader.
type fanoutLeaderView interface {
	LeaderID() string
	GetMember(id string) (metastore.Member, error)
}

// fanoutTopicFetcher fetches a topic record from a peer; peerClient
// satisfies it.
type fanoutTopicFetcher interface {
	GetTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error)
}

func epochConfirmedByLeader(ctx context.Context, view fanoutLeaderView, peer fanoutTopicFetcher, selfID string, key fanoutCursorKey, log *slog.Logger) bool {
	if selfID == "" {
		return true // no cluster identity: the local store is the authority
	}
	leaderID := view.LeaderID()
	if leaderID == "" {
		log.Warn("fanout: no leader known; deferring tail anchor",
			"parent", key.parent, "partition", key.partition, "child", key.child)
		return false
	}
	if leaderID == selfID {
		return true // leader state is quorum-supported
	}
	member, err := view.GetMember(leaderID)
	if err != nil || member.Addr == "" {
		log.Warn("fanout: leader member unresolvable; deferring tail anchor",
			"leader", leaderID, "parent", key.parent, "child", key.child, "err", err)
		return false
	}
	rpcCtx, cancel := context.WithTimeout(ctx, fanoutEpochConfirmTimeout)
	defer cancel()
	res, err := peer.GetTopic(rpcCtx, member.Addr, key.child)
	if err != nil {
		log.Warn("fanout: leader epoch check unreachable; deferring tail anchor",
			"leader", leaderID, "parent", key.parent, "child", key.child, "err", err)
		return false
	}
	if res.Status != http.StatusOK {
		// 404 means the child is gone on the leader: the link is dead and
		// the reconciler will stop this cursor once the replica catches up.
		log.Warn("fanout: leader epoch check not OK; deferring tail anchor",
			"leader", leaderID, "status", res.Status, "parent", key.parent, "child", key.child)
		return false
	}
	var t topic.Topic
	if err := json.Unmarshal(res.Body, &t); err != nil {
		log.Warn("fanout: leader epoch check body undecodable; deferring tail anchor",
			"leader", leaderID, "parent", key.parent, "child", key.child, "err", err)
		return false
	}
	return t.Parent == key.parent && t.AttachEpoch == key.epoch
}
