package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// startupReconcileCaughtUpTimeout bounds how long startup reconciliation
// waits for the local metastore replica to catch up before giving up on
// the (destructive) orphan sweep.
const startupReconcileCaughtUpTimeout = 60 * time.Second

// runStartupReconcile waits for the local metastore replica to catch up,
// then (1) removes orphaned topic directories left by a crash between a
// topic's metastore delete and its file purge, and (2) opens this node's
// owned partition logs so retention reapers run for topics that are idle
// after a restart. The sweep is skipped if the replica never catches up,
// since acting on a stale topic set could delete live data.
//
// Local absence is NOT sufficient to delete: a freshly restarted node's
// replica can be restored from an old Raft snapshot and read as "caught
// up" against its own log while missing hours of cluster history — a
// sweep trusting it would destroy live topics' data (observed under
// crash testing). Every deletion candidate is therefore confirmed
// ABSENT ON THE LEADER first; if the leader cannot confirm (unreachable,
// this node believes it leads but is stale, any error), the directory
// is KEPT. Orphan reclamation is an optimization — leaking a directory
// until the next restart is always preferable to deleting live data.
//
// It runs in a background goroutine during startup while the armed create
// gate holds topic creates on every transport, so the sweep's
// topic-existence checks can never race a concurrent create; the caller
// releases the gate and marks the node ready only after it returns. It
// returns early on ctx cancellation so shutdown during startup isn't
// blocked.
func runStartupReconcile(ctx context.Context, store *metastore.Store, logs *runtime.Logs, peer *cluster.PeerClient, dataDir, nodeID string, log *slog.Logger) {
	if waitMetastoreCaughtUp(ctx, store, startupReconcileCaughtUpTimeout) {
		removed, err := runtime.SweepOrphanTopicDirs(dataDir, func(name string) bool {
			_, getErr := store.GetTopic(ctx, name)
			if !errors.Is(getErr, errs.ErrNotFound) {
				return true // present locally (or lookup failed): keep
			}
			return !confirmedAbsentOnLeader(ctx, store, peer, nodeID, name, log)
		}, log)
		if err != nil {
			log.Warn("startup orphan sweep encountered errors", "err", err)
		}
		if len(removed) > 0 {
			log.Info("startup orphan sweep reclaimed topic directories", "count", len(removed))
		}
	} else if ctx.Err() == nil {
		log.Warn("skipping startup orphan sweep: metastore not caught up within timeout")
	}
	if ctx.Err() != nil {
		// Shutting down during startup: don't open logs we're about to close.
		return
	}
	openOwnedPartitionLogs(ctx, store, logs, nodeID, log)
}

// leaderView is the slice of the metastore the absence check needs;
// *metastore.Store implements it, tests fake it.
type leaderView interface {
	LeaderID() string
	GetMember(id string) (metastore.Member, error)
	Barrier() error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
}

// topicGetter fetches a topic record from a peer; *cluster.PeerClient
// implements it.
type topicGetter interface {
	GetTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error)
}

// confirmedAbsentOnLeader reports whether the LEADER confirms the topic
// does not exist. Only a definitive 404 from the leader authorizes a
// deletion; every other outcome (leader unknown, unreachable, non-404
// answer) keeps the directory. When this node leads itself, its state is
// authoritative only past a Raft barrier: election guarantees a fresh
// leader's LOG, not that its FSM has applied it, so a just-elected node
// restored from an old snapshot must not trust local absence until the
// replay provably finished — and the absence must be re-checked AFTER
// the barrier, since the caller's check predates it.
func confirmedAbsentOnLeader(ctx context.Context, store leaderView, peer topicGetter, nodeID, name string, log *slog.Logger) bool {
	leaderID := store.LeaderID()
	if leaderID == "" {
		return false
	}
	if leaderID == nodeID {
		if err := store.Barrier(); err != nil {
			log.Warn("orphan sweep: leader barrier failed; keeping dir", "topic", name, "err", err)
			return false
		}
		_, err := store.GetTopic(ctx, name)
		return errors.Is(err, errs.ErrNotFound)
	}
	member, err := store.GetMember(leaderID)
	if err != nil || member.Addr == "" {
		log.Warn("orphan sweep: cannot resolve leader for absence check; keeping dir",
			"topic", name, "leader", leaderID, "err", err)
		return false
	}
	res, err := peer.GetTopic(ctx, member.Addr, name)
	if err != nil {
		log.Warn("orphan sweep: leader absence check failed; keeping dir",
			"topic", name, "leader", leaderID, "err", err)
		return false
	}
	return res.Status == http.StatusNotFound
}

// waitMetastoreCaughtUp polls until the local replica has applied all
// committed entries (with a leader present), ctx is cancelled, or timeout.
func waitMetastoreCaughtUp(ctx context.Context, store *metastore.Store, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if store.AppliedCaughtUp() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// openOwnedPartitionLogs opens the partition logs this node owns so their
// retention reapers run regardless of produce/consume activity. Logs.Get
// refuses topics absent from the metastore, so deleted topics are never
// reopened here.
func openOwnedPartitionLogs(ctx context.Context, store *metastore.Store, logs *runtime.Logs, nodeID string, log *slog.Logger) {
	topics, _, err := store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		log.Warn("retention warmup: list topics failed", "err", err)
		return
	}
	opened := 0
	for _, t := range topics {
		assignments, err := store.ListAssignments(t.Name)
		if err != nil {
			continue
		}
		for _, a := range assignments {
			if a.OwnerID != nodeID {
				continue
			}
			if _, err := logs.Get(t.Name, a.Partition); err != nil {
				log.Debug("retention warmup: open owned partition failed", "topic", t.Name, "partition", a.Partition, "err", err)
				continue
			}
			opened++
		}
	}
	if opened > 0 {
		log.Info("retention warmup: opened owned partition logs", "count", opened)
	}
}
