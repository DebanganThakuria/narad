package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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
// It runs in a background goroutine during startup while the armed create
// gate holds topic creates on every transport, so the sweep's
// topic-existence checks can never race a concurrent create; the caller
// releases the gate and marks the node ready only after it returns. It
// returns early on ctx cancellation so shutdown during startup isn't
// blocked.
func runStartupReconcile(ctx context.Context, store *metastore.Store, logs *runtime.Logs, dataDir, nodeID string, log *slog.Logger) {
	if waitMetastoreCaughtUp(ctx, store, startupReconcileCaughtUpTimeout) {
		removed, err := runtime.SweepOrphanTopicDirs(dataDir, func(name string) bool {
			_, getErr := store.GetTopic(ctx, name)
			return !errors.Is(getErr, errs.ErrNotFound)
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
