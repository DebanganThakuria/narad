package cluster

// Tail-anchor confirmation. Anchoring a fan-out cursor at the parent's
// tail skips every earlier record and overwrites the shared offset file,
// so it may only happen under an attach epoch the LEADER confirms is the
// child's live attachment — via leaderTopicView, which also handles the
// freshly-elected-leader case with a Raft barrier. Deferring is cheap
// (the reconciler respawns the cursor every pass); destroying cursor
// state is not.

import (
	"context"
	"log/slog"
)

func epochConfirmedByLeader(ctx context.Context, view leaderView, peer topicFetcher, selfID string, key fanoutCursorKey, log *slog.Logger) bool {
	child, absent, ok := leaderTopicView(ctx, view, peer, selfID, key.child, log)
	if !ok || absent {
		// Unconfirmed, or the child is gone on the leader: the link is
		// dead and the reconciler will stop this cursor once the local
		// replica catches up.
		return false
	}
	return child.Parent == key.parent && child.AttachEpoch == key.epoch
}

// fanoutLinkDissolvedOnLeader reports whether the leader confirms that
// child is NOT attached to parent — the bar for removing the (parent,
// partition, child) cursor offset file. The local view is not enough: a
// stale replica can be missing a live link, and deleting the file forces
// the eventual real cursor to tail-anchor, silently skipping its backlog.
func fanoutLinkDissolvedOnLeader(ctx context.Context, view leaderView, peer topicFetcher, selfID, parent, child string, log *slog.Logger) bool {
	rec, absent, ok := leaderTopicView(ctx, view, peer, selfID, child, log)
	if !ok {
		return false
	}
	if absent {
		return true
	}
	return !rec.IsChild() || rec.Parent != parent
}
