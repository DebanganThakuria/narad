package runtime

// Retention alters on open logs, on every owner.
//
// An alter is forwarded to the Raft leader, and only there were cached
// logs closed so the next access reopened under the new record. Every
// other owner applied the replicated entry, which only moved the topic's
// version: the next Get re-validated the open log (same incarnation) and
// kept serving it under the retention it opened with, and the shared
// reaper kept reaping it at that bound for as long as it stayed open. A
// raise meant to protect a backlog still deleted at the old bound; a
// lowered bound freed nothing.
//
// Two paths now carry the record's retention into an open log, in place
// (storage.Log.SetRetentionMaxAge): the Get slow path's same-incarnation
// branch (openGuarded), for a log in use, and a background pass over the
// open logs whose topic version moved (followOpenLogs), for a log nobody
// touched since the alter, which is exactly the one whose records the
// reaper takes. Both run under the topic's guard and read the version
// before the record, so they never apply an older record over a newer
// one.
//
// The background pass runs every second but costs one atomic load while
// the replica's metadata holds still: it scans the open logs only when
// the replica's metadata version moved since the last complete pass. So
// an alter reaches an untouched log within about a second of the replica
// applying it, no wider than the window the leader itself has between
// applying the alter and closing its logs. The gate is the metadata
// version rather than the latest domain version because an apply
// advances it only after publishing the topic versions it moved: a pass
// that read it first sees them all, where one gated on the latest domain
// version (drawn before the topic's version is published) could record
// an alter as covered without having seen it. It also moves on member
// heartbeats, so a pass runs every few seconds in a live cluster, and
// anything one pass missed is caught by the next.

import (
	"context"
	"errors"
	"runtime/debug"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// retentionFollowTick is how often the background pass looks for open
// logs whose topic record changed.
const retentionFollowTick = time.Second

// metadataVersioner is the optional metastore capability that lets the
// background pass skip a scan while nothing in the replica's metadata
// changed; *metastore.Store has it.
type metadataVersioner interface {
	MetadataVersion() uint64
}

// retentionFollowState is what the background pass carries between
// passes: the metadata version the last complete pass ran against, and
// whether one ran at all.
type retentionFollowState struct {
	seen    uint64
	scanned bool
}

// applyRetention makes maxAge the open log's retention bound and logs a
// change. Caller holds the topic's guard.
func (g *Logs) applyRetention(topicName string, idx int, l *storage.Log, maxAge time.Duration) {
	before := l.RetentionMaxAge()
	if l.SetRetentionMaxAge(maxAge) {
		g.logger.Info("partition log retention follows the topic record",
			"topic", topicName, "partition", idx, "from", before, "to", max(maxAge, 0))
	}
}

// startRetentionFollow starts the background pass once for the life of
// the Logs, bound to ctx. RunIdleEviction and RunColdRetention both call
// it, so it runs on every node that serves, whether or not either of
// them is enabled.
func (g *Logs) startRetentionFollow(ctx context.Context) {
	g.followOnce.Do(func() {
		go g.followRetention(ctx, retentionFollowTick)
	})
}

// followRetention runs the background pass every interval until ctx is
// cancelled.
func (g *Logs) followRetention(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var st retentionFollowState
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.followRetentionOnce(&st)
		}
	}
}

// followRetentionOnce is one background pass. A panic is logged with
// its stack and the loop carries on: the pass must never take the
// process down, nor stop the next pass.
func (g *Logs) followRetentionOnce(st *retentionFollowState) {
	defer func() {
		if rec := recover(); rec != nil {
			g.logger.Error("partition log retention pass panicked; the loop continues",
				"panic", rec, "stack", string(debug.Stack()))
		}
	}()
	mv, gated := g.metastore.(metadataVersioner)
	var latest uint64
	if gated {
		// Read before the scan: a change that lands during it moves the
		// version past the recorded one, and the next pass scans again.
		latest = mv.MetadataVersion()
		if st.scanned && latest == st.seen {
			return
		}
	}
	if _, complete := g.followOpenLogs(); complete && gated {
		st.seen, st.scanned = latest, true
	}
}

// followOpenLogs applies the current topic record's retention to every
// open log whose topic version moved since the log was last validated,
// and reports how many logs it re-validated and whether every record it
// needed could be read (an incomplete pass is retried by the next). A
// Logs without a metastore or without topic versions has nothing to do.
func (g *Logs) followOpenLogs() (n int, complete bool) {
	if g.metastore == nil || g.versions == nil {
		return 0, true
	}
	var stale []string
	g.mu.RLock()
	for k, e := range g.logs {
		if e.closing || e.version.Load() == g.versions.TopicVersion(k.topic) {
			continue
		}
		stale = append(stale, k.topic)
	}
	g.mu.RUnlock()
	complete = true
	done := make(map[string]struct{}, len(stale))
	for _, name := range stale {
		if _, ok := done[name]; ok {
			continue
		}
		done[name] = struct{}{}
		m, err := g.followTopic(name)
		n += m
		if err != nil {
			complete = false
		}
	}
	return n, complete
}

// followTopic re-validates the open logs of one topic under its guard,
// as the Get slow path would for each: the version is read before the
// record, and a log opened under another incarnation is left for Get,
// which retires it. A deleted topic changes nothing and is not an error
// (its logs go with the purge); a lookup that failed is.
func (g *Logs) followTopic(topicName string) (int, error) {
	unlock := g.lockTopic(topicName)
	defer unlock()
	version := g.versions.TopicVersion(topicName)
	t, err := g.metastore.GetTopic(context.Background(), topicName)
	if errors.Is(err, errs.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	maxAge := retentionFromTopic(t.RetentionMs, g.storageOpts.Retention.CheckInterval).MaxAge
	type open struct {
		idx int
		e   *logEntry
	}
	var entries []open
	g.mu.RLock()
	for k, e := range g.logs {
		if k.topic == topicName && !e.closing && e.incarnation == t.ID {
			entries = append(entries, open{idx: k.idx, e: e})
		}
	}
	g.mu.RUnlock()
	for _, o := range entries {
		g.applyRetention(topicName, o.idx, o.e.log, maxAge)
		o.e.version.Store(version)
	}
	return len(entries), nil
}

// replicaCaughtUp reports whether the local metastore replica has
// applied everything the leader committed, when the metastore can tell
// (*metastore.Store can); one that cannot is taken as caught up. The
// cold walk opens closed logs under the replica's retention, so on a
// lagging replica (a restart, a partition) it would reap at a bound an
// alter already raised.
func (g *Logs) replicaCaughtUp() bool {
	c, ok := g.metastore.(interface{ AppliedCaughtUp() bool })
	return !ok || c.AppliedCaughtUp()
}
