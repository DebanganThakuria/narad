package runtime

// Retention for partitions whose log is not open.
//
// The shared reaper (storage.sharedReaper) walks the logs registered
// with it, and a log is registered only while it is open. The idle
// evictor closes a log untouched for the configured window once its
// retention "owes nothing", which in practice means a single active
// segment; that segment then ages past MaxAge with nothing left to roll
// and delete it, and nothing reopens the log until a producer or a local
// consumer touches the partition. A node that restarts registers nothing
// until first use either. Measured on devstack: a node held 24 expired
// partitions for minutes past eligibility until they were reopened by
// hand, and reaped them within two minutes of that.
//
// The walk below closes that gap from the file system side. Every
// interval it lists the partition directories on disk, skips the ones
// whose log is open (the shared reaper owns those), stats the rest
// without opening them, and for a partition with an expired segment
// opens the log through the ordinary lazy path, runs ONE retention
// sweep synchronously, and closes it again. Opening is deliberate: the
// reaper never deletes an active segment, it rolls it first so the next
// segment starts at the right base offset. Deleting files without the
// Log would either strand an expired active segment or restart offsets
// at zero under the committed-offset frontier. Open, sweep and close
// reuse the roll (commit path, high-watermark check), the sealed-segment
// picks, the detach under the write lock and the lock-free unlink that
// the open-log path already exercises.
//
// Cost: one ReadDir per topic and one stat per segment file per walk,
// with no opens unless something is due. A due partition is opened once
// per retention period. Partitions are handled one at a time, so a node
// with thousands due at once never holds thousands of logs open.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// coldWalkPause is the gap between two partitions the walk opens. Opening
// goes through the registry's slow path, which holds the global log
// lock while the segment tail is recovered, so a backlog of thousands of
// due partitions (every one is due after a restart) must not be opened
// back to back: the pause keeps the walk to at most ~100 opens a second
// and lets produce and consume through between them.
const coldWalkPause = 10 * time.Millisecond

// ReaperRestarts reports how many times the process-wide retention loop
// had to be replaced. Exposed here so the metrics poller is wired through
// the runtime layer that owns storage on this node.
func (g *Logs) ReaperRestarts() int64 { return storage.ReaperRestarts() }

// RunColdRetention runs the cold-partition retention walk every interval
// until ctx is cancelled. interval <= 0 disables it and returns at once.
func (g *Logs) RunColdRetention(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Watchdog for the shared reaper: a node whose logs are all
			// open never registers a new one, so this is the only other
			// place a stalled loop would be noticed.
			storage.EnsureReaperRunning()
			swept, err := g.ColdRetentionOnce(ctx, time.Now())
			if err != nil && g.logger != nil {
				g.logger.Warn("cold retention walk", "err", err, "swept", swept)
			}
		}
	}
}

// ColdRetentionOnce runs one walk and reports how many closed partitions
// it opened, swept and closed. The first error stops the walk of that
// topic only; the returned error is the first one seen.
func (g *Logs) ColdRetentionOnce(ctx context.Context, now time.Time) (int, error) {
	topicsRoot := filepath.Join(g.dataDir, "topics")
	entries, err := os.ReadDir(topicsRoot)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	swept := 0
	var firstErr error
	for _, entry := range entries {
		if ctx.Err() != nil {
			return swept, ctx.Err()
		}
		if !entry.IsDir() {
			continue
		}
		c, cerr := classifyTopicDir(topicsRoot, entry.Name())
		if cerr != nil || c.Quarantined {
			// A quarantined directory belongs to the orphan sweep, and a
			// directory whose marker cannot be read is not one to open.
			continue
		}
		maxAge, incarnation, ok := g.coldRetentionMaxAge(ctx, c.Topic)
		if !ok || maxAge <= 0 {
			continue
		}
		if c.Incarnation != "" && incarnation != "" && c.Incarnation != incarnation {
			// A directory left by a deleted incarnation of the same name:
			// opening it would quarantine it and create the new
			// incarnation's partition here whether or not this node owns
			// it. The orphan sweep and the next real access own that.
			continue
		}
		n, terr := g.coldRetentionTopic(ctx, c.Dir, c.Topic, maxAge, now)
		swept += n
		if terr != nil && firstErr == nil {
			firstErr = terr
		}
	}
	if swept > 0 && g.metrics != nil {
		g.metrics.ColdRetentionSweptTotal.Add(float64(swept))
	}
	return swept, firstErr
}

// coldRetentionMaxAge resolves a topic's age bound the way the lazy-open
// path does. ok=false means the topic is not one to touch: the local
// metastore no longer knows it (the orphan sweep owns that directory) or
// the lookup failed.
func (g *Logs) coldRetentionMaxAge(ctx context.Context, topicName string) (maxAge time.Duration, incarnation string, ok bool) {
	if g.metastore == nil {
		return g.storageOpts.Retention.MaxAge, "", true
	}
	t, err := g.metastore.GetTopic(ctx, topicName)
	if err != nil {
		if !errors.Is(err, errs.ErrNotFound) && g.logger != nil {
			g.logger.Warn("cold retention: topic lookup", "topic", topicName, "err", err)
		}
		return 0, "", false
	}
	return retentionFromTopic(t.RetentionMs, g.storageOpts.Retention.CheckInterval).MaxAge, t.ID, true
}

func (g *Logs) coldRetentionTopic(ctx context.Context, topicDir, topicName string, maxAge time.Duration, now time.Time) (int, error) {
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return 0, err
	}
	swept := 0
	cutoff := now.Add(-maxAge)
	var firstErr error
	for _, entry := range entries {
		if ctx.Err() != nil {
			return swept, ctx.Err()
		}
		idx, ok := storage.ParsePartitionDirName(entry.Name())
		if !ok || !entry.IsDir() {
			continue
		}
		key := keyOf(topicName, idx)
		if g.isOpen(key) {
			// Registered with the shared reaper already.
			continue
		}
		if g.coldDeferred(key, now) {
			// Looked due on a recent walk but the reaper could not reap
			// it (records above the persisted high-watermark, say): leave
			// it alone for a while rather than open it every walk.
			continue
		}
		st, err := coldPartitionStat(filepath.Join(topicDir, entry.Name()))
		if err != nil {
			// One unreadable partition must not hide every higher index
			// of the topic from the walk for good.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !coldPartitionDue(st, cutoff) {
			continue
		}
		opened := time.Now()
		changed, err := g.sweepColdPartition(topicName, idx)
		if err != nil {
			if errors.Is(err, errs.ErrTopicNotFound) {
				return swept, firstErr
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if changed {
			swept++
		} else {
			g.deferCold(key, now.Add(coldWalkRetryAfter))
		}
		// Pace the opens: at least coldWalkPause, and no faster than one
		// open per open-duration, so a partition with a large tail to
		// recover cannot make the walk hold the global lock back to back.
		pause := max(coldWalkPause, time.Since(opened))
		select {
		case <-ctx.Done():
			return swept, ctx.Err()
		case <-time.After(pause):
		}
	}
	return swept, firstErr
}

// coldWalkRetryAfter is how long the walk leaves a partition alone after
// opening it found nothing the reaper could remove yet.
const coldWalkRetryAfter = 30 * time.Minute

// isOpen reports whether the partition's log is open, walk-owned or not
// (Peek hides walk-owned entries on purpose; the walk itself must see
// them so it never opens one twice).
func (g *Logs) isOpen(key string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, open := g.logs[key]
	return open
}

// coldDeferred reports whether the walk deferred this partition and the
// deferral still stands; an elapsed one is dropped.
func (g *Logs) coldDeferred(key string, now time.Time) bool {
	g.coldMu.Lock()
	defer g.coldMu.Unlock()
	until, ok := g.coldDefer[key]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(g.coldDefer, key)
	return false
}

func (g *Logs) deferCold(key string, until time.Time) {
	g.coldMu.Lock()
	defer g.coldMu.Unlock()
	if g.coldDefer == nil {
		g.coldDefer = make(map[string]time.Time)
	}
	g.coldDefer[key] = until
}

// coldStat is what the due check needs from a closed partition, read
// from one directory listing: no open, no index, no high-watermark file.
type coldStat struct {
	segments        int
	sizeBytes       int64
	oldestSegmentAt int64 // unix seconds; mtime of the lowest-offset segment
}

// coldPartitionStat lists a partition directory once and takes segment
// count, total size and the oldest segment's mtime from the entries.
// Segment files sort by base offset because their names are zero-padded,
// so the first one is the oldest. A directory that does not exist is an
// empty partition.
func coldPartitionStat(dir string) (coldStat, error) {
	var st coldStat
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	for _, e := range entries {
		if _, ok := storage.ParseSegmentFileName(e.Name()); !ok || e.IsDir() {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, os.ErrNotExist) {
			// Reaped between the listing and the stat.
			continue
		}
		if err != nil {
			return st, err
		}
		if st.segments == 0 {
			st.oldestSegmentAt = info.ModTime().Unix()
		}
		st.segments++
		st.sizeBytes += info.Size()
	}
	return st, nil
}

// coldPartitionDue reports whether a closed partition holds anything the
// reaper would remove: a sealed segment older than the cutoff, or a
// non-empty active segment whose last write is older than the cutoff
// (every record in it has expired, so the reaper rolls and deletes it).
// A segment file's mtime is its last write, and the oldest segment is
// the one the reaper would take first, so its mtime decides.
func coldPartitionDue(st coldStat, cutoff time.Time) bool {
	if st.segments == 0 || st.oldestSegmentAt == 0 {
		return false
	}
	if !time.Unix(st.oldestSegmentAt, 0).Before(cutoff) {
		return false
	}
	return st.segments > 1 || st.sizeBytes > 0
}

// sweepColdPartition opens the partition through the ordinary lazy path,
// runs one retention sweep, and closes it again unless something else
// started using it in between: a Get that arrived after ours stamped
// the entry, and closing a log a caller is about to use is the evictor's
// invariant 5 all over again. The idle evictor reclaims it later in that
// case, the same as any other warm log.
func (g *Logs) sweepColdPartition(topicName string, idx int) (changed bool, err error) {
	l, entry, err := g.openForWalk(topicName, idx)
	if errors.Is(err, errColdWalkRaced) {
		// Someone opened it since the stat: it is registered with the
		// shared reaper now and no longer the walk's to touch.
		return false, nil
	}
	if err != nil {
		return false, err
	}

	changed = l.SweepRetentionNow()

	_, err = g.closeIfStill(keyOf(topicName, idx), entry, func(cur *logEntry) bool { return cur.walkOwned.Load() }, "cold_retention_close")
	return changed, err
}

// errColdWalkRaced reports that a partition the walk meant to open was
// already open by the time it took the locks.
var errColdWalkRaced = errors.New("cold retention: partition opened by someone else")

// openForWalk opens a closed partition's log for the walk and claims it
// in the same critical section that installs the entry, so no Get can
// slip in between the open and the claim: every Get that follows runs
// after the install and clears walkOwned (stamp), and the close at the
// end of the sweep only happens while the flag is still set. An entry
// that already exists is left alone with errColdWalkRaced.
func (g *Logs) openForWalk(topicName string, idx int) (*storage.Log, *logEntry, error) {
	key := keyOf(topicName, idx)
	unlock := g.lockTopic(topicName)
	defer unlock()
	g.mu.Lock()
	if _, open := g.logs[key]; open {
		g.mu.Unlock()
		return nil, nil, errColdWalkRaced
	}
	l, quarantined, err := g.openLocked(topicName, idx, key)
	var entry *logEntry
	if err == nil {
		entry = g.logs[key]
		if entry != nil {
			entry.walkOwned.Store(true)
		}
	}
	g.mu.Unlock()
	if quarantined {
		g.notifyRetired(topicName)
	}
	if err != nil {
		return nil, nil, err
	}
	if entry == nil {
		return nil, nil, errColdWalkRaced
	}
	return l, entry, nil
}
