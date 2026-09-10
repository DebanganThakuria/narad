package runtime

// Idle partition-log eviction. An open log costs two goroutines
// (flusher + reaper), open segment file descriptors, and buffer/index
// memory — forever, because nothing closed logs between topic delete
// and shutdown. Topics that were used once and abandoned accumulate
// that tax without bound. The evictor closes logs that no Get has
// touched for the configured idle window; the lazy-open path reopens
// them transparently on the next real use.
//
// Correctness invariants, in the order they are enforced:
//
//  1. Only Get stamps lastAccess. Peek (metrics polls) never does, so
//     observation cannot keep a log warm — and, symmetrically, the
//     fan-out read path uses Peek + the durable HWM file to check
//     "am I caught up?" so an attached-but-silent child cannot either.
//  2. A log is a candidate only when its retention owes nothing:
//     age-based retention with sealed segments still on disk defers
//     eviction (closing would stop the reaper and strand those
//     segments forever). Keep-forever logs evict regardless — their
//     segments are meant to stay.
//  3. Eviction holds the partition's produce-serialization mutex, so
//     it can never interleave with a produce commit's append+fsync
//     critical section.
//  4. The close-and-delete happens under the map's WRITE lock — the
//     same discipline as CloseTopic — so a concurrent Get cannot open
//     a second log over the same directory while the first is still
//     flushing its final state.
//  5. Candidacy is re-checked under those locks before closing: a Get
//     that stamped the entry after the scan aborts the eviction.
//
// Close itself force-syncs the high-watermark file and wakes any
// long-poll waiters, so an evicted log leaves exact durable state and
// no goroutine sleeps against a dead notify channel.

import (
	"context"
	"strings"
	"time"
)

// evictionTick is how often the evictor scans for idle logs. The scan
// is a read-locked map walk — cheap at any realistic log count.
const evictionTick = time.Minute

// RunIdleEviction closes partition logs untouched for idleAfter,
// blocking until ctx is cancelled. idleAfter <= 0 disables eviction
// and returns immediately.
func (g *Logs) RunIdleEviction(ctx context.Context, idleAfter time.Duration) {
	if idleAfter <= 0 {
		return
	}
	ticker := time.NewTicker(evictionTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.EvictIdleOnce(idleAfter)
		}
	}
}

// EvictIdleOnce runs one eviction sweep and reports how many logs it
// closed. Exported for tests; production use goes through
// RunIdleEviction.
func (g *Logs) EvictIdleOnce(idleAfter time.Duration) int {
	cutoff := time.Now().Add(-idleAfter).UnixNano()

	// Snapshot candidates under the read lock; all closing happens
	// per-candidate under the full lock discipline below.
	type candidate struct {
		key   string
		entry *logEntry
	}
	g.mu.RLock()
	candidates := make([]candidate, 0)
	for k, e := range g.logs {
		if !evictable(e, cutoff) {
			continue
		}
		candidates = append(candidates, candidate{key: k, entry: e})
	}
	openCount := len(g.logs)
	g.mu.RUnlock()

	evicted := 0
	for _, c := range candidates {
		// Re-verify under the locks: same entry still installed, still
		// idle, retention still owes nothing. A Get since the scan
		// (which stamped it) or a CloseTopic (which removed it) aborts.
		if closed, _ := g.closeIfStill(c.key, c.entry, func(cur *logEntry) bool { return evictable(cur, cutoff) }, "idle_evict_close"); closed {
			evicted++
		}
	}

	if g.metrics != nil {
		if evicted > 0 {
			g.metrics.IdleLogsEvictedTotal.Add(float64(evicted))
		}
		g.metrics.OpenPartitionLogs.Set(float64(openCount - evicted))
	}
	return evicted
}

// closeIfStill closes and forgets one open log under the full lock
// discipline (invariants 3-5): the partition's produce mutex, then the
// registry write lock, then a re-check that the same entry is still
// installed and still satisfies `still`. A Get that raced in (stamping
// the entry, or reopening it) makes the check fail and the log stays
// open for its caller. Close runs under g.mu, like CloseTopic: a Get
// blocks until the old log has fully flushed and released its files,
// then reopens fresh. A close error is counted under errKind.
func (g *Logs) closeIfStill(key string, entry *logEntry, still func(*logEntry) bool, errKind string) (closed bool, err error) {
	topicName, idx, ok := splitKey(key)
	if !ok {
		return false, nil
	}
	unlock := g.lockProduce(topicName, idx)
	defer unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	cur, present := g.logs[key]
	if !present || cur != entry || !still(cur) {
		return false, nil
	}
	delete(g.logs, key)
	if err := cur.log.Close(); err != nil {
		if g.metrics != nil {
			g.metrics.IncError("storage", errKind)
		}
		return true, err
	}
	return true, nil
}

// OpenCount is the number of partition logs currently open on this
// node. The metrics poller sets narad_open_partition_logs from it on
// every tick; the eviction sweep used to be the gauge's only writer, so
// with eviction disabled it stayed at 0 forever and otherwise lagged by
// up to a sweep interval.
func (g *Logs) OpenCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.logs)
}

// evictable reports whether an entry may be closed: idle past the
// cutoff, and not deferred by pending retention work (invariant 2).
func evictable(e *logEntry, cutoffUnixNano int64) bool {
	if e.lastAccess.Load() > cutoffUnixNano {
		return false
	}
	return e.log.RetentionMaxAge() == 0 || e.log.SegmentCount() <= 1
}

// splitKey reverses keyOf. Topic names cannot contain '/', so the last
// separator is unambiguous.
func splitKey(key string) (topicName string, idx int, ok bool) {
	i := strings.LastIndexByte(key, '/')
	if i <= 0 || i == len(key)-1 {
		return "", 0, false
	}
	n := 0
	for _, ch := range key[i+1:] {
		if ch < '0' || ch > '9' {
			return "", 0, false
		}
		n = n*10 + int(ch-'0')
	}
	return key[:i], n, true
}
