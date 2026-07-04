package consumer

import (
	"context"
	"time"
)

// RunPurger periodically sweeps all shards and evicts expired reservations.
// Run it in a goroutine alongside the broker; it stops when ctx is cancelled.
// A 1-second interval is a reasonable default for most deployments.
func (f *InFlight) RunPurger(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f.purgeAll()
		}
	}
}

// SetReleaseNotifier registers fn to be invoked whenever a live
// reservation is released for a (topic, partition) — by the background
// purger evicting expired reservations, or by an ack/skip removing one
// (CommitHandle/SkipCorrupt), which frees a MaxInFlight cap slot.
// Either way the partition may have deliverable messages again.
// Long-poll consumers block until a partition signals activity, and
// neither an expiry nor a cap slot freeing is activity the partition
// log can see on its own, so the broker wires this to the log's
// broadcast wake-up. fn is called without shard locks held. Passing nil
// disables notification.
func (f *InFlight) SetReleaseNotifier(fn ReleaseFunc) {
	f.notifyMu.Lock()
	f.onRelease = fn
	f.notifyMu.Unlock()
}

func (f *InFlight) releaseNotifier() ReleaseFunc {
	f.notifyMu.RLock()
	fn := f.onRelease
	f.notifyMu.RUnlock()
	return fn
}

// purgeAll sweeps every shard and evicts expired reservations, then
// notifies the release notifier for each partition where reservations
// were actually released.
func (f *InFlight) purgeAll() {
	now := f.now()
	f.mu.RLock()
	keys := make([]shardKey, 0, len(f.shards))
	shards := make([]*partitionShard, 0, len(f.shards))
	for k, sh := range f.shards {
		keys = append(keys, k)
		shards = append(shards, sh)
	}
	f.mu.RUnlock()

	notify := f.releaseNotifier()
	for i, sh := range shards {
		sh.mu.Lock()
		released := sh.purgeExpired(now)
		sh.mu.Unlock()
		if released > 0 && notify != nil {
			notify(keys[i].topic, keys[i].partition)
		}
	}
}

func (f *InFlight) now() int64 {
	f.clockMu.RLock()
	now := f.timeNow
	f.clockMu.RUnlock()
	return now()
}

func (f *InFlight) setTimeNow(now func() int64) {
	f.clockMu.Lock()
	f.timeNow = now
	f.clockMu.Unlock()
}

func nowUnixMs() int64 {
	return time.Now().UnixMilli()
}
