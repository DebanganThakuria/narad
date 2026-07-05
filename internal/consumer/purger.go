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
		released := sh.purgeExpiredLocked(now)
		sh.mu.Unlock()
		if released > 0 && notify != nil {
			notify(keys[i].topic, keys[i].partition)
		}
	}
}
