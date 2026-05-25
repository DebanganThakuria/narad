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

// purgeAll sweeps every shard and evicts expired reservations.
func (f *InFlight) purgeAll() {
	now := f.now()
	f.mu.RLock()
	shards := make([]*partitionShard, 0, len(f.shards))
	for _, sh := range f.shards {
		shards = append(shards, sh)
	}
	f.mu.RUnlock()

	for _, sh := range shards {
		sh.mu.Lock()
		sh.purgeExpired(now)
		sh.mu.Unlock()
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
