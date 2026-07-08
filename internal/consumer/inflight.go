// Package consumer implements broker-side consumer state: receipt
// handles and the per-partition in-flight reservation table that backs
// visibility timeouts, acks, and redelivery.
package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Aliases of the canonical sentinels in internal/errs, re-exported so
// callers of this package can match failures without importing errs.
var (
	// ErrHandleStale reports a handle whose reservation no longer
	// exists: expired, re-reserved under a new nonce, already
	// committed, or belonging to a dropped shard.
	ErrHandleStale = errs.ErrHandleStale

	// ErrAckedAheadFull reports that the bounded ahead-of-frontier
	// state (out-of-order acks plus corrupt skips) is at capacity.
	ErrAckedAheadFull = errs.ErrAckedAheadFull
)

// CommitFunc is called after the committed-offset frontier advances for
// a partition. Implementations write the new offset to the per-partition
// .offsets log for crash-recovery durability. Errors are handled inside
// the implementation; the caller does not fail if the write fails.
type CommitFunc func(topic string, partition int, offset int64)

// CapsResolver returns per-topic in-flight limits. Called once at shard
// creation; update live shards via RefreshCaps when caps change.
type CapsResolver func(ctx context.Context, topic string) (Caps, error)

// CommittedRecoverFunc returns the durably committed consumer offset for
// a partition (ok=false when none was ever committed). Wired to the
// persisted per-partition offset file so a lazily created shard — first
// consume after a restart, or after ownership moved to this node — never
// starts at -1 and re-delivers the whole partition. Recovering from DISK
// at shard creation (not from a startup scan over metastore assignments)
// keeps this correct even while the local metastore replica is stale:
// the file's presence is ground truth that this node served the
// partition before.
type CommittedRecoverFunc func(topic string, partition int) (int64, bool)

// Caps bounds per-partition in-flight state.
type Caps struct {
	MaxInFlight   int
	MaxAckedAhead int
}

// ReleaseFunc is called after a live reservation was released for a
// partition — by the background purger evicting an expired reservation,
// or by an ack/skip removing one — so the messages are redeliverable
// again and blocked long-poll consumers must be woken.
// Called without any shard lock held; implementations may be slow but
// must not call back into InFlight for the same partition synchronously
// in a way that assumes reservation state is unchanged.
type ReleaseFunc func(topic string, partition int)

// InFlight tracks in-flight message reservations per partition.
// All state is in-memory — a restart clears reservations, causing
// at-most one redelivery per message (visibility timeout).
type InFlight struct {
	mu       sync.RWMutex
	shards   map[shardKey]*partitionShard
	onCommit CommitFunc
	resolve  CapsResolver
	recover  CommittedRecoverFunc

	clockMu sync.RWMutex
	timeNow func() int64 // replaced in tests

	notifyMu  sync.RWMutex
	onRelease ReleaseFunc
}

// NewInFlight creates an InFlight tracker. onCommit may be nil (no
// persistence, useful for tests or early development).
func NewInFlight(resolve CapsResolver, onCommit CommitFunc) *InFlight {
	return &InFlight{
		shards:   make(map[shardKey]*partitionShard),
		onCommit: onCommit,
		resolve:  resolve,
		timeNow:  nowUnixMs,
	}
}

// SetCommittedRecovery registers fn as the durable-offset source consulted
// when a shard is created lazily. Call once during wiring, before serving.
func (f *InFlight) SetCommittedRecovery(fn CommittedRecoverFunc) {
	f.recover = fn
}

// Init seeds a partition shard with a known committed offset, resolving
// caps eagerly. Production recovery happens lazily via
// SetCommittedRecovery; Init remains for tests and callers that must
// pre-warm a shard.
func (f *InFlight) Init(ctx context.Context, topic string, partition int, committed int64) error {
	caps, err := f.resolvedCaps(ctx, topic)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.shards[shardKey{topic, partition}] = newPartitionShard(committed, caps)
	f.mu.Unlock()
	return nil
}

// Next returns the next offset to deliver (committed+1). Used by the
// metrics poller to compute consumer lag.
func (f *InFlight) Next(topic string, partition int) int64 {
	sh := f.shard(topic, partition)
	if sh == nil {
		return 0
	}

	sh.mu.Lock()
	n := sh.committed + 1
	sh.mu.Unlock()
	return n
}

// Snapshot returns current shard sizes for the metrics poller.
func (f *InFlight) Snapshot(topic string, partition int) (inFlight, ackedAhead int) {
	sh := f.shard(topic, partition)
	if sh == nil {
		return 0, 0
	}

	sh.mu.Lock()
	inFlight = len(sh.entries)
	ackedAhead = len(sh.ackedAhead)
	sh.mu.Unlock()
	return
}

// RefreshCaps updates limits on all live shards for a topic. Called
// when an operator alters a topic's caps via the alter endpoint.
func (f *InFlight) RefreshCaps(ctx context.Context, topic string) error {
	caps, err := f.resolvedCaps(ctx, topic)
	if err != nil {
		return err
	}

	f.mu.RLock()
	for k, sh := range f.shards {
		if k.topic != topic {
			continue
		}
		sh.mu.Lock()
		sh.maxInFlight = caps.MaxInFlight
		sh.maxAckedAhead = caps.MaxAckedAhead
		sh.mu.Unlock()
	}
	f.mu.RUnlock()
	return nil
}

// DropTopic removes all shards for a topic. Called on topic deletion.
func (f *InFlight) DropTopic(topic string) {
	f.mu.Lock()
	for k := range f.shards {
		if k.topic == topic {
			delete(f.shards, k)
		}
	}
	f.mu.Unlock()
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

// notifyRelease invokes the release notifier for (topic, partition), if
// one is registered. Callers must NOT hold any shard lock — the
// notifier may call back into log wake-up paths (deadlock discipline
// documented on SetReleaseNotifier and ReleaseFunc).
func (f *InFlight) notifyRelease(topic string, partition int) {
	if notify := f.releaseNotifier(); notify != nil {
		notify(topic, partition)
	}
}

// shard returns the live shard for (topic, partition), or nil.
func (f *InFlight) shard(topic string, partition int) *partitionShard {
	f.mu.RLock()
	sh := f.shards[shardKey{topic, partition}]
	f.mu.RUnlock()
	return sh
}

// shardOrCreate returns the live shard for (topic, partition), creating
// one with freshly resolved caps if none exists. On a create race the
// first shard stored wins and the loser is discarded.
func (f *InFlight) shardOrCreate(ctx context.Context, topic string, partition int) (*partitionShard, error) {
	key := shardKey{topic, partition}
	if sh := f.shard(topic, partition); sh != nil {
		return sh, nil
	}

	caps, err := f.resolvedCaps(ctx, topic)
	if err != nil {
		return nil, err
	}

	committed := int64(-1)
	if f.recover != nil {
		if off, ok := f.recover(topic, partition); ok {
			committed = off
		}
	}
	fresh := newPartitionShard(committed, caps)

	f.mu.Lock()
	if existing := f.shards[key]; existing != nil {
		f.mu.Unlock()
		return existing, nil
	}
	f.shards[key] = fresh
	f.mu.Unlock()
	return fresh, nil
}

// resolvedCaps fetches and validates the topic's caps.
func (f *InFlight) resolvedCaps(ctx context.Context, topic string) (Caps, error) {
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return Caps{}, err
	}
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return Caps{}, errors.New("consumer: caps must be positive")
	}
	return caps, nil
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
