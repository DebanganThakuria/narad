package consumer

import (
	"context"
	"errors"
)

const initialExpiryHeapCap = 64

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

// Init seeds a partition shard with the committed offset recovered from
// the .offsets file on startup. Call before the first ReserveNext on any
// partition that has prior commit history.
func (f *InFlight) Init(ctx context.Context, topic string, partition int, committed int64) error {
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return err
	}
	if err := validateCaps(caps); err != nil {
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
	sh := f.getShard(topic, partition)
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
	sh := f.getShard(topic, partition)
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
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return err
	}
	if err := validateCaps(caps); err != nil {
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

func (f *InFlight) getShard(topic string, partition int) *partitionShard {
	f.mu.RLock()
	sh := f.shards[shardKey{topic, partition}]
	f.mu.RUnlock()
	return sh
}

func (f *InFlight) getOrCreate(ctx context.Context, topic string, partition int) (*partitionShard, error) {
	key := shardKey{topic, partition}
	if sh := f.getShard(topic, partition); sh != nil {
		return sh, nil
	}

	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := validateCaps(caps); err != nil {
		return nil, err
	}

	fresh := newPartitionShard(-1, caps)

	f.mu.Lock()
	if existing := f.shards[key]; existing != nil {
		f.mu.Unlock()
		return existing, nil
	}
	f.shards[key] = fresh
	f.mu.Unlock()
	return fresh, nil
}

func newPartitionShard(committed int64, caps Caps) *partitionShard {
	return &partitionShard{
		committed:     committed,
		entries:       make(map[int64]reservation),
		expiry:        make(expiryHeap, 0, initialPartitionExpiryCap(caps.MaxInFlight)),
		ackedAhead:    make(map[int64]struct{}),
		corrupt:       make(map[int64]struct{}),
		maxInFlight:   caps.MaxInFlight,
		maxAckedAhead: caps.MaxAckedAhead,
	}
}

func initialPartitionExpiryCap(maxInFlight int) int {
	if maxInFlight < initialExpiryHeapCap {
		return maxInFlight
	}
	return initialExpiryHeapCap
}

func validateCaps(caps Caps) error {
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return errors.New("consumer: caps must be positive")
	}
	return nil
}
