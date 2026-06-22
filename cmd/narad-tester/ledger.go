package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	consumeOutcomeValid     = "valid"
	consumeOutcomeDuplicate = "duplicate"
	consumeOutcomeUnknown   = "unknown"

	ledgerShardCount = 256
)

type ledger struct {
	shards    [ledgerShardCount]ledgerShard
	sequences [ledgerShardCount]sequenceShard

	outstanding     atomic.Int64
	consumed        atomic.Int64
	highestProduced atomic.Int64
}

type ledgerShard struct {
	mu sync.Mutex

	outstanding map[string]ledgerRecord
}

type ledgerRecord struct {
	ID               string
	RunID            string
	Topic            string
	Sequence         int64
	Key              string
	ProducedAtUnixMs int64
	Partition        int
	Offset           int64
}

type sequenceShard struct {
	mu    sync.Mutex
	words []uint64
}

type consumeLedgerResult struct {
	Outcome          string
	ProducedAtUnixMs int64
}

type ledgerStats struct {
	ProducedOutstanding int64
	Pending             int64
	Missing             int64
	OldestProducedAge   float64
	OutstandingRecords  int64
	ConsumedSequences   int64
}

func newLedger() *ledger {
	l := &ledger{}
	for i := range l.shards {
		l.shards[i].outstanding = make(map[string]ledgerRecord)
	}
	return l
}

func (l *ledger) Close() error {
	return nil
}

func (l *ledger) recordProduced(rec ledgerRecord) error {
	if rec.ID == "" {
		return errors.New("ledger record id is required")
	}
	if rec.ProducedAtUnixMs == 0 {
		return errors.New("ledger produced timestamp is required")
	}

	shard := l.shard(rec.ID)
	shard.mu.Lock()
	_, existed := shard.outstanding[rec.ID]
	shard.outstanding[rec.ID] = rec
	shard.mu.Unlock()
	if !existed {
		l.outstanding.Add(1)
	}
	l.updateHighestProduced(rec.Sequence)
	return nil
}

func (l *ledger) updateProducedLocation(id string, partition int, offset int64) bool {
	shard := l.shard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	rec, ok := shard.outstanding[id]
	if !ok {
		return false
	}
	rec.Partition = partition
	rec.Offset = offset
	shard.outstanding[id] = rec
	return true
}

func (l *ledger) deleteProduced(id string) bool {
	shard := l.shard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.outstanding[id]; !ok {
		return false
	}
	delete(shard.outstanding, id)
	l.outstanding.Add(-1)
	return true
}

func (l *ledger) markConsumed(msg testerMessage, topic string, _ time.Time) (consumeLedgerResult, error) {
	if msg.ID == "" || msg.Sequence <= 0 {
		return consumeLedgerResult{Outcome: consumeOutcomeUnknown}, nil
	}

	result := consumeLedgerResult{Outcome: consumeOutcomeUnknown}

	shardIndex := ledgerShardIndex(msg.ID)
	shard := &l.shards[shardIndex]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if rec, ok := shard.outstanding[msg.ID]; ok {
		if rec.Sequence != msg.Sequence {
			return result, nil
		}
		alreadyConsumed := l.markSequenceConsumedLocked(msg.Sequence)
		delete(shard.outstanding, msg.ID)
		l.outstanding.Add(-1)
		if alreadyConsumed {
			result.Outcome = consumeOutcomeDuplicate
			result.ProducedAtUnixMs = rec.ProducedAtUnixMs
			return result, nil
		}
		if rec.Topic != topic {
			return result, nil
		}
		result.Outcome = consumeOutcomeValid
		result.ProducedAtUnixMs = rec.ProducedAtUnixMs
		return result, nil
	}

	if l.sequenceConsumed(msg.Sequence) {
		result.Outcome = consumeOutcomeDuplicate
		result.ProducedAtUnixMs = msg.ProducedAtUnixMs
	}
	return result, nil
}

func (l *ledger) statsAndCompact(now time.Time, missingAfter time.Duration) ledgerStats {
	nowMs := now.UnixMilli()
	var stats ledgerStats
	for shardIndex := range l.shards {
		shard := &l.shards[shardIndex]
		shard.mu.Lock()

		stats.OutstandingRecords += int64(len(shard.outstanding))
		stats.ProducedOutstanding += int64(len(shard.outstanding))

		for _, rec := range shard.outstanding {
			ageSeconds := float64(nowMs-rec.ProducedAtUnixMs) / 1000
			if ageSeconds > stats.OldestProducedAge {
				stats.OldestProducedAge = ageSeconds
			}
			if nowMs-rec.ProducedAtUnixMs > missingAfter.Milliseconds() {
				stats.Missing++
			}
		}
		shard.mu.Unlock()
	}
	stats.ConsumedSequences = l.consumed.Load()
	return stats
}

func (l *ledger) outstandingCount() int64 {
	return l.outstanding.Load()
}

func (l *ledger) shard(id string) *ledgerShard {
	return &l.shards[ledgerShardIndex(id)]
}

func ledgerShardIndex(id string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(id); i++ {
		hash ^= uint32(id[i])
		hash *= prime32
	}
	return hash % ledgerShardCount
}

func sequenceShardIndex(sequence int64) uint32 {
	return uint32(sequence-1) % ledgerShardCount
}

func (l *ledger) markSequenceConsumedLocked(sequence int64) bool {
	shard := &l.sequences[sequenceShardIndex(sequence)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx := uint64(sequence-1) / ledgerShardCount
	wordIdx := int(idx / 64)
	bit := uint(idx % 64)
	for len(shard.words) <= wordIdx {
		shard.words = append(shard.words, 0)
	}
	mask := uint64(1) << bit
	if shard.words[wordIdx]&mask != 0 {
		return true
	}
	shard.words[wordIdx] |= mask
	l.consumed.Add(1)
	return false
}

func (l *ledger) sequenceConsumed(sequence int64) bool {
	if sequence <= 0 {
		return false
	}
	shard := &l.sequences[sequenceShardIndex(sequence)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	idx := uint64(sequence-1) / ledgerShardCount
	wordIdx := int(idx / 64)
	if wordIdx >= len(shard.words) {
		return false
	}
	bit := uint(idx % 64)
	return shard.words[wordIdx]&(uint64(1)<<bit) != 0
}

func (l *ledger) updateHighestProduced(sequence int64) {
	cur := l.highestProduced.Load()
	for sequence > cur && !l.highestProduced.CompareAndSwap(cur, sequence) {
		cur = l.highestProduced.Load()
	}
}
