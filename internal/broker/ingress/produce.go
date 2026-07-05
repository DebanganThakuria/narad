// Package ingress owns the WAL-first produce path: a produce request
// is durably accepted into this node's ingress WAL before a background
// dispatcher moves it to the partition owner and commits it to the
// partition log.
//
// The WAL sequence space is tracked by two watermarks:
//
//   - the durable next seq (DurableProduceNext): everything below it
//     has been fsynced into the WAL;
//   - the dispatch checkpoint (Load/StoreProduceCheckpoint): everything
//     below it has been committed to its partition log, so the WAL may
//     compact past it (CompactProduceBefore).
//
// A Manager is safe for concurrent use.
package ingress

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// ProduceRecord is one accepted produce request as stored in the
// ingress WAL. WAL identifies the record within the log; the remaining
// fields are the payload the dispatcher commits to the target
// partition.
type ProduceRecord struct {
	Topic           string
	Key             string
	TargetPartition int
	Payload         []byte
	CreatedAtUnixMs int64
	WAL             wal.RecordID
}

// AcceptedProduce is the receipt returned to a producer once its
// record is durable in the ingress WAL. It intentionally omits the
// payload — the caller already has it.
type AcceptedProduce struct {
	Topic           string
	TargetPartition int
	CreatedAtUnixMs int64
	WAL             wal.RecordID
}

// Manager owns this node's ingress produce WAL: appending accepted
// records, replaying them for dispatch, and maintaining the dispatch
// checkpoint used for compaction.
type Manager struct {
	produceDir  string
	log         *wal.Log
	durableNext atomic.Uint64
}

// DefaultWALOptions returns the WAL options used for the ingress
// produce log. Zero values defer to the wal package defaults.
func DefaultWALOptions() wal.Options {
	return wal.Options{}
}

// OpenManager opens (or creates) the ingress produce WAL under
// dataDir and recovers the durable next sequence from its records.
func OpenManager(dataDir string, opts wal.Options) (*Manager, error) {
	if dataDir == "" {
		return nil, errors.New("ingress: data dir required")
	}
	produceDir := filepath.Join(dataDir, "ingress", "produce")
	log, err := wal.Open(produceDir, opts)
	if err != nil {
		return nil, err
	}
	nextSeq, err := nextProduceSeq(produceDir)
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	// A checkpoint ahead of the recovered WAL means segments were removed
	// while the checkpoint survived; new records would get seqs below the
	// checkpoint and silently never be dispatched.
	checkpoint, err := loadCheckpoint(filepath.Join(produceDir, produceCheckpointFile))
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	if checkpoint > nextSeq {
		_ = log.Close()
		return nil, fmt.Errorf("ingress: produce checkpoint %d is ahead of recovered WAL next seq %d (missing WAL segments?)", checkpoint, nextSeq)
	}
	manager := &Manager{
		produceDir: produceDir,
		log:        log,
	}
	manager.durableNext.Store(nextSeq)
	return manager, nil
}

// AcceptProduce validates and durably appends one produce request to
// the ingress WAL, returning its receipt. The record is not yet
// visible to consumers — the dispatcher commits it to the partition
// log later.
func (m *Manager) AcceptProduce(ctx context.Context, topicName, key string, targetPartition int, payload []byte) (AcceptedProduce, error) {
	if m == nil || m.log == nil {
		return AcceptedProduce{}, errors.New("ingress: manager is nil")
	}
	if topicName == "" {
		return AcceptedProduce{}, errors.New("ingress: topic required")
	}
	if targetPartition < 0 {
		return AcceptedProduce{}, errors.New("ingress: target partition must be >= 0")
	}
	if len(payload) == 0 {
		return AcceptedProduce{}, errors.New("ingress: payload required")
	}

	record := ProduceRecord{
		Topic:           topicName,
		Key:             key,
		TargetPartition: targetPartition,
		Payload:         payload,
		CreatedAtUnixMs: time.Now().UTC().UnixMilli(),
	}
	encoded, err := EncodeProduceRecord(record)
	if err != nil {
		return AcceptedProduce{}, err
	}
	id, err := m.log.Append(ctx, encoded)
	if err != nil {
		return AcceptedProduce{}, err
	}
	m.advanceDurableNext(id.Seq + 1)
	return AcceptedProduce{
		Topic:           record.Topic,
		TargetPartition: record.TargetPartition,
		CreatedAtUnixMs: record.CreatedAtUnixMs,
		WAL:             id,
	}, nil
}

// DurableProduceNext returns the sequence one past the newest record
// known to be durable in the ingress WAL.
func (m *Manager) DurableProduceNext() uint64 {
	if m == nil {
		return 0
	}
	return m.durableNext.Load()
}

// ReplayProduce replays this node's ingress WAL from the given
// sequence. See the package-level ReplayProduce.
func (m *Manager) ReplayProduce(from uint64, fn func(ProduceRecord) error) error {
	if m == nil {
		return errors.New("ingress: manager is nil")
	}
	return ReplayProduce(m.produceDir, from, fn)
}

// ReplayProduceFromCursor replays this node's ingress WAL from an
// exact byte cursor. See the package-level ReplayProduceFromCursor.
func (m *Manager) ReplayProduceFromCursor(cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	if m == nil {
		return errors.New("ingress: manager is nil")
	}
	return ReplayProduceFromCursor(m.produceDir, cursor, fn)
}

// CompactProduceBefore drops WAL segments wholly below seq. Callers
// must only pass a persisted dispatch checkpoint — compacting past
// undispatched records loses them.
func (m *Manager) CompactProduceBefore(seq uint64) error {
	if m == nil || m.log == nil {
		return errors.New("ingress: manager is nil")
	}
	return m.log.CompactBefore(seq)
}

// LoadProduceCheckpoint reads the persisted dispatch checkpoint (the
// next sequence to dispatch). A missing checkpoint reads as 0.
func (m *Manager) LoadProduceCheckpoint() (uint64, error) {
	if m == nil {
		return 0, errors.New("ingress: manager is nil")
	}
	return loadCheckpoint(filepath.Join(m.produceDir, produceCheckpointFile))
}

// StoreProduceCheckpoint durably persists the dispatch checkpoint.
func (m *Manager) StoreProduceCheckpoint(nextSeq uint64) error {
	if m == nil {
		return errors.New("ingress: manager is nil")
	}
	return storeCheckpoint(m.produceDir, produceCheckpointFile, nextSeq)
}

// Close closes the underlying WAL. Safe on a nil manager.
func (m *Manager) Close() error {
	if m == nil || m.log == nil {
		return nil
	}
	return m.log.Close()
}

// advanceDurableNext lifts durableNext to next unless a concurrent
// append already advanced it further.
func (m *Manager) advanceDurableNext(next uint64) {
	for {
		current := m.durableNext.Load()
		if next <= current {
			return
		}
		if m.durableNext.CompareAndSwap(current, next) {
			return
		}
	}
}
