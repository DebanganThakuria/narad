package ingress

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

type ProduceRecord struct {
	Topic           string
	Key             string
	TargetPartition int
	Payload         []byte
	CreatedAtUnixMs int64
	WAL             wal.RecordID
}

type AcceptedProduce struct {
	Topic           string
	TargetPartition int
	CreatedAtUnixMs int64
	WAL             wal.RecordID
}

type Manager struct {
	produceDir  string
	log         *wal.Log
	durableNext atomic.Uint64
}

func DefaultWALOptions() wal.Options {
	return wal.Options{}
}

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
	manager := &Manager{
		produceDir: produceDir,
		log:        log,
	}
	manager.durableNext.Store(nextSeq)
	return manager, nil
}

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
	advanceDurableNext(&m.durableNext, id.Seq+1)
	return AcceptedProduce{
		Topic:           record.Topic,
		TargetPartition: record.TargetPartition,
		CreatedAtUnixMs: record.CreatedAtUnixMs,
		WAL:             id,
	}, nil
}

func (m *Manager) DurableProduceNext() uint64 {
	if m == nil {
		return 0
	}
	return m.durableNext.Load()
}

func (m *Manager) ReplayProduce(from uint64, fn func(ProduceRecord) error) error {
	if m == nil {
		return errors.New("ingress: manager is nil")
	}
	return ReplayProduce(m.produceDir, from, fn)
}

func (m *Manager) ReplayProduceFromCursor(cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	if m == nil {
		return errors.New("ingress: manager is nil")
	}
	return ReplayProduceFromCursor(m.produceDir, cursor, fn)
}

func (m *Manager) CompactProduceBefore(seq uint64) error {
	if m == nil || m.log == nil {
		return errors.New("ingress: manager is nil")
	}
	return m.log.CompactBefore(seq)
}

func (m *Manager) LoadProduceCheckpoint() (uint64, error) {
	if m == nil {
		return 0, errors.New("ingress: manager is nil")
	}
	return loadCheckpoint(filepath.Join(m.produceDir, produceCheckpointFile))
}

func (m *Manager) StoreProduceCheckpoint(nextSeq uint64) error {
	if m == nil {
		return errors.New("ingress: manager is nil")
	}
	return storeCheckpoint(m.produceDir, produceCheckpointFile, nextSeq)
}

func (m *Manager) Close() error {
	if m == nil || m.log == nil {
		return nil
	}
	return m.log.Close()
}

func advanceDurableNext(durableNext *atomic.Uint64, next uint64) {
	for {
		current := durableNext.Load()
		if next <= current {
			return
		}
		if durableNext.CompareAndSwap(current, next) {
			return
		}
	}
}
