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

const (
	defaultProduceWALShards = 1
)

type ProduceRecord struct {
	MessageID       string
	Topic           string
	Key             string
	TargetPartition int
	Payload         []byte
	CreatedAtUnixMs int64
	WAL             wal.RecordID
	WALShard        int
}

type AcceptedProduce struct {
	MessageID       string
	Topic           string
	TargetPartition int
	CreatedAtUnixMs int64
	WAL             wal.RecordID
	WALShard        int
}

type Manager struct {
	produceDir       string
	shards           []*produceShard
	messageIDPrefix  string
	nextMessageIDSeq atomic.Uint64
}

type Options struct {
	WAL    wal.Options
	Shards int
}

type produceShard struct {
	id          int
	dir         string
	log         *wal.Log
	durableNext atomic.Uint64
}

func DefaultWALOptions(observers ...wal.StageObserver) wal.Options {
	opts := wal.Options{}
	if len(observers) > 0 {
		opts.Observer = observers[0]
		opts.ObserverComponent = "wal"
		opts.ObserverOperation = "ingress_produce"
	}
	return opts
}

func OpenManager(dataDir string, opts wal.Options) (*Manager, error) {
	return OpenManagerWithOptions(dataDir, Options{WAL: opts})
}

func OpenManagerWithOptions(dataDir string, opts Options) (*Manager, error) {
	if dataDir == "" {
		return nil, errors.New("ingress: data dir required")
	}
	produceDir := filepath.Join(dataDir, "ingress", "produce")
	shardCount := opts.Shards
	if shardCount <= 0 {
		shardCount = defaultProduceWALShards
	}
	shards := make([]*produceShard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		shardDir := produceShardDir(produceDir, i, shardCount)
		log, err := wal.Open(shardDir, opts.WAL)
		if err != nil {
			closeProduceShards(shards)
			return nil, err
		}
		nextSeq, err := nextProduceSeq(shardDir)
		if err != nil {
			_ = log.Close()
			closeProduceShards(shards)
			return nil, err
		}
		shard := &produceShard{id: i, dir: shardDir, log: log}
		shard.durableNext.Store(nextSeq)
		shards = append(shards, shard)
	}
	manager := &Manager{
		produceDir:      produceDir,
		shards:          shards,
		messageIDPrefix: newMessageIDPrefix(),
	}
	return manager, nil
}

func (m *Manager) AcceptProduce(ctx context.Context, topicName, key string, targetPartition int, payload []byte) (AcceptedProduce, error) {
	if m == nil || len(m.shards) == 0 {
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
		MessageID:       m.newMessageID(),
		Topic:           topicName,
		Key:             key,
		TargetPartition: targetPartition,
		Payload:         payload,
		CreatedAtUnixMs: time.Now().UTC().UnixMilli(),
	}
	shard := m.pickShard(record.TargetPartition)
	if shard == nil {
		return AcceptedProduce{}, errors.New("ingress: shard unavailable")
	}
	encoded, err := EncodeProduceRecord(record)
	if err != nil {
		return AcceptedProduce{}, err
	}
	id, err := shard.log.Append(ctx, encoded)
	if err != nil {
		return AcceptedProduce{}, err
	}
	advanceDurableNext(&shard.durableNext, id.Seq+1)
	return AcceptedProduce{
		MessageID:       record.MessageID,
		Topic:           record.Topic,
		TargetPartition: record.TargetPartition,
		CreatedAtUnixMs: record.CreatedAtUnixMs,
		WAL:             id,
		WALShard:        shard.id,
	}, nil
}

func (m *Manager) DurableProduceNext() uint64 {
	if m == nil || len(m.shards) == 0 {
		return 0
	}
	return m.shards[0].durableNext.Load()
}

func (m *Manager) DurableProduceNextForShard(shard int) (uint64, error) {
	s, err := m.shard(shard)
	if err != nil {
		return 0, err
	}
	return s.durableNext.Load(), nil
}

func (m *Manager) ShardCount() int {
	if m == nil {
		return 0
	}
	return len(m.shards)
}

func (m *Manager) ReplayProduce(from uint64, fn func(ProduceRecord) error) error {
	if m == nil || len(m.shards) == 0 {
		return errors.New("ingress: manager is nil")
	}
	for _, shard := range m.shards {
		if err := ReplayProduceShard(shard.dir, shard.id, from, fn); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) ReplayProduceFromCursor(cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	if m == nil || len(m.shards) == 0 {
		return errors.New("ingress: manager is nil")
	}
	return m.ReplayProduceShardFromCursor(0, cursor, fn)
}

func (m *Manager) ReplayProduceShardFromCursor(shard int, cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	s, err := m.shard(shard)
	if err != nil {
		return err
	}
	return ReplayProduceShardFromCursor(s.dir, s.id, cursor, fn)
}

func (m *Manager) CompactProduceBefore(seq uint64) error {
	if m == nil || len(m.shards) == 0 {
		return errors.New("ingress: manager is nil")
	}
	return m.CompactProduceShardBefore(0, seq)
}

func (m *Manager) CompactProduceShardBefore(shard int, seq uint64) error {
	s, err := m.shard(shard)
	if err != nil {
		return err
	}
	return s.log.CompactBefore(seq)
}

func (m *Manager) LoadProduceCheckpoint() (uint64, error) {
	return m.LoadProduceCheckpointForShard(0)
}

func (m *Manager) StoreProduceCheckpoint(nextSeq uint64) error {
	return m.StoreProduceCheckpointForShard(0, nextSeq)
}

func (m *Manager) LoadProduceCheckpointForShard(shard int) (uint64, error) {
	s, err := m.shard(shard)
	if err != nil {
		return 0, err
	}
	return loadCheckpoint(filepath.Join(s.dir, produceCheckpointFile))
}

func (m *Manager) StoreProduceCheckpointForShard(shard int, nextSeq uint64) error {
	s, err := m.shard(shard)
	if err != nil {
		return err
	}
	return storeCheckpoint(s.dir, produceCheckpointFile, nextSeq)
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	return closeProduceShards(m.shards)
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

func (m *Manager) pickShard(targetPartition int) *produceShard {
	if m == nil || len(m.shards) == 0 {
		return nil
	}
	if len(m.shards) == 1 {
		return m.shards[0]
	}
	return m.shards[targetPartition%len(m.shards)]
}

func (m *Manager) shard(id int) (*produceShard, error) {
	if m == nil || len(m.shards) == 0 {
		return nil, errors.New("ingress: manager is nil")
	}
	if id < 0 || id >= len(m.shards) {
		return nil, fmt.Errorf("ingress: shard %d out of range", id)
	}
	return m.shards[id], nil
}

func produceShardDir(produceDir string, shard, shardCount int) string {
	if shardCount == 1 || shard == 0 {
		return produceDir
	}
	return filepath.Join(produceDir, fmt.Sprintf("shard-%04d", shard))
}

func closeProduceShards(shards []*produceShard) error {
	var err error
	for _, shard := range shards {
		if shard == nil || shard.log == nil {
			continue
		}
		err = errors.Join(err, shard.log.Close())
	}
	return err
}
