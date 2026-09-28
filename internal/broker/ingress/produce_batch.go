package ingress

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// BatchRecord is one record of AcceptProduceBatch: the key, target
// partition and payload AcceptProduceWithTopicID takes for one record.
type BatchRecord struct {
	Key             string
	TargetPartition int
	Payload         []byte
}

// AcceptProduceBatch validates every record and then durably appends
// them all to the ingress WAL in slice order, each stamped with topicID
// and one accept time. It returns once every record is durable, with a
// receipt per record in the same order. A record that fails validation
// fails the call before any record is appended.
//
// Slice order is WAL order, and the dispatcher commits a partition's
// records in WAL order, so records that share a key keep their order in
// the batch. The records are staged in one WAL append
// (wal.Log.AppendManyWith), so the batch waits for exactly one group
// commit however many records it holds, unless it fills the active
// segment and the WAL rolls in the middle of it. A failure (a latched
// write or sync failure, a segment roll that failed) fails the call and
// acks none of the batch, but records a roll had synced before it
// failed stay in the WAL and are dispatched, as with any failed sync:
// the caller reports the batch as failed, and a retry may duplicate
// them.
//
// ctx is checked once, before anything is appended. Past that point the
// batch is not abandoned half way for a client that went away.
func (m *Manager) AcceptProduceBatch(ctx context.Context, topicName, topicID string, records []BatchRecord) ([]AcceptedProduce, error) {
	if m == nil || m.log == nil {
		return nil, errors.New("ingress: manager is nil")
	}
	if topicName == "" {
		return nil, errors.New("ingress: topic required")
	}
	if len(records) == 0 {
		return nil, errors.New("ingress: batch has no records")
	}
	createdAt := time.Now().UTC().UnixMilli()
	full := make([]ProduceRecord, len(records))
	sizes := make([]int, len(records))
	for i, r := range records {
		full[i] = ProduceRecord{
			Topic:           topicName,
			TopicID:         topicID,
			Key:             r.Key,
			TargetPartition: r.TargetPartition,
			Payload:         r.Payload,
			CreatedAtUnixMs: createdAt,
		}
		if err := validateProduceRecord(full[i]); err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		sizes[i] = produceRecordSize(full[i])
	}

	// Each record is encoded straight into the WAL's group-commit
	// buffer, as AcceptProduceWithTopicID does for one.
	ids, err := m.log.AppendManyWith(ctx, sizes, func(i int, dst []byte) []byte {
		return appendProduceRecord(dst, full[i])
	})
	if err != nil {
		return nil, err
	}
	accepted := make([]AcceptedProduce, len(full))
	for i, id := range ids {
		accepted[i] = AcceptedProduce{
			Topic:           topicName,
			TargetPartition: full[i].TargetPartition,
			CreatedAtUnixMs: createdAt,
			WAL:             id,
		}
	}
	m.advanceDurableNext(ids[len(ids)-1].Seq + 1)
	return accepted, nil
}
