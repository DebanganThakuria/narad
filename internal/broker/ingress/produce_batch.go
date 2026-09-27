package ingress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
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
// the batch. The records usually share one group commit, but the WAL
// has no multi-record append (see appendInOrder), so a flush can fall
// between two of them and the batch then waits for two. A failure after
// the first record was staged (a latched write or sync failure, a
// segment roll that failed) fails the call while the records staged
// before it may still be replayed, as with any failed sync: the caller
// reports the batch as failed, and a retry may duplicate them.
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
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ids, err := m.appendInOrder(context.WithoutCancel(ctx), full)
	if err != nil {
		return nil, err
	}
	accepted := make([]AcceptedProduce, len(full))
	var next uint64
	for i, id := range ids {
		accepted[i] = AcceptedProduce{
			Topic:           topicName,
			TargetPartition: full[i].TargetPartition,
			CreatedAtUnixMs: createdAt,
			WAL:             id,
		}
		next = max(next, id.Seq+1)
	}
	m.advanceDurableNext(next)
	return accepted, nil
}

// appendInOrder appends records to the WAL in slice order and waits
// until every one is durable, returning their ids.
//
// wal.Log appends one record per call, and AppendWith returns only once
// its record is durable, so appending the records one after another on
// one goroutine would wait out one fsync per record. Instead every
// record but the last is appended on a goroutine of its own, and each
// append starts only once the record before it has taken its sequence
// number (the fill callback runs under the WAL's lock as the record is
// staged), which keeps WAL order equal to slice order while the records
// wait for their group commits together. The last record is appended on
// the calling goroutine.
//
// An append that fails before staging its record stops the ones after
// it from being started; the call still waits for those already staged,
// then returns the first error.
func (m *Manager) appendInOrder(ctx context.Context, records []ProduceRecord) ([]wal.RecordID, error) {
	n := len(records)
	ids := make([]wal.RecordID, n)
	errs := make([]error, n)
	// staged carries one outcome per goroutine-appended record: true once
	// its fill ran (it has a sequence number), false when its append
	// failed first. Only one is ever outstanding, so the send never
	// blocks, not even the one made under the WAL's lock.
	var staged chan bool
	if n > 1 {
		staged = make(chan bool, 1)
	}
	var wg sync.WaitGroup
	started := n
	for i := range n - 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			filled := false
			ids[i], errs[i] = m.log.AppendWith(ctx, produceRecordSize(records[i]), func(dst []byte) []byte {
				filled = true
				staged <- true
				return appendProduceRecord(dst, records[i])
			})
			if !filled {
				staged <- false
			}
		}()
		if !<-staged {
			started = i + 1
			break
		}
	}
	if started == n {
		last := records[n-1]
		ids[n-1], errs[n-1] = m.log.AppendWith(ctx, produceRecordSize(last), func(dst []byte) []byte {
			return appendProduceRecord(dst, last)
		})
	}
	wg.Wait()
	for _, err := range errs[:started] {
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}
