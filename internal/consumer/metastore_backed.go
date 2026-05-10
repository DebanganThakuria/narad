package consumer

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// MetastoreBacked delegates persistence to a metastore.Metastore. Reads
// translate "no recorded offset" into Next == 0 (start from the
// beginning), which is the at-least-once default.
type MetastoreBacked struct {
	store metastore.Metastore
}

// NewMetastoreBacked wires a tracker over the given metastore.
func NewMetastoreBacked(store metastore.Metastore) *MetastoreBacked {
	return &MetastoreBacked{store: store}
}

// Next returns the next offset to deliver: stored_committed_offset + 1,
// or 0 for a partition with no commits yet.
func (m *MetastoreBacked) Next(ctx context.Context, topic string, partition int) (int64, error) {
	off, err := m.store.GetConsumerOffset(ctx, topic, partition)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	// Stored offset is the last *committed* one — next to deliver is +1.
	return off + 1, nil
}

// Commit records `offset` as the last processed offset. Commits at or
// below the current high-water mark are silently dropped to keep the
// operation idempotent.
func (m *MetastoreBacked) Commit(ctx context.Context, topic string, partition int, offset int64) error {
	current, err := m.store.GetConsumerOffset(ctx, topic, partition)
	if err != nil && !errors.Is(err, metastore.ErrNotFound) {
		return err
	}
	if errors.Is(err, metastore.ErrNotFound) || offset > current {
		return m.store.SetConsumerOffset(ctx, topic, partition, offset)
	}
	return nil
}
