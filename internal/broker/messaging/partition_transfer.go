package messaging

// Serve-side of partition rebalance: a node that owns a partition
// exposes its segments so a destination node can copy them verbatim.
// Reads the partition directory directly (immutable sealed segments +
// the growing active tail) — no Log reopen, safe under a concurrent
// writer, and works even if the log is idle-evicted.

import (
	"context"
	"fmt"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// PartitionTransferInfo is the source's view of a partition for the
// transfer protocol: its segments plus the durable positions a copy
// must reproduce.
type PartitionTransferInfo struct {
	Segments        []storage.SegmentInfo `json:"segments"`
	HighWatermark   int64                 `json:"high_watermark"`
	CommittedOffset int64                 `json:"committed_offset"`
	HasCommitted    bool                  `json:"has_committed"`
}

// PartitionTransferInfo lists a locally-owned partition's segments and
// durable positions for a destination to fetch. Errors with
// ErrNotPartitionOwner if this node does not own the partition — only
// the owner's copy is authoritative.
func (e *Engine) PartitionTransferInfo(ctx context.Context, topicName string, partition int) (PartitionTransferInfo, error) {
	if e.logs == nil {
		return PartitionTransferInfo{}, unavailableError("partition logs")
	}
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	if partition < 0 || partition >= t.Partitions {
		return PartitionTransferInfo{}, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if !e.isLocalOwner(topicName, partition) {
		return PartitionTransferInfo{}, ErrNotPartitionOwner
	}
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	segs, err := storage.ListPartitionSegments(dir)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	// The copy must expose every committed record. Records are fsynced
	// into the segment files on commit, but the durable HWM file is
	// batched and can lag the live HWM — so use the open log's current
	// HWM when it is open (Peek, never Get: observing must not resurrect
	// an idle-evicted log), falling back to the durable file (which
	// Close force-syncs) only when the log is closed.
	var hwm int64
	if log, open := e.logs.Peek(topicName, partition); open {
		hwm = log.HighWatermark()
	} else {
		persisted, _, perr := storage.ReadPersistedHighWatermark(dir)
		if perr != nil {
			return PartitionTransferInfo{}, perr
		}
		hwm = persisted
	}
	committed, hasCommitted, err := storage.ReadConsumerOffset(dir)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	return PartitionTransferInfo{
		Segments:        segs,
		HighWatermark:   hwm,
		CommittedOffset: committed,
		HasCommitted:    hasCommitted,
	}, nil
}

// ReadPartitionSegment returns up to length bytes at offset `at` of the
// segment with the given base offset in a locally-owned partition. A
// read past EOF returns the available bytes (the active segment grows
// under the writer); callers re-list to learn the final size.
func (e *Engine) ReadPartitionSegment(ctx context.Context, topicName string, partition int, baseOffset, at, length int64) ([]byte, error) {
	if e.logs == nil {
		return nil, unavailableError("partition logs")
	}
	if !e.isLocalOwner(topicName, partition) {
		return nil, ErrNotPartitionOwner
	}
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	return storage.ReadSegmentRange(dir, baseOffset, at, length)
}
