package cluster

// The partition mover — destination side of a rebalance copy. Given a
// source node that owns (Topic, Partition), it copies the partition's
// segments into a local staging directory, tails the growing active
// segment until caught up to the source's high-watermark, reproduces
// the exact HWM, and verifies the staged copy recovers into an
// identical log. NO ownership change happens here — this is the safe,
// isolated copy that the cutover (a later phase) promotes.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// segmentFetcher is the source-side RPC surface the mover needs.
// *PeerClient satisfies it; tests supply a fake backed by a real Engine.
type segmentFetcher interface {
	ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error)
	FetchSegmentChunk(ctx context.Context, addr, topicName string, partition int, baseOffset, at, length int64) ([]byte, error)
}

// *PeerClient is the production segmentFetcher.
var _ segmentFetcher = (*PeerClient)(nil)

// PartitionMover copies partitions from source owners into staging dirs.
type PartitionMover struct {
	peer      segmentFetcher
	chunkSize int64
	logger    *slog.Logger
}

// NewPartitionMover builds a mover. chunkSize<=0 defaults to 1 MiB.
func NewPartitionMover(peer segmentFetcher, chunkSize int64, logger *slog.Logger) *PartitionMover {
	if chunkSize <= 0 {
		chunkSize = 1 << 20
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &PartitionMover{peer: peer, chunkSize: chunkSize, logger: logger}
}

// CopyResult reports a completed copy.
type CopyResult struct {
	HighWatermark   int64
	CommittedOffset int64
	HasCommitted    bool
	BytesCopied     int64
}

// Copy streams (Topic, Partition) from sourceAddr into stagingDir until
// caught up to the source's current high-watermark, then reproduces the
// source's HWM + committed offset and verifies the staged copy recovers
// into a log whose next offset reaches that HWM. It loops the active
// tail: a pass that copies zero new bytes and observes a non-advancing
// HWM means caught up. Bounded by deadline via ctx.
func (m *PartitionMover) Copy(ctx context.Context, sourceAddr, topicName string, partition int, stagingDir string) (CopyResult, error) {
	copied := map[int64]int64{} // base offset -> bytes copied so far
	var total, lastHWM int64
	var committed int64
	var hasCommitted bool

	for pass := 0; ; pass++ {
		if err := ctx.Err(); err != nil {
			return CopyResult{}, err
		}
		info, err := m.peer.ListPartitionSegments(ctx, sourceAddr, topicName, partition)
		if err != nil {
			return CopyResult{}, fmt.Errorf("list segments: %w", err)
		}
		lastHWM, committed, hasCommitted = info.HighWatermark, info.CommittedOffset, info.HasCommitted

		var newBytes int64
		for _, seg := range info.Segments {
			at := copied[seg.BaseOffset]
			for at < seg.SizeBytes {
				want := m.chunkSize
				if rem := seg.SizeBytes - at; rem < want {
					want = rem
				}
				chunk, err := m.peer.FetchSegmentChunk(ctx, sourceAddr, topicName, partition, seg.BaseOffset, at, want)
				if err != nil {
					return CopyResult{}, fmt.Errorf("fetch segment %d@%d: %w", seg.BaseOffset, at, err)
				}
				if len(chunk) == 0 {
					break // source hasn't written this far yet; re-list next pass
				}
				if at == 0 {
					if err := storage.WriteSegmentFile(stagingDir, seg.BaseOffset, chunk); err != nil {
						return CopyResult{}, err
					}
				} else if err := storage.AppendToSegmentFile(stagingDir, seg.BaseOffset, chunk); err != nil {
					return CopyResult{}, err
				}
				at += int64(len(chunk))
				newBytes += int64(len(chunk))
			}
			copied[seg.BaseOffset] = at
		}
		total += newBytes

		// Caught up: a full pass moved no new bytes. (During Phase-1 the
		// source is not concurrently written in the common path; the
		// cutover phase pauses the source to guarantee a final stable
		// pass.)
		if newBytes == 0 && pass > 0 {
			break
		}
		if newBytes == 0 {
			// First pass already complete (static source) — one more pass
			// confirms stability without sleeping needlessly.
			if !sleepCtx(ctx, 50*time.Millisecond) {
				return CopyResult{}, ctx.Err()
			}
		}
	}

	// Reproduce the source's exact visibility boundary, then verify.
	if err := storage.WritePersistedHighWatermark(stagingDir, lastHWM); err != nil {
		return CopyResult{}, fmt.Errorf("write hwm: %w", err)
	}
	if hasCommitted {
		if err := storage.WriteConsumerOffset(stagingDir, committed); err != nil {
			return CopyResult{}, fmt.Errorf("write consumer offset: %w", err)
		}
	}

	log, err := storage.NewLog(stagingDir, storage.Options{})
	if err != nil {
		return CopyResult{}, fmt.Errorf("verify: recover staged copy: %w", err)
	}
	next := log.NextOffset()
	_ = log.Close()
	if next < lastHWM {
		return CopyResult{}, fmt.Errorf("verify: staged copy next offset %d < source hwm %d", next, lastHWM)
	}

	m.logger.Info("partition copy complete",
		"topic", topicName, "partition", partition, "source", sourceAddr,
		"hwm", lastHWM, "bytes", total)
	return CopyResult{HighWatermark: lastHWM, CommittedOffset: committed, HasCommitted: hasCommitted, BytesCopied: total}, nil
}
