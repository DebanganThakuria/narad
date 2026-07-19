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

// A MoveSession copies one partition from a source owner into a staging
// dir. It carries the per-segment copied-bytes state across two phases:
//
//   CatchUp  — freeze-free, BOUNDED pre-copy. Copies the bulk (GBs of
//              sealed segments + the growing active tail) while produce
//              flows normally, iterating to shrink the un-copied tail. It
//              returns converged=true once caught up (short freeze), or
//              converged=false after a bounded number of passes if the
//              writers keep pace with the copy (a longer stop-and-copy
//              freeze). It NEVER loops forever.
//   Finalize — the drain after the source is frozen (PrepareHandoff):
//              copies whatever tail remains to a now-static source,
//              reproduces the source's exact HWM + committed offset, and
//              verifies the staged copy recovers into a log reaching it.
//              Always terminates — the freeze stopped the writes.
//
// The reconcile loop does: Begin → CatchUp → PrepareHandoff (freeze,
// last moment) → Finalize → CompleteMove (flip). The freeze covers only
// Finalize; for a partition whose writes stay below the copy bandwidth it
// lasts milliseconds regardless of partition size. For a partition whose
// writers outrun the copy, CatchUp stops pre-copying and the freeze does a
// stop-and-copy of the remaining tail — a longer freeze, but the move
// always cuts over.
type MoveSession struct {
	m          *PartitionMover
	sourceAddr string
	topic      string
	partition  int
	stagingDir string
	copied     map[int64]int64 // base offset -> bytes copied so far
	total      int64
}

// Begin starts a copy session.
func (m *PartitionMover) Begin(sourceAddr, topicName string, partition int, stagingDir string) *MoveSession {
	return &MoveSession{
		m: m, sourceAddr: sourceAddr, topic: topicName, partition: partition,
		stagingDir: stagingDir, copied: map[int64]int64{},
	}
}

// pass lists the source and copies every reported-but-not-yet-copied
// byte once. Returns the bytes copied this pass and the source's current
// transfer info.
func (s *MoveSession) pass(ctx context.Context) (int64, messaging.PartitionTransferInfo, error) {
	info, err := s.m.peer.ListPartitionSegments(ctx, s.sourceAddr, s.topic, s.partition)
	if err != nil {
		return 0, messaging.PartitionTransferInfo{}, fmt.Errorf("list segments: %w", err)
	}
	var newBytes int64
	for _, seg := range info.Segments {
		at := s.copied[seg.BaseOffset]
		for at < seg.SizeBytes {
			want := s.m.chunkSize
			if rem := seg.SizeBytes - at; rem < want {
				want = rem
			}
			chunk, err := s.m.peer.FetchSegmentChunk(ctx, s.sourceAddr, s.topic, s.partition, seg.BaseOffset, at, want)
			if err != nil {
				return 0, messaging.PartitionTransferInfo{}, fmt.Errorf("fetch segment %d@%d: %w", seg.BaseOffset, at, err)
			}
			if len(chunk) == 0 {
				break // source hasn't written this far yet; re-list next pass
			}
			if at == 0 {
				if err := storage.WriteSegmentFile(s.stagingDir, seg.BaseOffset, chunk); err != nil {
					return 0, messaging.PartitionTransferInfo{}, err
				}
			} else if err := storage.AppendToSegmentFile(s.stagingDir, seg.BaseOffset, chunk); err != nil {
				return 0, messaging.PartitionTransferInfo{}, err
			}
			at += int64(len(chunk))
			newBytes += int64(len(chunk))
		}
		s.copied[seg.BaseOffset] = at
	}
	s.total += newBytes
	return newBytes, info, nil
}

// CatchUp copies the partition with produce still flowing, iterating to
// shrink the un-copied tail before the freeze. This is the pre-copy half of
// a pre-copy / stop-and-copy cutover (as in live VM migration): bounded
// pre-copy, then a guaranteed freeze. It stops — and the caller proceeds to
// freeze + Finalize — as soon as ANY of these holds:
//
//   - a pass copies <= lagBytes: the destination is caught up to the live
//     tail, so the frozen Finalize drains almost nothing (a short freeze).
//     converged=true.
//   - the tail stops shrinking for stallRounds passes, or maxRounds passes
//     elapse: the writers are keeping pace with (or outrunning) the copy, so
//     iterating further only lets the partition — and the eventual freeze —
//     grow. We stop NOW, at the smallest tail we achieved. converged=false.
//
// The move ALWAYS completes: the freeze (PrepareHandoff) stops the writes, so
// Finalize drains a fixed tail at full bandwidth no matter how hot the
// partition was. converged=false only means the freeze will be longer (it
// drains the tail we couldn't pre-copy) — never that the move hangs. When the
// copy can't win the race, stopping sooner is better, because the tail (hence
// the freeze) only grows while we keep trying.
func (s *MoveSession) CatchUp(ctx context.Context, lagBytes int64, maxRounds, stallRounds int) (converged bool, err error) {
	if lagBytes < 0 {
		lagBytes = 0
	}
	if maxRounds <= 0 {
		maxRounds = 1
	}
	if stallRounds <= 0 {
		stallRounds = 1
	}
	best := int64(-1) // smallest per-pass tail seen so far
	stalls := 0
	for pass := 0; ; pass++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		newBytes, _, err := s.pass(ctx)
		if err != nil {
			return false, err
		}
		// Pass 0 is the bulk copy; measure the shrinking tail from pass 1.
		if pass > 0 {
			if newBytes <= lagBytes {
				return true, nil // caught up — the freeze drains ~nothing
			}
			if best < 0 || newBytes < best {
				best, stalls = newBytes, 0
			} else {
				stalls++ // no progress toward the tail bound this pass
			}
			if stalls >= stallRounds || pass >= maxRounds {
				// Writers keep pace with the copy; stop pre-copying and let
				// the freeze do a stop-and-copy of the remaining tail.
				return false, nil
			}
		}
		if !sleepCtx(ctx, catchUpPassInterval) {
			return false, ctx.Err()
		}
	}
}

// catchUpPassInterval paces the pre-copy passes: long enough that a pass
// carries a meaningful chunk of writes (so the shrink/stall signal is
// stable), short enough that catch-up stays responsive.
const catchUpPassInterval = 50 * time.Millisecond

// Finalize drains the (now-frozen) source until a pass copies zero new
// bytes, reproduces the source's exact HWM + committed offset, and
// verifies the staged copy recovers into a log reaching that HWM. Call
// only AFTER the source is frozen (PrepareHandoff), or against a static
// source — otherwise it would chase a growing tail.
func (s *MoveSession) Finalize(ctx context.Context) (CopyResult, error) {
	var lastHWM, committed int64
	var hasCommitted bool
	for pass := 0; ; pass++ {
		if err := ctx.Err(); err != nil {
			return CopyResult{}, err
		}
		newBytes, info, err := s.pass(ctx)
		if err != nil {
			return CopyResult{}, err
		}
		lastHWM, committed, hasCommitted = info.HighWatermark, info.CommittedOffset, info.HasCommitted
		if newBytes == 0 && pass > 0 {
			break
		}
		if newBytes == 0 {
			if !sleepCtx(ctx, 50*time.Millisecond) {
				return CopyResult{}, ctx.Err()
			}
		}
	}

	if err := storage.WritePersistedHighWatermark(s.stagingDir, lastHWM); err != nil {
		return CopyResult{}, fmt.Errorf("write hwm: %w", err)
	}
	if hasCommitted {
		if err := storage.WriteConsumerOffset(s.stagingDir, committed); err != nil {
			return CopyResult{}, fmt.Errorf("write consumer offset: %w", err)
		}
	}
	log, err := storage.NewLog(s.stagingDir, storage.Options{})
	if err != nil {
		return CopyResult{}, fmt.Errorf("verify: recover staged copy: %w", err)
	}
	next := log.NextOffset()
	_ = log.Close()
	if next < lastHWM {
		return CopyResult{}, fmt.Errorf("verify: staged copy next offset %d < source hwm %d", next, lastHWM)
	}
	s.m.logger.Info("partition copy complete",
		"topic", s.topic, "partition", s.partition, "source", s.sourceAddr,
		"hwm", lastHWM, "bytes", s.total)
	return CopyResult{HighWatermark: lastHWM, CommittedOffset: committed, HasCommitted: hasCommitted, BytesCopied: s.total}, nil
}

// Copy is Begin+Finalize for a static source (or tests) — it drains
// until stable. Against a live source use Begin/CatchUp/Finalize with a
// freeze between.
func (m *PartitionMover) Copy(ctx context.Context, sourceAddr, topicName string, partition int, stagingDir string) (CopyResult, error) {
	return m.Begin(sourceAddr, topicName, partition, stagingDir).Finalize(ctx)
}
