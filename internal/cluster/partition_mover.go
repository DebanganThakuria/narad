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
//   CatchUp  — freeze-free, lag-bounded. Copies the bulk (GBs of sealed
//              segments + the growing active tail) while produce flows
//              normally, returning once a pass copies at most lagBytes.
//              A big partition copies here with ZERO produce impact.
//   Finalize — the drain after the source is frozen (PrepareHandoff):
//              copies the last few records to a now-static tail,
//              reproduces the source's exact HWM + committed offset, and
//              verifies the staged copy recovers into a log reaching it.
//
// The reconcile loop does: Begin → CatchUp → PrepareHandoff (freeze,
// last moment) → Finalize → CompleteMove (flip). The freeze covers only
// Finalize, so it lasts milliseconds regardless of partition size.
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

// CatchUp copies the partition, produce still flowing, until a pass moves
// at most lagBytes — i.e. the destination is within lagBytes of the live
// tail. NO freeze here: this is where a GB partition copies for minutes
// with zero produce impact. lagBytes bounds how much the subsequent
// frozen Finalize must drain, so it controls the freeze duration.
func (s *MoveSession) CatchUp(ctx context.Context, lagBytes int64) error {
	if lagBytes < 0 {
		lagBytes = 0
	}
	for pass := 0; ; pass++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		newBytes, _, err := s.pass(ctx)
		if err != nil {
			return err
		}
		if pass > 0 && newBytes <= lagBytes {
			return nil // within the lag bound; ready to freeze + finalize
		}
		if newBytes == 0 {
			if !sleepCtx(ctx, 50*time.Millisecond) {
				return ctx.Err()
			}
		}
	}
}

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
