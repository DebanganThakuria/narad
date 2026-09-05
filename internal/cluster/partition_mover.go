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
	"os"
	"sync"
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
//	CatchUp  — freeze-free, BOUNDED pre-copy. Copies the bulk (GBs of
//	           sealed segments + the growing active tail) while produce
//	           flows normally, iterating to shrink the un-copied tail. It
//	           returns converged=true once caught up (short freeze), or
//	           converged=false after a bounded number of passes if the
//	           writers keep pace with the copy (a longer stop-and-copy
//	           freeze). It NEVER loops forever.
//	Finalize — the drain after the source is frozen (PrepareHandoff):
//	           copies whatever tail remains to a now-static source,
//	           reproduces the source's exact HWM + committed offset, and
//	           verifies the staged copy recovers into a log reaching it.
//	           Always terminates — the freeze stopped the writes.
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

	// Last state the source reported on a successful list. Retained so that
	// if the source later DIES, a force-promote can reproduce exactly the
	// visibility boundary (HWM) the source last exposed — never more, never
	// less. sawInfo guards against force-promoting a session that never
	// reached the source at all.
	lastHWM       int64
	lastCommitted int64
	hasCommitted  bool
	sawInfo       bool
	// lastSidecars are the fan-out cursor files the source reported on
	// its last successful list. They are installed into the staged copy
	// at finalize time (after the last segment pass, so they are as
	// fresh as the source's cursors got) and by a force-promote, which
	// has nothing newer.
	lastSidecars []storage.SidecarFile

	// keepFrozen, when set, is called every keepFrozenEvery during
	// Finalize to re-arm the source's handoff freeze (whose TTL is
	// shorter than a slow drain can take). An error means the freeze
	// lapsed: commits may have landed on the source since, so Finalize
	// fails and the caller re-freezes and drains again.
	keepFrozen      func(context.Context) error
	keepFrozenEvery time.Duration
}

// KeepFrozen registers the re-arm hook Finalize calls every `every` to
// keep the source frozen while it drains. Without it (a static source,
// or tests) Finalize drains unfenced.
func (s *MoveSession) KeepFrozen(fn func(context.Context) error, every time.Duration) {
	s.keepFrozen, s.keepFrozenEvery = fn, every
	if every <= 0 {
		s.keepFrozenEvery = time.Second
	}
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
	// Record the source's last-known visibility boundary before copying, so a
	// force-promote after the source dies reproduces exactly this HWM.
	s.lastHWM, s.lastCommitted, s.hasCommitted, s.sawInfo = info.HighWatermark, info.CommittedOffset, info.HasCommitted, true
	s.lastSidecars = info.Sidecars
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

// Finalize drains the (now-frozen) source until two consecutive passes
// copy zero new bytes AND report the same HWM, reproduces the source's
// exact HWM + committed offset, installs the fan-out cursor sidecars
// from the last listing, and verifies the staged copy recovers into a
// log reaching that HWM. Call only AFTER the source is frozen
// (PrepareHandoff), or against a static source; otherwise it would
// chase a growing tail. While draining it re-arms the freeze through the
// KeepFrozen hook; a lapsed freeze fails the drain.
//
// The double-quiet rule guards against a commit that passed the freeze
// gate before the freeze and finished its fsync between two listings:
// one quiet pass could observe the segment before the append and the
// HWM after it. Two identical observations in a row, on a source whose
// freeze holds, cannot straddle a commit.
func (s *MoveSession) Finalize(ctx context.Context) (CopyResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The re-arm runs on its own ticker, not between passes: a single
	// pass can outlive the freeze TTL (a non-converged tail is drained
	// here), and the freeze must be extended while it does. A failed
	// re-arm cancels the drain; the error surfaces below.
	var rearmErr error
	var rearmMu sync.Mutex
	if s.keepFrozen != nil {
		done := make(chan struct{})
		defer func() { cancel(); <-done }()
		go func() {
			defer close(done)
			ticker := time.NewTicker(s.keepFrozenEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := s.keepFrozen(ctx); err != nil && ctx.Err() == nil {
						rearmMu.Lock()
						rearmErr = fmt.Errorf("re-arm handoff freeze: %w", err)
						rearmMu.Unlock()
						cancel()
						return
					}
				}
			}
		}()
	}
	lapsed := func() error {
		rearmMu.Lock()
		defer rearmMu.Unlock()
		return rearmErr
	}

	var last messaging.PartitionTransferInfo
	quiet := 0
	for pass := 0; ; pass++ {
		if err := ctx.Err(); err != nil {
			if lerr := lapsed(); lerr != nil {
				return CopyResult{}, lerr
			}
			return CopyResult{}, err
		}
		newBytes, info, err := s.pass(ctx)
		if err != nil {
			if lerr := lapsed(); lerr != nil {
				return CopyResult{}, lerr
			}
			return CopyResult{}, err
		}
		if newBytes == 0 && pass > 0 && info.HighWatermark == last.HighWatermark {
			quiet++
		} else {
			quiet = 0
		}
		last = info
		if quiet >= 1 {
			// This pass and the previous one both copied nothing and saw
			// the same HWM: the tail is static.
			break
		}
		if newBytes == 0 {
			if !sleepCtx(ctx, 50*time.Millisecond) {
				if lerr := lapsed(); lerr != nil {
					return CopyResult{}, lerr
				}
				return CopyResult{}, ctx.Err()
			}
		}
	}
	// The listings above are only final if the freeze held through the
	// last one.
	if lerr := lapsed(); lerr != nil {
		return CopyResult{}, lerr
	}
	return s.finalizeStaged(last.HighWatermark, last.CommittedOffset, last.HasCommitted, last.Sidecars)
}

// ForcePromote completes a move WITHOUT the source: it promotes whatever the
// destination already copied, reproducing the source's LAST-KNOWN high
// watermark. It is the recovery path for a source that dies mid-move — the
// source can no longer be frozen or drained, but a dead source is not writing
// either, so the copy the destination holds is the authoritative survivor.
//
// It is strictly gated for data safety:
//   - sawInfo: the session must have reached the source at least once (else
//     we have no idea what the source exposed — refuse);
//   - the staged copy must recover to an offset >= the last-known HWM, i.e.
//     the destination copied everything the source had made visible. If the
//     source died before the copy caught up, promoting would expose FEWER
//     records than were visible — data loss — so this refuses and the caller
//     keeps waiting for the source to return.
//
// Records the source committed in the window between the destination's last
// successful list and the source's death are unrecoverable — but they live
// only on the (now-dead) source's disk, so with single-owner partitions they
// are lost regardless. Force-promote recovers the maximum that is recoverable.
func (s *MoveSession) ForcePromote() (CopyResult, error) {
	if !s.sawInfo {
		return CopyResult{}, fmt.Errorf("force-promote refused: source was never reached")
	}
	return s.finalizeStaged(s.lastHWM, s.lastCommitted, s.hasCommitted, s.lastSidecars)
}

// finalizeStaged writes the target HWM + committed offset onto the staged
// copy, verifies it recovers into a log reaching that HWM, and returns the
// result. Shared by Finalize (frozen live source) and ForcePromote (dead
// source). The verify is the data-safety gate: it fails if the staged copy
// does not reach hwm.
func (s *MoveSession) finalizeStaged(hwm, committed int64, hasCommitted bool, sidecars []storage.SidecarFile) (CopyResult, error) {
	// An idle source has no segments, so no pass created the staging
	// directory; the copy is still a valid (empty) partition.
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return CopyResult{}, fmt.Errorf("create staging dir: %w", err)
	}
	if err := storage.WritePersistedHighWatermark(s.stagingDir, hwm); err != nil {
		return CopyResult{}, fmt.Errorf("write hwm: %w", err)
	}
	if hasCommitted {
		if err := storage.WriteConsumerOffset(s.stagingDir, committed); err != nil {
			return CopyResult{}, fmt.Errorf("write consumer offset: %w", err)
		}
	}
	// The fan-out cursors travel with the partition. Last, so they are as
	// fresh as the source's cursors got before the freeze took effect;
	// anything they fanned out between this copy and the flip is fanned
	// out again by the new owner (duplicates, never a skipped backlog).
	if err := installSidecars(s.stagingDir, sidecars); err != nil {
		return CopyResult{}, err
	}
	log, err := storage.NewLog(s.stagingDir, storage.Options{})
	if err != nil {
		return CopyResult{}, fmt.Errorf("verify: recover staged copy: %w", err)
	}
	next := log.NextOffset()
	_ = log.Close()
	if next < hwm {
		return CopyResult{}, fmt.Errorf("verify: staged copy next offset %d < source hwm %d", next, hwm)
	}
	s.m.logger.Info("partition copy complete",
		"topic", s.topic, "partition", s.partition, "source", s.sourceAddr,
		"hwm", hwm, "bytes", s.total)
	return CopyResult{HighWatermark: hwm, CommittedOffset: committed, HasCommitted: hasCommitted, BytesCopied: s.total}, nil
}

// installSidecars writes the transferred fan-out cursor files into dir.
// Every name is validated by storage (a plain fanout-<child>.offset base
// name) so a transfer can never plant an arbitrary path.
func installSidecars(dir string, sidecars []storage.SidecarFile) error {
	for _, f := range sidecars {
		if err := storage.InstallFanoutCursorFile(dir, f); err != nil {
			return fmt.Errorf("install sidecar: %w", err)
		}
	}
	return nil
}

// Copy is Begin+Finalize for a static source (or tests) — it drains
// until stable. Against a live source use Begin/CatchUp/Finalize with a
// freeze between.
func (m *PartitionMover) Copy(ctx context.Context, sourceAddr, topicName string, partition int, stagingDir string) (CopyResult, error) {
	return m.Begin(sourceAddr, topicName, partition, stagingDir).Finalize(ctx)
}
