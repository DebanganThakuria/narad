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
	// IncarnationID is the topic incarnation the source reported its
	// copy belongs to (empty from an older source). The runner compares
	// it with the local record before installing.
	IncarnationID string
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
	// synced records the staged segments already fdatasynced, at the
	// size they had then: a sealed segment is synced once its last chunk
	// is staged, outside the freeze, so makeDurable has little left to do
	// before the flip.
	synced map[int64]int64

	// Last state the source reported on a successful list. Retained so that
	// if the source later DIES, a force-promote can reproduce exactly the
	// visibility boundary (HWM) the source last exposed — never more, never
	// less. sawInfo guards against force-promoting a session that never
	// reached the source at all.
	lastHWM         int64
	lastCommitted   int64
	hasCommitted    bool
	sawInfo         bool
	lastIncarnation string
	// lastSidecars are the fan-out cursor files the source reported on
	// its last successful list. They are installed into the staged copy
	// at finalize time (after the last segment pass, so they are as
	// fresh as the source's cursors got) and by a force-promote, which
	// has nothing newer.
	lastSidecars []storage.SidecarFile
	// lastAhead is the acked-ahead set the source reported with its
	// last listing; written into the staged copy next to the frontier.
	lastAhead []int64
	// floorHWM is the highest source HWM an earlier session of the same
	// worker saw (see carryFrom): a force-promote never promotes a copy
	// that is behind it, even before this session reaches the source.
	floorHWM int64

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
		stagingDir: stagingDir, copied: map[int64]int64{}, synced: map[int64]int64{},
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
	s.lastAhead = info.AckedAhead
	s.lastIncarnation = info.IncarnationID
	var newBytes int64
	for _, seg := range info.Segments {
		at, seen := s.copied[seg.BaseOffset]
		if seen && seg.SizeBytes < at {
			// Only a source on an older release lists a segment shorter
			// than it listed before: its listing covered frames a commit
			// had not made visible, and a failed commit discarded them.
			// The staged bytes past the listed size are those frames, and
			// the records committed at their offsets since go after the
			// listed size, so they are cut before anything is appended.
			if err := storage.TruncateSegmentFile(s.stagingDir, seg.BaseOffset, seg.SizeBytes); err != nil {
				return 0, messaging.PartitionTransferInfo{}, fmt.Errorf("cut staged segment %d back to the source's %d bytes: %w", seg.BaseOffset, seg.SizeBytes, err)
			}
			s.m.logger.Warn("move: the source lists a segment shorter than the copy holds (it discarded records it never committed); cutting the staged segment back",
				"topic", s.topic, "partition", s.partition, "source", s.sourceAddr,
				"segment", seg.BaseOffset, "copied_bytes", at, "listed_bytes", seg.SizeBytes)
			at = seg.SizeBytes
			s.copied[seg.BaseOffset] = at
			delete(s.synced, seg.BaseOffset)
		}
		if seg.SizeBytes == 0 && !seen {
			// A retained log whose records have all aged out keeps one empty
			// segment whose file name carries the partition's base offset.
			// The chunk loop below never runs for it, so stage the empty
			// file explicitly: recovery of the staged copy takes its next
			// offset from that name, and without it an aged-out partition
			// recovers to offset 0 and the finalize verify can never reach
			// the source's high watermark (the move retries forever).
			if err := storage.WriteSegmentFile(s.stagingDir, seg.BaseOffset, nil); err != nil {
				return 0, messaging.PartitionTransferInfo{}, err
			}
			s.copied[seg.BaseOffset] = 0
			continue
		}
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
			// Never stage past what the listing reported: bytes written
			// since may be frames the source has not committed, which a
			// failed commit discards and other records replace.
			if int64(len(chunk)) > want {
				chunk = chunk[:want]
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
		if seg.Sealed && at > 0 && at == seg.SizeBytes && s.synced[seg.BaseOffset] != at {
			if err := storage.SyncSegmentFile(s.stagingDir, seg.BaseOffset); err != nil {
				return 0, messaging.PartitionTransferInfo{}, fmt.Errorf("sync staged segment %d: %w", seg.BaseOffset, err)
			}
			s.synced[seg.BaseOffset] = at
		}
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
	return s.finalizeStaged(last.HighWatermark, last.CommittedOffset, last.HasCommitted, last.AckedAhead, last.Sidecars)
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
//
// The HWM it promotes at is never below the floor an earlier session of
// the same worker carried (carryFrom), so starting the copy over after a
// flip that did not happen cannot weaken the gate.
func (s *MoveSession) ForcePromote() (CopyResult, error) {
	if !s.sawInfo {
		return CopyResult{}, fmt.Errorf("force-promote refused: source was never reached")
	}
	return s.finalizeStaged(s.promoteHWM(), s.lastCommitted, s.hasCommitted, s.lastAhead, s.lastSidecars)
}

// promoteHWM is the HWM a force-promote of this session promotes at: the
// source's last-known one, never below the carried floor.
func (s *MoveSession) promoteHWM() int64 {
	return max(s.lastHWM, s.floorHWM)
}

// carryFrom starts s with what an earlier session of the same worker
// knew about the source, when the staged copy had to be thrown away: the
// last listing's positions, and the highest HWM seen as the
// force-promote floor. The staged bytes are not carried (the staging
// directory starts empty), so the floor only ever makes a promote wait
// longer.
func (s *MoveSession) carryFrom(old *MoveSession) {
	if old == nil || !old.sawInfo {
		return
	}
	s.floorHWM = old.promoteHWM()
	s.lastHWM, s.lastCommitted, s.hasCommitted, s.sawInfo = old.lastHWM, old.lastCommitted, old.hasCommitted, true
	s.lastIncarnation, s.lastSidecars, s.lastAhead = old.lastIncarnation, old.lastSidecars, old.lastAhead
}

// finalizeStaged writes the target HWM, the consumer frontier and the
// fan-out cursors onto the staged copy, verifies it recovers into a log
// ending exactly at that HWM, and returns the result. Shared by Finalize
// (frozen live source) and ForcePromote (dead source). The verify is the
// data-safety gate: it fails if the staged copy does not reach hwm, and
// a copy past hwm is cut back to it first (verifyStaged).
//
// Every position is clamped to hwm first. A listing is consistent only
// when the source read its frontier before its boundary; a source that
// reads the boundary first (every release before this check) ships a
// frontier at or above it when a record is committed, delivered and
// acked between the two reads. The frozen Finalize never sees that (the
// freeze holds the boundary still, and the fence refuses one that
// moved), but ForcePromote works from whatever CatchUp listing came
// last, and the new owner then started with its frontier past its own
// log end: every record it later wrote below that frontier was
// committed, readable and never delivered. Clamped, the new owner
// redelivers what the source's consumers acked in that window instead.
func (s *MoveSession) finalizeStaged(hwm, committed int64, hasCommitted bool, ackedAhead []int64, sidecars []storage.SidecarFile) (CopyResult, error) {
	// An idle source has no segments, so no pass created the staging
	// directory; the copy is still a valid (empty) partition.
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return CopyResult{}, fmt.Errorf("create staging dir: %w", err)
	}
	if err := storage.WritePersistedHighWatermark(s.stagingDir, hwm); err != nil {
		return CopyResult{}, fmt.Errorf("write hwm: %w", err)
	}
	if hasCommitted {
		clamped, ahead := messaging.FrontierBelowBoundary(hwm, committed, ackedAhead)
		if clamped != committed || len(ahead) != len(ackedAhead) {
			s.m.logger.Warn("move: source frontier reached past the copy's high watermark; clamped (the new owner redelivers those acked records)",
				"topic", s.topic, "partition", s.partition, "source", s.sourceAddr,
				"hwm", hwm, "committed", committed, "clamped_to", clamped,
				"acked_ahead_dropped", len(ackedAhead)-len(ahead))
		}
		committed, ackedAhead = clamped, ahead
		if err := storage.WriteConsumerOffset(s.stagingDir, committed); err != nil {
			return CopyResult{}, fmt.Errorf("write consumer offset: %w", err)
		}
		// Written even with nothing acked ahead: this can run more than
		// once on one staging directory (a failed fence sends the move
		// back to catch-up, and a force-promote can follow), and the new
		// owner recovers the larger of the two files' frontiers, so an
		// earlier attempt's record must not outlive this one.
		if err := storage.WriteConsumerAhead(s.stagingDir, 0, uint64(time.Now().UnixNano()), committed, ackedAhead); err != nil {
			return CopyResult{}, fmt.Errorf("write consumer ahead: %w", err)
		}
	}
	// The fan-out cursors travel with the partition. Last, so they are as
	// fresh as the source's cursors got before the freeze took effect;
	// anything they fanned out between this copy and the flip is fanned
	// out again by the new owner (duplicates, never a skipped backlog).
	if err := installSidecars(s.stagingDir, sidecars, hwm); err != nil {
		return CopyResult{}, err
	}
	next, err := s.verifyStaged(hwm)
	if err != nil {
		return CopyResult{}, err
	}
	if next < hwm {
		return CopyResult{}, &copyBehindError{next: next, hwm: hwm}
	}
	if next > hwm {
		return CopyResult{}, fmt.Errorf("verify: staged copy next offset %d > source hwm %d (a frame straddles the high watermark)", next, hwm)
	}
	s.m.logger.Info("partition copy complete",
		"topic", s.topic, "partition", s.partition, "source", s.sourceAddr,
		"hwm", hwm, "bytes", s.total)
	return CopyResult{
		HighWatermark:   hwm,
		CommittedOffset: committed,
		HasCommitted:    hasCommitted,
		BytesCopied:     s.total,
		IncarnationID:   s.lastIncarnation,
	}, nil
}

// copyBehindError is a staged copy that recovers short of the high
// watermark it must reach: a force-promote whose copy is behind the
// source's last high watermark, or a frozen drain that did not get every
// record.
type copyBehindError struct{ next, hwm int64 }

func (e *copyBehindError) Error() string {
	return fmt.Sprintf("verify: staged copy next offset %d < source hwm %d", e.next, e.hwm)
}

// verifyStaged recovers the staged copy and returns the offset after its
// last record. A copy that recovers past hwm holds records the source
// never committed (only a source on an older release lists them): it is
// cut back to hwm (storage.CutStagedCopy) and recovered again. The
// session's cursors are then lowered to the staged files as they are,
// since recovery cuts a torn tail too, so the next pass appends where
// the staged bytes end.
func (s *MoveSession) verifyStaged(hwm int64) (int64, error) {
	recoverNext := func() (int64, error) {
		log, err := storage.NewLog(s.stagingDir, storage.Options{})
		if err != nil {
			return 0, fmt.Errorf("verify: recover staged copy: %w", err)
		}
		next := log.NextOffset()
		_ = log.Close()
		return next, nil
	}
	next, err := recoverNext()
	if err != nil {
		return 0, err
	}
	if next > hwm {
		cut, err := storage.CutStagedCopy(s.stagingDir, hwm)
		if err != nil {
			return 0, fmt.Errorf("verify: cut the staged copy back to source hwm %d: %w", hwm, err)
		}
		if cut {
			s.m.logger.Warn("move: the staged copy held records past the source's high watermark (a source on an older release listed records it never committed); cut back to it",
				"topic", s.topic, "partition", s.partition, "source", s.sourceAddr, "hwm", hwm, "staged_next", next)
			if next, err = recoverNext(); err != nil {
				return 0, err
			}
		}
	}
	if err := s.resyncCopied(); err != nil {
		return 0, fmt.Errorf("verify: %w", err)
	}
	return next, nil
}

// resyncCopied lowers the session's per-segment cursors to the staged
// files' sizes where those are smaller, and forgets the segments whose
// files are gone, so the next pass copies from where the staged bytes
// end.
func (s *MoveSession) resyncCopied() error {
	for base, at := range s.copied {
		size, ok, err := storage.SegmentFileSize(s.stagingDir, base)
		if err != nil {
			return fmt.Errorf("stat staged segment %d: %w", base, err)
		}
		switch {
		case !ok:
			delete(s.copied, base)
			delete(s.synced, base)
		case size < at:
			s.copied[base] = size
			delete(s.synced, base)
		}
	}
	return nil
}

// installSidecars writes the transferred fan-out cursor files into dir,
// each lowered to hwm when it points past it: like the consumer
// frontier, a cursor from a listing that read the boundary first can sit
// above it, and would skip the parent records the new owner writes
// there. Every name is validated by storage (a plain
// fanout-<child>.offset base name) so a transfer can never plant an
// arbitrary path.
func installSidecars(dir string, sidecars []storage.SidecarFile, hwm int64) error {
	for _, f := range sidecars {
		f, _, err := storage.FanoutCursorFileAtMost(f, hwm)
		if err != nil {
			return fmt.Errorf("install sidecar: %w", err)
		}
		if err := storage.InstallFanoutCursorFile(dir, f); err != nil {
			return fmt.Errorf("install sidecar: %w", err)
		}
	}
	return nil
}

// makeDurable fdatasyncs every staged file not already synced at its
// current size (sealed segments are synced as their last chunk lands),
// so the copy survives a power loss once ownership flips to it. A nil
// session syncs every file.
func (s *MoveSession) makeDurable(stagingDir string) error {
	return storage.SyncStagedFiles(stagingDir, func(name string, size int64) bool {
		if s == nil || size == 0 {
			return false
		}
		base, ok := storage.ParseSegmentFileName(name)
		return ok && s.synced[base] == size
	})
}

// Copy is Begin+Finalize for a static source (or tests) — it drains
// until stable. Against a live source use Begin/CatchUp/Finalize with a
// freeze between.
func (m *PartitionMover) Copy(ctx context.Context, sourceAddr, topicName string, partition int, stagingDir string) (CopyResult, error) {
	return m.Begin(sourceAddr, topicName, partition, stagingDir).Finalize(ctx)
}
