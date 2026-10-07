package messaging

// Serve-side of partition rebalance: a node that owns a partition
// exposes its segments so a destination node can copy them verbatim.
// Reads the partition directory directly (immutable sealed segments +
// the growing active tail) — no Log reopen, safe under a concurrent
// writer, and works even if the log is idle-evicted.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// PartitionTransferInfo is the source's view of a partition for the
// transfer protocol: its segments plus the durable positions a copy
// must reproduce. The optional fields are omitted by older sources; a
// destination must treat their absence as "not reported", never as an
// error.
type PartitionTransferInfo struct {
	Segments        []storage.SegmentInfo `json:"segments"`
	HighWatermark   int64                 `json:"high_watermark"`
	CommittedOffset int64                 `json:"committed_offset"`
	HasCommitted    bool                  `json:"has_committed"`
	// AckedAhead are the offsets acked out of order above
	// CommittedOffset, from the source's live shard, so the new owner
	// does not redeliver them. An older source omits it and an older
	// destination ignores it: duplicates on that move, never loss.
	AckedAhead []int64 `json:"acked_ahead,omitempty"`
	// Sidecars are the fan-out cursor files living in the partition
	// directory (fanout-<child>.offset), verbatim. They must move with
	// the partition: the cursor runs on the parent partition's owner,
	// and a new owner that finds no cursor file tail-anchors, silently
	// skipping the child's backlog (the whole pending window of a delay
	// child). The source's cursors keep advancing until the flip, so
	// the destination copies these last; the overlap costs duplicates,
	// never loss.
	Sidecars []storage.SidecarFile `json:"sidecars,omitempty"`
	// FreezeToken identifies the handoff freeze PrepareHandoff armed.
	// The destination presents it on every re-arm and on the fence
	// before the flip; the source refuses a token whose freeze lapsed,
	// so a cutover that outlived the freeze TTL can never flip over
	// records the source accepted after the freeze silently expired.
	FreezeToken string `json:"freeze_token,omitempty"`
	// MoveMarker is the marker the move that installed this copy left in
	// the partition directory, if any. The old owner's stale-copy sweep
	// compares its local copy against MoveMarker.HighWatermark before
	// deleting it: a local copy that is ahead of the promoted position
	// holds records nobody else has.
	MoveMarker *MoveMarker `json:"move_marker,omitempty"`
	// IncarnationID is the ID of the topic incarnation the source's
	// copy belongs to (its metastore record's topic.Topic.ID). The
	// destination compares it with its own record before installing
	// the copy, so a partition of a deleted incarnation is never moved
	// into the recreated topic. Empty from an older source, or for a
	// record without an ID.
	IncarnationID string `json:"incarnation_id,omitempty"`
	// ListedAtUnixNano is the source's clock when it listed the
	// segments. With each segment's ModTimeUnixNano it gives the
	// segment's age, which the destination stamps on its copy on its own
	// clock, so clock skew between the two nodes does not move the
	// retention clock. Zero from an older source.
	ListedAtUnixNano int64 `json:"listed_at_unix_nano,omitempty"`
}

// MoveMarkerFileName is the marker a move writes into the partition
// directory it installs, next to the segments. It is not a segment, not
// a sidecar (it is never copied onward; the next move writes its own),
// and never read by the log.
const MoveMarkerFileName = "move.marker"

// MoveMarker records how a partition copy came to live in this
// directory: which move installed it and at which HWM the copy was
// promoted. Children snapshots the parent's live child links at install
// time so a fan-out cursor that finds no cursor file after the install
// can tell a lost cursor (link existed at install: resume from the
// oldest retained offset, duplicates over loss) from a fresh attach
// (tail-anchor, the no-backfill contract).
type MoveMarker struct {
	Source            string `json:"source"`
	HighWatermark     int64  `json:"high_watermark"`
	ForcePromoted     bool   `json:"force_promoted,omitempty"`
	InstalledAtUnixMs int64  `json:"installed_at_unix_ms"`
	// DurableAtUnixMs is when the destination finished syncing the copy
	// (every staged file and the staging directory) before it installed
	// it and proposed the flip. Zero from a release that did not sync
	// the copy: its segments may still sit in the new owner's page cache
	// shortly after the install, so the old owner's sweep waits before
	// trusting that owner's listing.
	DurableAtUnixMs int64 `json:"durable_at_unix_ms,omitempty"`
	// Children maps each child topic attached to this parent at install
	// time to that link's attach epoch.
	Children map[string]string `json:"children,omitempty"`
}

// ReadMoveMarker loads the move marker from a partition directory.
// ok=false (with nil error) when none exists.
func ReadMoveMarker(partitionDir string) (MoveMarker, bool, error) {
	buf, err := os.ReadFile(filepath.Join(partitionDir, MoveMarkerFileName))
	if errors.Is(err, os.ErrNotExist) {
		return MoveMarker{}, false, nil
	}
	if err != nil {
		return MoveMarker{}, false, err
	}
	var m MoveMarker
	if err := json.Unmarshal(buf, &m); err != nil {
		return MoveMarker{}, false, fmt.Errorf("messaging: move marker corrupt: %w", err)
	}
	return m, true, nil
}

// WriteMoveMarker atomically persists the move marker into a partition
// (or staging) directory, replacing any earlier one.
func WriteMoveMarker(partitionDir string, m MoveMarker) error {
	buf, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(partitionDir, MoveMarkerFileName+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(buf); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(partitionDir, MoveMarkerFileName))
}

// PartitionTransferInfo lists a locally-owned partition's segments and
// durable positions for a destination to fetch. Errors with
// ErrNotPartitionOwner if this node does not own the partition; only
// the owner's copy is authoritative.
func (e *Engine) PartitionTransferInfo(ctx context.Context, topicName string, partition int) (PartitionTransferInfo, error) {
	t, err := e.checkTransferable(ctx, topicName, partition)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	// The listing reads the directory without opening the log, so the
	// open path's incarnation check does not run: do it here, or a
	// deleted incarnation's segments would be shipped to a new owner.
	if ok, err := e.logs.TopicIncarnationMatches(topicName, t.ID); err != nil {
		return PartitionTransferInfo{}, err
	} else if !ok {
		return PartitionTransferInfo{}, fmt.Errorf("%w: %s", runtime.ErrStaleTopicIncarnation, topicName)
	}
	dir, err := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	info, err := e.transferInfoAt(dir, topicName, partition, func() (int64, error) {
		return e.transferHighWatermark(dir, topicName, partition)
	}, func(base, hwm int64) (int64, bool) {
		if log, open := e.logs.Peek(topicName, partition); open {
			return log.CommittedBoundary(base, hwm)
		}
		return 0, false
	})
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	info.IncarnationID = t.ID
	return info, nil
}

// transferHighWatermark is the visibility boundary a listing of an
// unfrozen partition ships. The copy must expose every committed
// record, and the hwm file holds the boundary only while the log is
// closed (Close writes it; an open log empties it before its first
// advance). So the open log's live HWM is used when it is open (Peek,
// never Get: observing must not resurrect an idle-evicted log, whose
// file holds the exact boundary anyway), and the file when it is closed.
//
// A closed log whose file holds no boundary is either one that opened
// between the Peek and the read (asked again), a partition that never
// exposed a record, or a crash image nothing has opened yet: a
// restarted owner serves cluster RPCs before startup opens its
// partitions. Shipping 0 for a crash image put every listed frontier
// above the boundary, and made a force-promote's gate (staged next
// offset >= boundary) accept a copy of any length, hiding records the
// source had made visible. Only an open recovers that boundary (the
// CRC-verified record tail), and this node owns the partition, so the
// log is opened here as startup would.
func (e *Engine) transferHighWatermark(dir, topicName string, partition int) (int64, error) {
	if log, open := e.logs.Peek(topicName, partition); open {
		return log.HighWatermark(), nil
	}
	persisted, ok, err := storage.ReadPersistedHighWatermark(dir)
	if err != nil {
		return 0, err
	}
	if ok {
		return persisted, nil
	}
	if log, open := e.logs.Peek(topicName, partition); open {
		return log.HighWatermark(), nil
	}
	segs, err := storage.ListPartitionSegments(dir)
	if err != nil {
		return 0, err
	}
	// With no record bytes the answer is exact without an open: the
	// offset the newest segment is named for, which is where recovery
	// puts the tail (0 when the partition has no segment at all).
	var tail int64
	for _, seg := range segs {
		if seg.SizeBytes > 0 {
			log, err := e.logs.Get(topicName, partition)
			if err != nil {
				return 0, fmt.Errorf("open partition for its high watermark: %w", err)
			}
			return log.HighWatermark(), nil
		}
		tail = max(tail, seg.BaseOffset)
	}
	return tail, nil
}

// checkTransferable validates the transfer target: topic exists,
// partition in range, and this node owns it. Returns the topic record.
func (e *Engine) checkTransferable(ctx context.Context, topicName string, partition int) (topic.Topic, error) {
	if e.logs == nil {
		return topic.Topic{}, unavailableError("partition logs")
	}
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return topic.Topic{}, err
	}
	if partition < 0 || partition >= t.Partitions {
		return topic.Topic{}, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if !e.isLocalOwner(topicName, partition) {
		return topic.Topic{}, ErrNotPartitionOwner
	}
	return t, nil
}

// EnsureTopicIncarnation prepares the topic directory for the
// incarnation id before a moved partition is installed into it: an
// unmarked directory is adopted, a directory of another incarnation is
// quarantined, and the marker is stamped. See runtime.Logs.
func (e *Engine) EnsureTopicIncarnation(topicName, id string) error {
	if e.logs == nil {
		return unavailableError("partition logs")
	}
	return e.logs.EnsureTopicIncarnation(topicName, id)
}

// transferInfoAt assembles the transfer info for a partition directory:
// the consumer frontier, the fan-out cursor sidecars, the high watermark
// hwmAt reports, the segment listing, and the move marker.
//
// The listing covers only the records below that high watermark
// (storage.ListCommittedSegments): the active segment's file also holds
// frames a commit wrote and has not made visible, and the hidden tail a
// failed commit leaves, and a failed commit hands their offsets to other
// records. A copy that took them kept records the source never
// committed. boundary reports a segment's committed boundary from the
// open log (Log.CommittedBoundary); when it does not know the segment
// (the log is closed), the segment file's frames are walked.
//
// The order makes one listing consistent. The frontier and the fan-out
// cursors only ever cover visible records, so read before the boundary
// they sit below it (the frontier) or at most at it (a cursor's next
// offset); the boundary is read before the segments, which only grow,
// so the listed bytes cover it. The other way round (the boundary
// first, the frontier last), a record committed, delivered and acked
// between the reads shipped a frontier at or above the boundary, and a
// force-promote working from that listing gave the new owner a frontier
// past its log end: the records it later wrote below that frontier were
// never delivered. The clamp below keeps the invariant even for a
// frontier this node recovered from a file that was already past it.
func (e *Engine) transferInfoAt(dir, topicName string, partition int, hwmAt func() (int64, error), boundary func(base, hwm int64) (int64, bool)) (PartitionTransferInfo, error) {
	committed, hasCommitted, ackedAhead, err := e.consumerFrontier(dir, topicName, partition)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	sidecars, err := storage.ListFanoutCursorFiles(dir)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	hwm, err := hwmAt()
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	segs, err := storage.ListCommittedSegments(dir, hwm, func(base int64) (int64, bool) {
		return boundary(base, hwm)
	})
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	// The segment times are file times, so the listing time is the wall
	// clock too.
	listedAt := time.Now().UnixNano()
	if hasCommitted {
		committed, ackedAhead = FrontierBelowBoundary(hwm, committed, ackedAhead)
	}
	info := PartitionTransferInfo{
		Segments:         segs,
		HighWatermark:    hwm,
		CommittedOffset:  committed,
		HasCommitted:     hasCommitted,
		AckedAhead:       ackedAhead,
		Sidecars:         sidecars,
		ListedAtUnixNano: listedAt,
	}
	if marker, ok, err := ReadMoveMarker(dir); err != nil {
		return PartitionTransferInfo{}, err
	} else if ok {
		info.MoveMarker = &marker
	}
	return info, nil
}

// consumerFrontier reads the partition's consumer frontier for a
// transfer: the committed offset and the offsets acked ahead of it.
//
// The persisted consumer.offset is flushed on a timer and lags the
// in-memory frontier by up to one flush; the copy must carry the
// frontier the consumers actually reached, or the new owner redelivers
// the last acked messages. The acked-ahead set comes from the live shard
// when this node has one, and from consumer.ahead on disk otherwise (a
// restarted owner nobody consumed from yet): either way the copy carries
// every ack.
func (e *Engine) consumerFrontier(dir, topicName string, partition int) (committed int64, hasCommitted bool, ackedAhead []int64, err error) {
	committed, hasCommitted, err = storage.ReadConsumerOffset(dir)
	if err != nil {
		return 0, false, nil, err
	}
	if e.offsets != nil {
		if mem, offsets, _, ok := e.offsets.AheadSnapshot(topicName, partition); ok {
			if !hasCommitted || mem > committed {
				committed, hasCommitted = mem, true
			}
			return committed, hasCommitted, offsets, nil
		}
	}
	rec, ok, err := storage.ReadConsumerAhead(dir)
	if err != nil {
		return 0, false, nil, err
	}
	if ok {
		if !hasCommitted || rec.Committed > committed {
			committed, hasCommitted = rec.Committed, true
		}
		ackedAhead = rec.Offsets
	}
	return committed, hasCommitted, ackedAhead, nil
}

// FrontierBelowBoundary clamps a consumer frontier to a partition's
// visibility boundary hwm: the committed offset to at most hwm-1, and
// the acked-ahead set to the offsets strictly between the result and
// hwm. It returns a new slice when it drops anything and never modifies
// ackedAhead.
//
// A copy promoted at hwm holds no visible record at or above it, and a
// first commit that fails there discards the hidden tail and hands its
// offsets to new records. A frontier at or above hwm would make the new
// owner skip whatever it writes there: records committed and readable,
// never delivered. Clamping only ever redelivers records the source's
// consumers had acked, never skips one.
func FrontierBelowBoundary(hwm, committed int64, ackedAhead []int64) (int64, []int64) {
	committed = min(committed, hwm-1)
	outside := func(off int64) bool { return off <= committed || off >= hwm }
	if !slices.ContainsFunc(ackedAhead, outside) {
		return committed, ackedAhead
	}
	return committed, slices.DeleteFunc(slices.Clone(ackedAhead), outside)
}

// ReadPartitionSegment returns up to length bytes at offset `at` of the
// segment with the given base offset in a locally-owned partition. A
// read past EOF returns the available bytes (the active segment grows
// under the writer); callers re-list to learn the final size.
//
// While the log is open, a read never goes past the segment's committed
// boundary, as the listing reports it: the bytes past it can be
// truncated and rewritten with other records. A closed log's file is
// read as it is: nothing rewrites it until the log opens, and a copy
// asks only for the bytes a listing reported.
func (e *Engine) ReadPartitionSegment(ctx context.Context, topicName string, partition int, baseOffset, at, length int64) ([]byte, error) {
	if e.logs == nil {
		return nil, unavailableError("partition logs")
	}
	if !e.isLocalOwner(topicName, partition) {
		return nil, ErrNotPartitionOwner
	}
	if log, open := e.logs.Peek(topicName, partition); open {
		if pos, ok := log.CommittedBoundary(baseOffset, log.HighWatermark()); ok {
			if at >= pos {
				return []byte{}, nil
			}
			length = min(length, pos-at)
		}
	}
	dir, err := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	if err != nil {
		return nil, err
	}
	return storage.ReadSegmentRange(dir, baseOffset, at, length)
}
