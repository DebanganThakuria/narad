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
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
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
	info, err := e.transferInfoAt(dir, topicName, partition, hwm)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	info.IncarnationID = t.ID
	return info, nil
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

// transferInfoAt assembles the transfer info for a partition directory
// at a caller-determined HWM: the segment listing, the consumer
// frontier, the fan-out cursor sidecars, and the move marker.
func (e *Engine) transferInfoAt(dir, topicName string, partition int, hwm int64) (PartitionTransferInfo, error) {
	segs, err := storage.ListPartitionSegments(dir)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	committed, hasCommitted, err := storage.ReadConsumerOffset(dir)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	// The persisted consumer.offset is flushed on a timer and lags the
	// in-memory frontier by up to one flush; the copy must carry the
	// frontier the consumers actually reached, or the new owner
	// redelivers the last acked messages.
	if e.offsets != nil {
		if mem, ok := e.offsets.CommittedOffset(topicName, partition); ok && (!hasCommitted || mem > committed) {
			committed, hasCommitted = mem, true
		}
	}
	sidecars, err := storage.ListFanoutCursorFiles(dir)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	info := PartitionTransferInfo{
		Segments:        segs,
		HighWatermark:   hwm,
		CommittedOffset: committed,
		HasCommitted:    hasCommitted,
		Sidecars:        sidecars,
	}
	if marker, ok, err := ReadMoveMarker(dir); err != nil {
		return PartitionTransferInfo{}, err
	} else if ok {
		info.MoveMarker = &marker
	}
	return info, nil
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
