package wal

import (
	"time"
)

const (
	defaultSegmentBytes       = 64 << 20
	defaultSyncInterval       = 10 * time.Millisecond
	defaultMaxRecord          = 16 << 20
	defaultCompactRotateBytes = 1 << 20
)

// Options configures a Log. Zero values pick sensible defaults.
type Options struct {
	// SegmentBytes caps a segment file's size; an append that would
	// exceed it rolls the log to a new segment.
	SegmentBytes int64

	// SyncInterval is the backstop timer for the sync loop. Every Append
	// wakes the loop immediately (group commit), so this only bounds how
	// long buffered records can wait if a wakeup is ever missed.
	SyncInterval time.Duration

	// MaxRecord is the maximum payload size accepted by Append and the
	// upper bound trusted when validating frame lengths during recovery.
	MaxRecord int

	// CompactRotateBytes is the minimum active-segment size at which
	// CompactBefore rolls a fully-compactable active segment so its file
	// can be reclaimed. Without rotation, an active segment whose records
	// are all below the compaction point is pinned on disk forever — it
	// only seals when NEW appends overflow it, which never happens once
	// its topics stop producing. <=0 uses the default (1 MiB); the floor
	// bounds how much fully-compacted data may linger rather than churning
	// a segment roll on every compaction under light traffic.
	CompactRotateBytes int64

	// Prealloc selects whether the log zero-fills its next segment in the
	// background so that appends overwrite blocks that are already
	// allocated and written. See SegmentPrealloc.
	Prealloc SegmentPrealloc

	// preparedWriteLimit bounds each write into a prepared segment; see
	// defaultPreparedWriteLimit. Tests shrink it to reach the limit with
	// small segments.
	preparedWriteLimit int64
}

// defaultPreparedWriteLimit is the most frame bytes a prepared segment
// ever has written but not yet synced. A group commit larger than this
// is written and synced in runs of whole frames of at most this many
// bytes (a single larger frame is a run of its own), because in a
// prepared segment a crash can tear the unsynced run into any mix of
// its new bytes and the zeros they overwrote, including valid frames
// after a hole. Recovery accepts that as a torn tail only within this
// many bytes of the hole (see tornPreparedWrite); anything farther
// stays a loud corruption failure. The limit is large so that the
// extra syncs never touch the produce path: splitting costs a device
// flush per run (with F_FULLFSYNC a 32 MiB batch in 4 MiB runs took
// about 1.5 times as long as in one), and a group commit of 16 MiB needs
// sixteen maximum-size produce bodies to arrive within one sync.
const defaultPreparedWriteLimit = 16 << 20

// SegmentPrealloc selects whether segments are prepared ahead of use.
//
// A segment that grows by appending changes the file's size and extents
// on every group commit, so on journaling file systems (ext4, XFS) each
// batch's fdatasync also commits the inode through the journal: an
// extra journal write and flush on the path every produce waits on. A
// prepared segment is created and zero-filled to SegmentBytes off the
// append path (in a file whose name replay and recovery ignore), and a
// roll renames it into place, so appends only overwrite and the sync is
// data-only. Readers treat an all-zero frame header as the end of the
// active segment's data, and the roll that seals a prepared segment
// trims it to its data first. Because a crash can tear an overwrite
// into a hole followed by valid frames, each write into a prepared
// segment is bounded and recovery knows the bound; see prealloc.go. The
// cost is one extra SegmentBytes of background writes per segment, up
// to two segments of preallocated disk (the active segment and a ready
// spare), and an extra sync per defaultPreparedWriteLimit bytes of a
// group commit larger than that.
type SegmentPrealloc int8

const (
	// PreallocAuto is the default and currently means off everywhere.
	// Preparation changes crash recovery (a tear inside the bounded write
	// window of a prepared segment is truncated rather than refused, and
	// a binary from before preparation refuses such a segment), and its
	// fdatasync win was measured on ext4 but not yet on the volumes Narad
	// runs on, so operators opt in with PreallocOn. APFS (macOS) showed
	// no difference between an extending and an overwriting data sync.
	PreallocAuto SegmentPrealloc = iota
	// PreallocOn always prepares segments.
	PreallocOn
	// PreallocOff never prepares segments: every segment starts empty and
	// grows by appending.
	PreallocOff
)

// preallocDefault is what PreallocAuto resolves to. Off until the win
// is measured on production volumes; see PreallocAuto.
var preallocDefault = false

// enabled reports whether p prepares segments on this platform.
func (p SegmentPrealloc) enabled() bool {
	switch p {
	case PreallocOn:
		return true
	case PreallocOff:
		return false
	}
	return preallocDefault
}

func normalizeOptions(opts Options) Options {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = defaultSegmentBytes
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = defaultSyncInterval
	}
	if opts.MaxRecord <= 0 {
		opts.MaxRecord = defaultMaxRecord
	}
	if opts.CompactRotateBytes <= 0 {
		opts.CompactRotateBytes = defaultCompactRotateBytes
	}
	if opts.preparedWriteLimit <= 0 {
		opts.preparedWriteLimit = defaultPreparedWriteLimit
	}
	return opts
}
