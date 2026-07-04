// Package storage implements the per-partition append-only record log.
//
// Architecture:
//
//   - Each partition is a directory of segment files. One *active*
//     segment receives writes; older segments are sealed (read-only).
//     The active segment is rolled when it crosses Options.SegmentBytes.
//
//   - Append/AppendBatch push records into an in-memory buffer and
//     return an offset immediately. There is no fsync on the produce
//     path.
//
//   - A single per-Log flusher goroutine drains the buffer to the
//     active segment in batches.
//
//   - A separate reaper goroutine deletes sealed segments past the
//     retention bound.
//
// Many goroutines may call Append/AppendBatch/Read concurrently. Only
// the flusher writes to the active segment file. Reads use positioned
// ReadAt and don't contend among themselves.
package storage

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

type indexEntry struct {
	segmentBaseOffset int64
	baseOffset        int64
	recordCount       int32
	framePos          int64
	frameLen          int32
}

type segmentIndex struct {
	entries  []indexEntry
	lastUsed uint64
}

const (
	maxHotSegmentIndexes = 2

	// segmentIndexStrideBytes is the default target spacing between
	// in-memory frame anchors. Reads scan forward from the nearest
	// anchor, so this keeps active index memory bounded without making
	// random reads walk an entire segment.
	segmentIndexStrideBytes = 32 << 10

	// targetMaxSegmentIndexEntries widens the stride for unusually
	// large segment settings. It is a target rather than a hard cap
	// because one final frame can legitimately roll past SegmentBytes.
	targetMaxSegmentIndexEntries = 2048
)

type Options struct {
	Codec codec.Codec

	FlushBytes      int
	FlushRecords    int
	FlushInterval   time.Duration
	SyncMode        SyncMode
	SyncInterval    time.Duration
	SyncBytes       int64
	HWMSyncInterval time.Duration

	SegmentBytes int64

	Retention RetentionConfig

	// Metrics is an optional observability plug. When nil, every
	// instrumented call site short-circuits to a noop.
	Metrics MetricsRecorder
}

func DefaultOptions() Options {
	c, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		panic(fmt.Sprintf("storage: default zstd codec: %v", err))
	}
	return Options{
		Codec:           c,
		FlushBytes:      1 << 20,
		FlushRecords:    1000,
		FlushInterval:   100 * time.Millisecond,
		SyncMode:        SyncBatched,
		SyncInterval:    time.Second,
		SyncBytes:       8 << 20,
		HWMSyncInterval: 5 * time.Second,
		SegmentBytes:    64 << 20,
	}
}

// SyncMode controls when the background flusher calls file.Sync().
type SyncMode string

const (
	// SyncPerWrite syncs every flushed batch. Appends are still buffered, so
	// this means "per storage batch", not "inside Produce".
	SyncPerWrite SyncMode = "per_write"

	// SyncBatched lets the flusher write many batches before syncing, bounded
	// by Options.SyncInterval, Options.SyncBytes, segment roll, and Close.
	SyncBatched SyncMode = "batched"
)

// Log is an append-only record log backed by a directory of segment
// files with an in-memory buffer in front of the disk.
type Log struct {
	dir   string
	codec codec.Codec
	opts  Options

	// rwmu serializes file writes and segment-list mutations against
	// the read path.
	rwmu sync.RWMutex

	// segments is sorted by baseOffset; segments[len-1] is active.
	segments []*segment

	segmentIndexes map[int64]*segmentIndex
	indexClock     uint64

	buffer        *buffer
	flushingMu    sync.Mutex
	flushingBase  int64
	flushingRec   [][]byte
	flushingValid bool
	highWatermark atomic.Int64
	durableTail   atomic.Int64
	persistedHWM  atomic.Int64
	hwmMu         sync.Mutex
	lastHWMSync   time.Time
	hwmPath       string
	hwmDirSynced  bool // guarded by hwmMu; set after the first hwm-file dir fsync
	flusher       *flusher
	reaper        *reaper

	// notify is the current broadcast channel for "new records may be
	// available" wake-ups. It is closed and replaced (under notifyMu)
	// on every notification so ALL waiters wake, not just one; waiters
	// must re-fetch via NotifyC before every wait.
	//
	// notifyWaiters records whether NotifyC handed out the current
	// channel since the last broadcast. When clear, notifyAll no-ops so
	// the append hot path pays zero allocations when nobody long-polls.
	// Sound because NotifyC and notifyAll serialize on notifyMu and
	// waiters snapshot the channel BEFORE their final data probe: a
	// broadcast ordered before NotifyC is not needed (the probe sees the
	// data), and one ordered after sees the flag set.
	notifyMu      sync.Mutex
	notify        chan struct{}
	notifyWaiters bool

	// appendGate makes Append/AppendBatch atomic with respect to Close:
	// appends hold the read side across the closed-check + buffer push,
	// and Close takes the write side after flipping closed and before
	// signalling the flusher's final drain. An append that observed
	// closed=false is therefore guaranteed to have its record in the
	// buffer before the final drain runs (acked ⇒ flushed), and an
	// append that loses the race observes closed=true and returns
	// ErrLogClosed.
	appendGate sync.RWMutex

	// Per-partition bounded LRU of decoded frames. Lets concurrent
	// consumers reading records of the same frame share one decode
	// instead of thrashing a single slot and re-decoding the (zstd)
	// frame on every read.
	frameCache *frameCache

	// Per-partition cache of recently-resolved frame positions. Lets a
	// sequential consume read resolve from the previous frame instead of
	// re-walking header-by-header from the sparse index anchor.
	navCache *navCache

	closed atomic.Bool
}

func NewLog(dir string, opts Options) (*Log, error) {
	if opts.Codec == nil {
		opts.Codec = codec.NewNoopCodec()
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 100 * time.Millisecond
	}
	if opts.SyncMode == "" {
		opts.SyncMode = SyncBatched
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = time.Second
	}
	if opts.SyncBytes < 0 {
		opts.SyncBytes = 0
	}
	if opts.HWMSyncInterval <= 0 {
		opts.HWMSyncInterval = 5 * time.Second
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}

	l := &Log{
		dir:            dir,
		codec:          opts.Codec,
		opts:           opts,
		segmentIndexes: make(map[int64]*segmentIndex),
		notify:         make(chan struct{}),
		hwmPath:        hwmFilePath(dir),
		frameCache:     newFrameCache(maxDecodeCacheFrames, maxDecodeCacheBytes),
		navCache:       newNavCache(maxNavCacheEntries),
	}
	l.highWatermark.Store(-1)

	nextOffset, err := l.recover()
	if err != nil {
		for _, s := range l.segments {
			_ = s.close()
		}
		return nil, err
	}

	l.buffer = newBuffer(nextOffset, opts.FlushBytes, opts.FlushRecords)
	if err := l.loadHighWatermark(nextOffset); err != nil {
		for _, s := range l.segments {
			_ = s.close()
		}
		return nil, err
	}
	l.durableTail.Store(nextOffset)
	l.lastHWMSync = time.Now()
	l.flusher = newFlusher(l, &l.rwmu, opts.FlushInterval)
	l.reaper = newReaper(l, opts.Retention)
	go l.flusher.run()
	go l.reaper.run()

	return l, nil
}

// NotifyC returns the current broadcast channel. It is CLOSED (never
// sent on) whenever new records may have become available — pushed
// into the buffer, flushed to disk, made visible by an HWM advance,
// or on Wake/Close — so every waiter blocked on it wakes at once.
// Because the channel is replaced after each broadcast, callers must
// fetch it BEFORE checking for data and re-fetch it before every
// subsequent wait.
func (l *Log) NotifyC() <-chan struct{} {
	l.notifyMu.Lock()
	l.notifyWaiters = true
	ch := l.notify
	l.notifyMu.Unlock()
	return ch
}

// notifyAll broadcasts to every goroutine blocked on the channel
// returned by NotifyC: it closes the current channel and installs a
// fresh one for the next round of waiters. When no one fetched the
// current channel (notifyWaiters clear) there is nothing to wake, so it
// returns without touching the channel — keeping Append/AppendBatch/
// AdvanceHighWatermark allocation-free in the no-waiter common case.
func (l *Log) notifyAll() {
	l.notifyMu.Lock()
	if l.notifyWaiters {
		close(l.notify)
		l.notify = make(chan struct{})
		l.notifyWaiters = false
	}
	l.notifyMu.Unlock()
}

// Wake broadcasts to long-poll waiters without any log-state change.
// Used when records become deliverable again for reasons the log
// cannot see — e.g. an in-flight reservation's visibility timeout
// expired. Safe to call concurrently and after Close.
func (l *Log) Wake() { l.notifyAll() }

func (l *Log) findSegmentLocked(baseOffset int64) *segment {
	for _, s := range l.segments {
		if s.baseOffset == baseOffset {
			return s
		}
	}
	return nil
}

func (l *Log) findSegmentForOffsetLocked(offset int64) *segment {
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].nextOffset > offset
	})
	if i >= len(l.segments) {
		return nil
	}
	s := l.segments[i]
	if offset < s.baseOffset {
		return nil
	}
	return s
}

func (l *Log) OldestOffset() int64 {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

func (l *Log) SizeBytes() int64 {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	var total int64
	for _, s := range l.segments {
		total += s.sizeBytes
	}
	return total
}

func (l *Log) SegmentCount() int {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	return len(l.segments)
}

// OldestSegmentAt returns the Unix-seconds mtime of the oldest
// segment file. ok=false when the partition has no segments yet or
// the file stat fails.
func (l *Log) OldestSegmentAt() (int64, bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	if len(l.segments) == 0 {
		return 0, false
	}
	mt, err := segmentMTime(l.segments[0])
	if err != nil {
		return 0, false
	}
	return mt, true
}

// SegmentMTimeForOffset returns the Unix-seconds mtime of the
// segment file that contains the given offset. ok=false when the
// offset is past the flushed tail (caller is caught up) or before
// LogStartOffset (data was retention-deleted).
//
// Note: per-message timestamps are not stored on disk. A segment's
// mtime is the time of its last write — within a segment, individual
// records may be older than the mtime by up to the segment's
// lifetime. Treat the returned time as an upper bound on "when the
// consumer's next message was last touched", not an exact produce
// timestamp.
func (l *Log) SegmentMTimeForOffset(offset int64) (int64, bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	for i := len(l.segments) - 1; i >= 0; i-- {
		s := l.segments[i]
		if offset >= s.baseOffset && offset < s.nextOffset {
			mt, err := segmentMTime(s)
			if err != nil {
				return 0, false
			}
			return mt, true
		}
	}
	return 0, false
}
