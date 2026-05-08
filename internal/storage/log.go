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
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

type indexEntry struct {
	segmentBaseOffset int64
	framePos          int64
	idx               int32
	frameLen          int32
}

type Options struct {
	Codec Codec

	FlushBytes    int
	FlushRecords  int
	FlushInterval time.Duration

	SegmentBytes int64

	Retention RetentionConfig

	// Metrics is an optional observability plug. When nil, every
	// instrumented call site short-circuits to a noop.
	Metrics MetricsRecorder
}

func DefaultOptions() Options {
	codec, err := NewZstdCodec(zstd.SpeedBestCompression)
	if err != nil {
		panic(fmt.Sprintf("storage: default zstd codec: %v", err))
	}
	return Options{
		Codec:         codec,
		FlushBytes:    1 << 20,
		FlushRecords:  1000,
		FlushInterval: 100 * time.Millisecond,
		SegmentBytes:  64 << 20,
	}
}

// Log is an append-only record log backed by a directory of segment
// files with an in-memory buffer in front of the disk.
type Log struct {
	dir   string
	codec Codec
	opts  Options

	// rwmu serializes file writes and segment-list mutations against
	// the read path.
	rwmu sync.RWMutex

	// segments is sorted by baseOffset; segments[len-1] is active.
	segments []*segment

	index map[int64]indexEntry

	buffer  *buffer
	flusher *flusher
	reaper  *reaper

	notify chan struct{}

	// Single-slot cache of the most recently decoded frame's
	// records. Hits make sequential consumer reads inside one batch
	// O(1) after the first.
	cacheMu          sync.Mutex
	cacheSegmentBase int64
	cachePos         int64
	cacheValid       bool
	cacheRec         [][]byte

	closed atomic.Bool
}

func NewLog(dir string, opts Options) (*Log, error) {
	if opts.Codec == nil {
		opts.Codec = NewNoopCodec()
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 100 * time.Millisecond
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}

	l := &Log{
		dir:    dir,
		codec:  opts.Codec,
		opts:   opts,
		index:  make(map[int64]indexEntry),
		notify: make(chan struct{}, 1),
	}

	nextOffset, err := l.recover()
	if err != nil {
		for _, s := range l.segments {
			_ = s.close()
		}
		return nil, err
	}

	l.buffer = newBuffer(nextOffset, opts.FlushBytes, opts.FlushRecords)
	l.flusher = newFlusher(l, &l.rwmu, opts.FlushInterval)
	l.reaper = newReaper(l, opts.Retention)
	go l.flusher.run()
	go l.reaper.run()

	return l, nil
}

// NotifyC fires whenever new records become available — pushed into
// the buffer or flushed to disk. Buffered (size 1); wake-ups
// coalesce.
func (l *Log) NotifyC() <-chan struct{} { return l.notify }

func (l *Log) findSegmentLocked(baseOffset int64) *segment {
	for _, s := range l.segments {
		if s.baseOffset == baseOffset {
			return s
		}
	}
	return nil
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

func (l *Log) OldestSegmentAt() (time.Time, bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	if len(l.segments) == 0 {
		return time.Time{}, false
	}
	mt, err := segmentMTime(l.segments[0])
	if err != nil {
		return time.Time{}, false
	}
	return mt, true
}

// SegmentMTimeForOffset returns the file mtime of the segment that
// contains the given offset. ok=false when the offset is past the
// flushed tail (caller is caught up) or before LogStartOffset (data
// was retention-deleted).
//
// Note: per-message timestamps are not stored on disk. A segment's
// mtime is the time of its last write — within a segment, individual
// records may be older than the mtime by up to the segment's
// lifetime. Treat the returned time as an upper bound on "when the
// consumer's next message was last touched", not an exact produce
// timestamp.
func (l *Log) SegmentMTimeForOffset(offset int64) (time.Time, bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	for i := len(l.segments) - 1; i >= 0; i-- {
		s := l.segments[i]
		if offset >= s.baseOffset && offset < s.nextOffset {
			mt, err := segmentMTime(s)
			if err != nil {
				return time.Time{}, false
			}
			return mt, true
		}
	}
	return time.Time{}, false
}
