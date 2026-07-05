package storage

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
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

	buffer *buffer

	// The flushing snapshot holds records between buffer drain and disk
	// write so a failed write can be retried; see flushing.go.
	flushingMu      sync.Mutex
	flushingBase    int64
	flushingRecords [][]byte
	flushingValid   bool

	highWatermark atomic.Int64
	durableTail   atomic.Int64
	persistedHWM  atomic.Int64
	hwmMu         sync.Mutex
	lastHWMSync   time.Time
	hwmPath       string
	hwmDirSynced  bool // guarded by hwmMu; set after the first hwm-file dir fsync

	flusher *flusher
	reaper  *reaper

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

// NewLog opens (or creates) the partition log rooted at dir, recovers
// segment bounds and the persisted high-watermark, and starts the
// flusher and reaper goroutines. The caller must Close the returned Log.
func NewLog(dir string, opts Options) (*Log, error) {
	opts = opts.withDefaults()

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
		l.closeSegments()
		return nil, err
	}

	l.buffer = newBuffer(nextOffset, opts.FlushBytes, opts.FlushRecords)
	if err := l.loadHighWatermark(nextOffset); err != nil {
		l.closeSegments()
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
