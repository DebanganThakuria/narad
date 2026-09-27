// Package wal provides a small segmented write-ahead log with grouped fsync.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Log is a segmented write-ahead log. Concurrent appends are staged into
// a shared buffer and flushed by a background sync loop with a single
// write+fsync per batch; each Append blocks until its batch is durable.
//
// Locking: mu guards the append state (buffer, seq counters, pending
// batch). fileOps guards the active file handle across write/fsync and
// segment rolls. Methods with a Locked suffix require mu to be held.
type Log struct {
	dir  string
	opts Options

	mu          sync.Mutex
	fileOps     sync.Mutex
	file        *os.File
	segmentBase uint64
	segmentSize int64
	nextSeq     uint64
	writeBuffer []byte
	// spare (guarded by mu) is the last written-out buffer, kept empty
	// for the next batch. flushSync swaps it in as writeBuffer when it
	// detaches a batch, so records staged while that batch's write and
	// sync are in flight land in a buffer already sized to the previous
	// batch instead of regrowing from a single frame. See recycleBuffer.
	spare   []byte
	pending *syncBatch
	closed  bool
	syncErr error
	// writeFailed is guarded by fileOps, not mu. It latches the first
	// write or fsync failure on the active file so that no later batch
	// is written on top of a possibly torn region and acked.
	writeFailed error

	// compactFloor (guarded by mu) is the smallest seq at which
	// CompactBefore could delete a sealed segment, as recorded by its
	// last directory listing; 0 means unknown (list on the next call).
	// See CompactBefore.
	compactFloor uint64

	// durableSize (guarded by mu) is how many bytes of the active
	// segment are known written and synced. Log.ReplayFromCursor reads
	// the active segment only that far, so it never parses a frame that
	// is still being written, nor the zero-filled space after the data
	// of a prepared segment.
	durableSize int64

	// Segment preparation (see SegmentPrealloc), all guarded by mu.
	// activePrepared marks an active segment that was prepared (it has a
	// zero tail up to SegmentBytes, trimmed by the roll that seals it);
	// prepRequested is set once the active segment passes half its size
	// and cleared by the next roll; spareReady marks a fully prepared
	// file waiting under prepFileName. prepWake and prepDone are nil when
	// preparation is off.
	prealloc       bool
	activePrepared bool
	prepRequested  bool
	spareReady     bool
	prepWake       chan struct{}
	prepDone       chan struct{}

	wakeup chan struct{}
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

// syncBatch is the completion handle shared by every append staged into
// one write+fsync. done is closed once the batch outcome is in err.
type syncBatch struct {
	done chan struct{}
	err  error
}

// Open opens (or creates) the log in dir, recovers the next sequence
// number from the existing segments, truncates any torn tail from the
// active segment, and starts the background sync loop.
func Open(dir string, opts Options) (*Log, error) {
	opts = normalizeOptions(opts)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	segments, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		segments = []segmentInfo{{base: 0, path: segmentPath(dir, 0)}}
		if err := createEmptySegment(segments[0].path); err != nil {
			return nil, err
		}
	}

	nextSeq, lastValidEnd, err := scanForOpen(segments, opts.MaxRecord)
	if err != nil {
		return nil, err
	}
	last := segments[len(segments)-1]
	if nextSeq < last.base {
		nextSeq = last.base
	}
	// A preparation interrupted by a stop or a crash is never resumed:
	// its file may be partly written. Nothing reads it, and the preparer
	// removes it before preparing again, so a failed remove is harmless.
	_ = os.Remove(filepath.Join(dir, prepFileName))

	// The active segment is truncated to its data, prepared or not (the
	// scan reads a prepared segment's zeros as a torn tail), so nothing
	// can sit behind the next append; it grows by appending until the
	// next roll brings in a prepared successor.
	file, err := openActiveSegment(last.path, lastValidEnd)
	if err != nil {
		return nil, err
	}

	l := &Log{
		dir:         dir,
		opts:        opts,
		file:        file,
		segmentBase: last.base,
		segmentSize: lastValidEnd,
		durableSize: lastValidEnd,
		nextSeq:     nextSeq,
		prealloc:    opts.Prealloc.enabled(),
		wakeup:      make(chan struct{}, 1),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	if l.prealloc {
		l.prepWake = make(chan struct{}, 1)
		l.prepDone = make(chan struct{})
		go l.prepLoop()
	}
	go l.syncLoop()
	return l, nil
}

// openActiveSegment opens the last segment for appending, discarding any
// bytes past validEnd (a torn tail detected by the open-time scan).
func openActiveSegment(path string, validEnd int64) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open active segment: %w", err)
	}
	if err := syncfile.Truncate(file, validEnd); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: truncate active segment: %w", err)
	}
	if _, err := file.Seek(validEnd, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: seek active segment: %w", err)
	}
	return file, nil
}

// Append writes payload as one record and blocks until it is durably
// synced, returning the record's ID. The returned error is the sync
// outcome of the whole batch the record was flushed in.
func (l *Log) Append(ctx context.Context, payload []byte) (RecordID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RecordID{}, err
	}
	if len(payload) == 0 {
		return RecordID{}, errors.New("wal: empty payload")
	}
	if len(payload) > l.opts.MaxRecord {
		return RecordID{}, fmt.Errorf("wal: payload size %d exceeds max %d", len(payload), l.opts.MaxRecord)
	}

	id, batch, err := l.stage(len(payload), func(dst []byte) []byte { return append(dst, payload...) })
	if err != nil {
		return RecordID{}, err
	}

	l.signalSync()

	// The record is now in the write buffer with a committed seq; the
	// sync loop will durably persist it and it will be replayed after a
	// restart regardless of ctx. Abandoning the wait on ctx.Done() here
	// would report failure for a record that is in fact durable, which a
	// retrying caller would then duplicate. So past the append we wait
	// for the true sync outcome rather than honouring cancellation.
	// (ctx is still checked before the append, at the top of Append.)
	<-batch.done
	return id, batch.err
}

// AppendWith is Append for a payload the caller can produce in place:
// size is the exact encoded length and fill appends exactly size bytes
// to the slice it is given (returning the extended slice). The bytes are
// written directly into the group-commit buffer, so the caller neither
// allocates nor copies its record. fill runs under the log's append
// lock and must be cheap and non-blocking.
func (l *Log) AppendWith(ctx context.Context, size int, fill func(dst []byte) []byte) (RecordID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RecordID{}, err
	}
	if size <= 0 {
		return RecordID{}, errors.New("wal: empty payload")
	}
	if size > l.opts.MaxRecord {
		return RecordID{}, fmt.Errorf("wal: payload size %d exceeds max %d", size, l.opts.MaxRecord)
	}
	if fill == nil {
		return RecordID{}, errors.New("wal: nil fill")
	}

	id, batch, err := l.stage(size, fill)
	if err != nil {
		return RecordID{}, err
	}

	l.signalSync()
	<-batch.done // see Append for why ctx is not honoured past this point
	return id, batch.err
}

// AppendManyWith is AppendWith for several records that are acked
// together: record i is sizes[i] bytes, appended by fill(i, dst) under
// the rules AppendWith sets for its fill. The records are staged in
// slice order under one hold of the append lock, so they take
// consecutive seqs, nothing is staged between them, and they share one
// group commit: the call wakes the sync loop once, waits once, and
// returns the records' IDs in slice order once all of them are durable.
// A batch that outgrows the room left in the active segment rolls it
// between two records exactly as single appends would (the records
// before the roll are synced first, inline), so no frame spans two
// segments.
//
// On disk a batch is the frames len(sizes) single appends would have
// written, and replay cannot tell the two apart. A crash can keep a
// prefix of a batch but never a gap: frames are written in order and
// recovery truncates at the first torn one.
//
// An error means the batch is not durable as a whole and none of it
// may be acked. Records a roll had already synced before it failed
// stay in the log and are replayed, as with any failed sync, so a
// caller that retries may duplicate them. A fill that fails or panics
// withdraws the records staged since the last such roll.
func (l *Log) AppendManyWith(ctx context.Context, sizes []int, fill func(i int, dst []byte) []byte) ([]RecordID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(sizes) == 0 {
		return nil, errors.New("wal: no records")
	}
	for i, size := range sizes {
		if size <= 0 {
			return nil, fmt.Errorf("wal: record %d: empty payload", i)
		}
		if size > l.opts.MaxRecord {
			return nil, fmt.Errorf("wal: record %d: payload size %d exceeds max %d", i, size, l.opts.MaxRecord)
		}
	}
	if fill == nil {
		return nil, errors.New("wal: nil fill")
	}

	ids, batch, err := l.stageMany(sizes, fill)
	if err != nil {
		return nil, err
	}

	l.signalSync()
	<-batch.done // see Append for why ctx is not honoured past this point
	if batch.err != nil {
		return nil, batch.err
	}
	return ids, nil
}

// stageMany runs appendManyLocked under mu, unlocking on a panic in fill
// as stage does.
func (l *Log) stageMany(sizes []int, fill func(i int, dst []byte) []byte) ([]RecordID, *syncBatch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendManyLocked(sizes, fill)
}

// stage runs appendLocked under mu. The unlock is deferred so a panic
// inside a caller-supplied fill cannot leave the log locked forever (an
// HTTP handler's recover middleware would otherwise hide the panic and
// every later append and the sync loop would block).
func (l *Log) stage(size int, fill func(dst []byte) []byte) (RecordID, *syncBatch, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendLocked(size, fill)
}

// NextSeq returns the next sequence number the log will assign: one past
// the newest record (durable or staged), as recovered at Open — where the
// last segment's base is a floor, so an empty active segment left by
// compaction rotation cannot regress the sequence space — and advanced by
// appends since.
func (l *Log) NextSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextSeq
}

// Err returns the latched write or sync failure, or nil while the log is
// healthy. Once set it never clears: the log refuses every append (and
// fails every waiting one) until it is reopened, because the bytes after
// the failure point are of unknown durability.
func (l *Log) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.syncErr
}

// Close stops the sync loop and the segment preparer, flushes any
// buffered records, and closes the active segment. It returns the
// latched sync error, if any.
func (l *Log) Close() error {
	l.once.Do(func() { close(l.stop) })
	<-l.done
	if l.prepDone != nil {
		<-l.prepDone
		// A prepared spare is not kept across restarts (Open discards
		// it), so do not leave it holding disk.
		_ = os.Remove(filepath.Join(l.dir, prepFileName))
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return l.syncErr
	}
	err := l.file.Close()
	l.file = nil
	if l.syncErr != nil {
		return l.syncErr
	}
	return err
}
