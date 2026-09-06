package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Data-file and directory modes. Segments carry every message payload,
// so nothing under the partition directory is group- or world-readable:
// the same protection fsm.db (password hashes) already has. MkdirAll
// and O_CREATE never chmod an existing entry, so directories and files
// created by an older binary keep the mode they were created with;
// tighten them by hand if that matters on a shared host.
const (
	dataFileMode = 0o600
	dataDirMode  = 0o700
)

// segment is one file in a partition's directory of segment files.
// The active segment is the highest-baseOffset entry; older segments
// are sealed (read-only).
//
// The active segment's file is always open. A sealed segment's file is
// opened lazily by the first read that needs it (handle) and released
// again when the segment's index leaves the hot set or the segment is
// deleted, so a partition with days of sealed history does not pin one
// descriptor per segment for the life of the Log.
type segment struct {
	// fmu guards file for the lazy open/release of sealed segments.
	fmu  sync.Mutex
	file *os.File

	path       string
	baseOffset int64
	nextOffset int64
	sizeBytes  int64

	// firstWriteAt is when the first frame landed in this segment and
	// lastWriteAt when the latest one did; both are zero for an empty
	// segment and are the file's mtime for a segment recovered from
	// disk. Written by the flusher under the Log's write lock; read
	// under either side of it. lastWriteAt of a sealed segment never
	// changes again, which is what the reaper's age check relies on
	// instead of a stat per segment per sweep.
	firstWriteAt time.Time
	lastWriteAt  time.Time
}

const segmentFileSuffix = ".log"

// segmentFileName: 20 zero-padded digits so lexicographic sort ==
// numeric sort.
func segmentFileName(baseOffset int64) string {
	return fmt.Sprintf("%020d%s", baseOffset, segmentFileSuffix)
}

func parseSegmentFileName(name string) (int64, bool) {
	if !strings.HasSuffix(name, segmentFileSuffix) {
		return 0, false
	}
	stem := strings.TrimSuffix(name, segmentFileSuffix)
	if len(stem) == 0 {
		return 0, false
	}
	off, err := strconv.ParseInt(stem, 10, 64)
	if err != nil || off < 0 {
		return 0, false
	}
	return off, true
}

func listSegmentFileNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type segName struct {
		name       string
		baseOffset int64
	}
	var segs []segName
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		off, ok := parseSegmentFileName(e.Name())
		if !ok {
			continue
		}
		segs = append(segs, segName{name: e.Name(), baseOffset: off})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].baseOffset < segs[j].baseOffset })

	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = s.name
	}
	return out, nil
}

func openSegment(path string, baseOffset int64) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, dataFileMode)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	// The file's mtime is its last write (its creation, for an empty
	// segment). The first write is not recoverable from the inode, so
	// it is approximated by the same value: the age-based roll of a
	// recovered active segment therefore starts counting from its last
	// write, and the segment lives up to one write-span longer than a
	// segment written entirely by this process.
	seg := &segment{
		file:        f,
		path:        path,
		baseOffset:  baseOffset,
		nextOffset:  baseOffset,
		sizeBytes:   st.Size(),
		lastWriteAt: st.ModTime(),
	}
	if st.Size() > 0 {
		seg.firstWriteAt = st.ModTime()
	}
	return seg, nil
}

// createSegment creates an empty segment file. now stamps it as the
// segment's creation time (reported as its last-write time until the
// first frame lands, so an empty partition still has an "oldest
// segment at").
func createSegment(dir string, baseOffset int64, now time.Time) (*segment, error) {
	if err := os.MkdirAll(dir, dataDirMode); err != nil {
		return nil, fmt.Errorf("storage: ensure segment dir: %w", err)
	}
	path := filepath.Join(dir, segmentFileName(baseOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, dataFileMode)
	if err != nil {
		return nil, fmt.Errorf("storage: create segment %s: %w", path, err)
	}
	// A new file is only durable once its directory entry is: without the
	// directory fsync a crash after the first frames were fdatasynced could
	// lose the whole segment (data present, name absent). The ingress WAL
	// roll does the same.
	if err := syncDir(dir); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("storage: sync dir for segment %s: %w", path, err)
	}
	return &segment{
		file:        f,
		path:        path,
		baseOffset:  baseOffset,
		nextOffset:  baseOffset,
		lastWriteAt: now,
	}, nil
}

// handle returns the segment's open file, opening it read-only if a
// previous release closed it. Safe to call concurrently; the returned
// handle may be closed by a later release or delete, in which case
// reads on it fail with os.ErrClosed and the caller re-resolves.
func (s *segment) handle() (*os.File, error) {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	if s.file != nil {
		return s.file, nil
	}
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	s.file = f
	return f, nil
}

// release closes the file handle of a sealed segment without an
// fdatasync (sealed data was synced when the segment rolled). The next
// handle call reopens it. No-op when the file is already closed.
func (s *segment) release() error {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	if err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

// writeEncodedFrame writes a pre-encoded frame at the segment's current
// size and returns its position and length. It does NOT advance
// sizeBytes/nextOffset: the flusher does that under the Log's write lock
// so readers never observe a frame boundary before the bytes are in
// place. Positional write (pwrite) instead of seek+write: one syscall,
// and on a partial-write failure the truncate back to pos means the
// retry overwrites the torn bytes even if the truncate itself failed.
func (s *segment) writeEncodedFrame(frame []byte) (pos int64, n int, err error) {
	pos = s.sizeBytes
	n, err = s.file.WriteAt(frame, pos)
	if err != nil {
		_ = s.file.Truncate(pos)
		return pos, n, fmt.Errorf("storage: segment write: %w", err)
	}
	return pos, n, nil
}

// sync is the per-commit-batch durability point on the consume-side
// log. Segments are append-only, so data-only sync suffices; see
// internal/persistence/syncfile.
func (s *segment) sync() error { return syncfile.SyncData(s.file) }

// close fdatasyncs and closes the file: the active segment's shutdown
// path. Sealed segments use release, which skips the sync.
func (s *segment) close() error {
	s.fmu.Lock()
	defer s.fmu.Unlock()
	if s.file == nil {
		return nil
	}
	syncErr := syncfile.SyncData(s.file)
	closeErr := s.file.Close()
	s.file = nil
	if syncErr != nil && !errors.Is(syncErr, os.ErrClosed) {
		return syncErr
	}
	if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
		return closeErr
	}
	return nil
}

// closeNoSync releases the file handle without a final fdatasync. For a
// segment that is about to be deleted the sync is wasted work: its bytes
// are going away either way.
func (s *segment) closeNoSync() error { return s.release() }

// truncate cuts the file back to pos and fully fsyncs it so the new
// size survives a crash; only the flusher calls it on the active
// segment (recovery of a torn tail, discard of an uncommitted tail).
// A data-only sync would not do here: the size change is inode
// metadata, and a crash that kept the old size would resurrect frames
// this process has already given up on.
func (s *segment) truncate(pos int64) error {
	if err := s.file.Truncate(pos); err != nil {
		return err
	}
	if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.sizeBytes = pos
	return nil
}
