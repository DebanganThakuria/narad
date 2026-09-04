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

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// segment is one file in a partition's directory of segment files.
// The active segment is the highest-baseOffset entry; older segments
// are sealed (read-only).
type segment struct {
	file       *os.File
	path       string
	baseOffset int64
	nextOffset int64
	sizeBytes  int64
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
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &segment{
		file:       f,
		path:       path,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		sizeBytes:  st.Size(),
	}, nil
}

func createSegment(dir string, baseOffset int64) (*segment, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: ensure segment dir: %w", err)
	}
	path := filepath.Join(dir, segmentFileName(baseOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
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
		file:       f,
		path:       path,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
	}, nil
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
// log. Segments are append-only, so data-only sync suffices — see
// internal/persistence/syncfile.
func (s *segment) sync() error { return syncfile.SyncData(s.file) }

func (s *segment) close() error {
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
func (s *segment) closeNoSync() error {
	if s.file == nil {
		return nil
	}
	closeErr := s.file.Close()
	s.file = nil
	if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
		return closeErr
	}
	return nil
}

func (s *segment) truncate(pos int64) error {
	if err := s.file.Truncate(pos); err != nil {
		return err
	}
	if _, err := s.file.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	s.sizeBytes = pos
	return nil
}
