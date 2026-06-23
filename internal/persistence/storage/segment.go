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
	return &segment{
		file:       f,
		path:       path,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
	}, nil
}

// writeEncodedFrame appends a pre-encoded frame to the segment. On a
// partial-write failure it truncates back to the pre-write size so
// recovery doesn't have to resync past a torn tail on the next startup.
func (s *segment) writeEncodedFrame(frame []byte, baseOffset int64, records int) (pos int64, n int, err error) {
	pos, err = s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, 0, fmt.Errorf("storage: segment seek: %w", err)
	}
	n, err = s.file.Write(frame)
	if err != nil {
		_ = s.file.Truncate(pos)
		_, _ = s.file.Seek(pos, io.SeekStart)
		return pos, n, fmt.Errorf("storage: segment write: %w", err)
	}
	s.sizeBytes = pos + int64(n)
	s.nextOffset = baseOffset + int64(records)
	return pos, n, nil
}

func (s *segment) sync() error { return s.file.Sync() }

func (s *segment) close() error {
	if s.file == nil {
		return nil
	}
	syncErr := s.file.Sync()
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
