package metrics

import (
	"os"
	"path/filepath"
	"strings"
)

// dirSizeScanner computes the total size of a data directory the way a
// recursive walk does, but without stat-ing every file every time.
//
// Almost all bytes under a Narad data directory live in segment files
// (partition logs `*.log`, ingress WAL `*.wal`). Within one directory
// every segment except the highest-numbered one is sealed: it is never
// written again and only ever disappears whole. So the scanner keeps
// the sizes of sealed segments it has already seen and re-stats only
// the active segment of each directory plus the small non-segment
// files (offset, hwm, checkpoint, Raft and bbolt files). A directory
// listing is still taken every scan (one syscall per directory, no
// per-entry stat), which is how deletions and new files are noticed.
// The result is identical to a full walk.
type dirSizeScanner struct {
	sealed map[string]int64 // path -> size of a sealed segment seen before
}

func newDirSizeScanner() *dirSizeScanner {
	return &dirSizeScanner{sealed: make(map[string]int64)}
}

// scan returns the total size of regular files under root, skipping
// `*.tmp` files, and refreshes the sealed-segment cache. Missing entries
// (deleted mid-scan) are skipped, like the walk they replace.
func (s *dirSizeScanner) scan(root string) (int64, error) {
	seen := make(map[string]struct{}, len(s.sealed))
	total, err := s.scanDir(root, seen)
	if err != nil {
		return 0, err
	}
	for path := range s.sealed {
		if _, ok := seen[path]; !ok {
			delete(s.sealed, path)
		}
	}
	return total, nil
}

func (s *dirSizeScanner) scanDir(dir string, seen map[string]struct{}) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	// The highest-named segment file of each suffix is the active one;
	// segment names are zero-padded so lexical order is numeric order.
	activeLog, activeWAL := "", ""
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		switch name := e.Name(); {
		case strings.HasSuffix(name, ".log") && name > activeLog:
			activeLog = name
		case strings.HasSuffix(name, ".wal") && name > activeWAL:
			activeWAL = name
		}
	}

	var total int64
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(dir, name)
		if e.IsDir() {
			sub, err := s.scanDir(path, seen)
			if err != nil {
				return 0, err
			}
			total += sub
			continue
		}
		if strings.HasSuffix(name, ".tmp") {
			continue
		}
		sealed := (strings.HasSuffix(name, ".log") && name != activeLog) ||
			(strings.HasSuffix(name, ".wal") && name != activeWAL)
		if sealed {
			seen[path] = struct{}{}
			if size, ok := s.sealed[path]; ok {
				total += size
				continue
			}
		}
		info, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		total += info.Size()
		if sealed {
			s.sealed[path] = info.Size()
		}
	}
	return total, nil
}
