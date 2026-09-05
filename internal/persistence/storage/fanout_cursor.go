package storage

// Fan-out cursor persistence. A cursor tracks how far one child topic
// has been fanned out from one parent partition; it lives in the PARENT
// partition's directory (the cursor runs on the parent partition's
// owner, next to the data it tails) as fanout-<child>.offset. The file
// carries the link's attach epoch so a cursor from an earlier
// attachment is never resumed after a detach/re-attach — re-attach
// starts fresh at the parent's tail, matching the no-backfill contract.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FanoutCursor is the persisted fan-out position of one
// (child, parentPartition) pair.
type FanoutCursor struct {
	// Epoch is the attach epoch of the parent→child link this cursor
	// belongs to (topic.Topic.AttachEpoch).
	Epoch string `json:"epoch"`
	// NextOffset is the next parent-log offset to fan out. It advances
	// only after the records below it are durably committed to the
	// child (commit-before-advance).
	NextOffset int64 `json:"next_offset"`
}

func fanoutCursorFileName(child string) string {
	return "fanout-" + child + ".offset"
}

// ReadFanoutCursor loads the persisted cursor for child from a parent
// partition directory. ok=false (with nil error) when none exists.
func ReadFanoutCursor(partitionDir, child string) (FanoutCursor, bool, error) {
	buf, err := os.ReadFile(filepath.Join(partitionDir, fanoutCursorFileName(child)))
	if errors.Is(err, os.ErrNotExist) {
		return FanoutCursor{}, false, nil
	}
	if err != nil {
		return FanoutCursor{}, false, err
	}
	var c FanoutCursor
	if err := json.Unmarshal(buf, &c); err != nil {
		return FanoutCursor{}, false, fmt.Errorf("storage: fan-out cursor for %q corrupt: %w", child, err)
	}
	return c, true, nil
}

// WriteFanoutCursorIfPartitionDirExists atomically persists the cursor,
// failing with ErrPartitionDirMissing instead of resurrecting a parent
// partition directory that a concurrent topic delete removed.
func WriteFanoutCursorIfPartitionDirExists(partitionDir, child string, c FanoutCursor) error {
	info, err := os.Stat(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("storage: fan-out cursor partition path is not a directory: %s", partitionDir)
	}
	buf, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return writeFileAtomic(partitionDir, fanoutCursorFileName(child), buf)
}

// RemoveFanoutCursor deletes the persisted cursor for child. Called
// when the parent→child link is gone (detach or delete) so a later
// re-attach cannot resume — and thereby replay — a dead cursor.
// Removing a cursor that does not exist is a no-op.
func RemoveFanoutCursor(partitionDir, child string) error {
	err := os.Remove(filepath.Join(partitionDir, fanoutCursorFileName(child)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ListFanoutCursorChildren returns the child topics that have a
// persisted cursor in the partition directory.
func ListFanoutCursorChildren(partitionDir string) ([]string, error) {
	entries, err := os.ReadDir(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var children []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "fanout-") || !strings.HasSuffix(name, ".offset") {
			continue
		}
		children = append(children, strings.TrimSuffix(strings.TrimPrefix(name, "fanout-"), ".offset"))
	}
	return children, nil
}

// SidecarFile is one small per-partition state file carried verbatim
// alongside the segments when a partition moves: the fan-out cursor
// files. Name is the file's base name inside the partition directory.
type SidecarFile struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

// IsFanoutCursorFileName reports whether name is a fan-out cursor file's
// base name (fanout-<child>.offset) with no path component, which is the
// only kind of sidecar a move installs into a partition directory.
func IsFanoutCursorFileName(name string) bool {
	if name == "" || name != filepath.Base(name) || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	if !strings.HasPrefix(name, "fanout-") || !strings.HasSuffix(name, ".offset") {
		return false
	}
	return len(name) > len("fanout-")+len(".offset")
}

// ListFanoutCursorFiles reads every fan-out cursor file in the partition
// directory verbatim, so a move can carry them to the new owner. A
// missing directory yields no files.
func ListFanoutCursorFiles(partitionDir string) ([]SidecarFile, error) {
	entries, err := os.ReadDir(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []SidecarFile
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !IsFanoutCursorFileName(name) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(partitionDir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue // rewritten atomically under us; the next listing sees the new file
		}
		if err != nil {
			return nil, err
		}
		files = append(files, SidecarFile{Name: name, Data: data})
	}
	return files, nil
}

// InstallFanoutCursorFile atomically writes one transferred cursor file
// into the partition directory. It refuses any name that is not a plain
// fan-out cursor file name, so a transfer can never plant an arbitrary
// path, and validates the payload parses as a cursor.
func InstallFanoutCursorFile(partitionDir string, f SidecarFile) error {
	if !IsFanoutCursorFileName(f.Name) {
		return fmt.Errorf("storage: refusing to install sidecar %q: not a fan-out cursor file name", f.Name)
	}
	var c FanoutCursor
	if err := json.Unmarshal(f.Data, &c); err != nil {
		return fmt.Errorf("storage: refusing to install sidecar %q: corrupt cursor: %w", f.Name, err)
	}
	return writeFileAtomic(partitionDir, f.Name, f.Data)
}
