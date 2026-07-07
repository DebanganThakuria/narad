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
