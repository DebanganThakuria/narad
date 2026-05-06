package metastore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// flush writes the in-memory state to <path>.tmp, fsyncs, then renames
// it over <path>. The caller MUST hold s.mu (write lock).
//
// The temp-file + rename pattern guarantees crash atomicity: the on-disk
// file is always either the old snapshot or the new one, never partial.
func (s *JSONFileStore) flush() error {
	tmp := s.path + ".tmp"

	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("metastore: encode: %w", err)
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("metastore: mkdir %s: %w", dir, err)
	}

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("metastore: open tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("metastore: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("metastore: fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metastore: close tmp: %w", err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metastore: rename: %w", err)
	}
	return nil
}
