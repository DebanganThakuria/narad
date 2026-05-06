package metastore

import (
	"sync"

	"github.com/debanganthakuria/narad/internal/topic"
)

// JSONFileStore persists the entire metastore as a single JSON file.
//
// Trade-off: every mutation rewrites the whole file, so this is fine
// for tens of thousands of metadata records but not for millions.
// Mutations take a write lock and are serialized; reads use a read
// lock. Crash safety comes from the temp-file + os.Rename pattern in
// flush.go: at any instant the on-disk file is either the old snapshot
// or the new one, never partial.
type JSONFileStore struct {
	path string

	mu    sync.RWMutex
	state state
}

// NewJSONFileStore opens (or initialises) the JSON metastore at path.
// The containing directory must already exist; the caller (typically
// cmd/narad) creates it during startup.
func NewJSONFileStore(path string) (*JSONFileStore, error) {
	s := &JSONFileStore{
		path: path,
		state: state{
			Version: stateVersion,
			Topics:  map[string]topic.Topic{},
			Schemas: map[string]map[int][]byte{},
			Offsets: map[string]map[int]int64{},
		},
	}

	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close is a no-op today; kept on the interface so a future
// SQLite-backed implementation can release resources.
func (s *JSONFileStore) Close() error { return nil }
