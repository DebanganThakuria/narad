package metastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/debanganthakuria/narad/internal/topic"
)

// load reads the JSON file from disk into s.state. A missing or empty
// file is treated as a fresh store (zero-value state).
func (s *JSONFileStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // first boot
		}
		return fmt.Errorf("metastore: read %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return nil
	}

	var on state
	if err := json.Unmarshal(data, &on); err != nil {
		return fmt.Errorf("metastore: decode %s: %w", s.path, err)
	}
	if on.Version != stateVersion {
		return fmt.Errorf("metastore: unsupported state version %d (expected %d)", on.Version, stateVersion)
	}
	if on.Topics == nil {
		on.Topics = map[string]topic.Topic{}
	}
	if on.Schemas == nil {
		on.Schemas = map[string]map[int][]byte{}
	}
	if on.Offsets == nil {
		on.Offsets = map[string]map[int]int64{}
	}
	s.state = on
	return nil
}
