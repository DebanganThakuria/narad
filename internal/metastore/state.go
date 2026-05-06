package metastore

import "github.com/debanganthakuria/narad/internal/topic"

// state is the on-disk shape used by JSONFileStore. Keeping it private
// to the package means we can change layout (versioning, splitting,
// etc.) without breaking consumers of the Metastore interface.
type state struct {
	Version int                       `json:"version"`
	Topics  map[string]topic.Topic    `json:"topics"`
	Schemas map[string]map[int][]byte `json:"schemas"`          // topic -> version -> raw schema
	Offsets map[string]map[int]int64  `json:"consumer_offsets"` // topic -> partition -> offset
}

const stateVersion = 1
