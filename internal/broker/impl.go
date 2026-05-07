package broker

import (
	"fmt"
	"sync"

	"github.com/debanganthakuria/narad/internal/storage"
)

type impl struct {
	deps Deps

	// mu protects the logs map for lazy open. Once a *storage.Log
	// is in the map, the log itself is internally synchronized.
	mu   sync.RWMutex
	logs map[string]*storage.Log
}

func New(d Deps) (Broker, error) {
	if d.DataDir == "" {
		return nil, fmt.Errorf("%w: data_dir empty", ErrInvalidArgument)
	}
	if d.Metastore == nil || d.Partitions == nil || d.Schemas == nil ||
		d.Offsets == nil || d.Replicator == nil || d.Logger == nil {
		return nil, fmt.Errorf("%w: missing dependency", ErrInvalidArgument)
	}
	if d.TopicPolicy.DefaultPartitions <= 0 {
		return nil, fmt.Errorf("%w: TopicPolicy.DefaultPartitions must be > 0", ErrInvalidArgument)
	}
	if d.TopicPolicy.DefaultReplicationFactor <= 0 {
		return nil, fmt.Errorf("%w: TopicPolicy.DefaultReplicationFactor must be > 0", ErrInvalidArgument)
	}

	return &impl{
		deps: d,
		logs: map[string]*storage.Log{},
	}, nil
}
