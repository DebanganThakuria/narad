package broker

import (
	"fmt"
	"sync"

	"github.com/debanganthakuria/narad/internal/storage"
)

// impl is the default Broker implementation. It is unexported because
// transports should depend on the Broker interface, not the type.
type impl struct {
	deps Deps

	mu    sync.RWMutex
	logs  map[string]*storage.Log // partKey -> log
	locks map[string]*sync.Mutex  // partKey -> per-partition write lock
}

// New constructs the default Broker.
func New(d Deps) (Broker, error) {
	if d.DataDir == "" {
		return nil, fmt.Errorf("%w: data_dir empty", ErrInvalidArgument)
	}
	if d.Metastore == nil || d.Partitions == nil || d.Schemas == nil ||
		d.Offsets == nil || d.Replicator == nil || d.Logger == nil {
		return nil, fmt.Errorf("%w: missing dependency", ErrInvalidArgument)
	}

	return &impl{
		deps:  d,
		logs:  map[string]*storage.Log{},
		locks: map[string]*sync.Mutex{},
	}, nil
}
