package broker

import (
	"log/slog"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
)

// Deps is the constructor input. Passing the dependencies as a struct
// keeps the call site readable as the list grows.
type Deps struct {
	DataDir    string
	Metastore  metastore.Metastore
	Partitions partition.Manager
	Schemas    schema.Registry
	Offsets    consumer.OffsetTracker
	Replicator replication.Replicator
	Logger     *slog.Logger
}
