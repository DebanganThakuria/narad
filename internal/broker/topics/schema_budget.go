package topics

import (
	"context"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Schema history byte budgets. Each version was capped at 256 KiB and
// each history at 1000 versions, but nothing bounded the bytes every
// node stores, snapshots and restores: a history could reach 256 MB,
// and attach copies the parent's whole history into each child. The leader refuses, before proposing, any schema write that
// would take a topic's stored history past topicSchemaBudgetBytes (a
// child's copy counts against the child) or every topic's schemas
// together past clusterSchemaBudgetBytes.
const (
	topicSchemaBudgetBytes   int64 = 4 << 20
	clusterSchemaBudgetBytes int64 = 256 << 20
)

// schemaBudgets are the budgets a Manager enforces: the constants
// above, held per Manager so tests can exercise them without writing
// hundreds of MiB.
type schemaBudgets struct {
	topic, cluster int64
}

// schemaByteCounter is the metastore capability behind the budgets
// (implemented by *metastore.Store): the stored bytes of one topic's
// schema history and of every schema in the cluster, read from the
// local replica, which the leader barrier brought up to date.
type schemaByteCounter interface {
	SchemaBytes(ctx context.Context, topicName string) (int64, error)
	ClusterSchemaBytes(ctx context.Context) (int64, error)
}

// topicSchemaBytes reads the stored bytes of the topic's schema
// history.
func (m *Manager) topicSchemaBytes(ctx context.Context, name string) (int64, error) {
	if c, ok := m.metastore.(schemaByteCounter); ok {
		n, err := c.SchemaBytes(ctx, name)
		if err != nil {
			return 0, fmt.Errorf("topics: read schema bytes of %q: %w", name, err)
		}
		return n, nil
	}
	history, err := schema.PersistedHistory(ctx, m.metastore, name)
	if err != nil {
		return 0, fmt.Errorf("topics: read schema history: %w", err)
	}
	return historyBytes(history), nil
}

// clusterSchemaBytes reads the stored bytes of every topic's schema
// history.
func (m *Manager) clusterSchemaBytes(ctx context.Context) (int64, error) {
	if c, ok := m.metastore.(schemaByteCounter); ok {
		n, err := c.ClusterSchemaBytes(ctx)
		if err != nil {
			return 0, fmt.Errorf("topics: read cluster schema bytes: %w", err)
		}
		return n, nil
	}
	var total int64
	opts := metastore.ListOptions{Limit: 1000}
	for {
		page, next, err := m.metastore.ListTopics(ctx, opts)
		if err != nil {
			return 0, fmt.Errorf("topics: list topics: %w", err)
		}
		for _, t := range page {
			n, err := m.topicSchemaBytes(ctx, t.Name)
			if err != nil {
				return 0, err
			}
			total += n
		}
		if next == "" {
			return total, nil
		}
		opts.PageToken = next
	}
}

func historyBytes(history []schema.Version) int64 {
	var n int64
	for _, v := range history {
		n += int64(len(v.Raw))
	}
	return n
}

// checkSchemaBudget refuses a schema write that adds add bytes to the
// history of name, which holds have bytes now, and stores copies copies
// of them in the cluster (a parent's version is copied into every
// child). The refusal matches errs.ErrSchemaHistoryFull (409) and names
// the budget and what is stored.
//
// A history, or the cluster as a whole, can already be over its budget:
// the budgets came with this release, and 3.0.x stored up to 1000
// versions of 256 KiB each. Such a history takes no new version, and
// smaller versions do not help, so the refusal says so and names what
// does: a new topic for the new schema, or deleting topics to bring the
// cluster under its budget. Nothing is removed on the broker's own
// initiative.
func (m *Manager) checkSchemaBudget(ctx context.Context, name string, have, add int64, copies int) error {
	if add <= 0 {
		return nil
	}
	if have > m.schemaBudget.topic {
		return fmt.Errorf("%w: the schema history of %q already holds %d bytes, over the per-topic budget of %d bytes (a history this large was stored before the budget applied), so it takes no new version however small; to change the schema, create a new topic with it and move producers and consumers there",
			errs.ErrSchemaHistoryFull, name, have, m.schemaBudget.topic)
	}
	if have+add > m.schemaBudget.topic {
		return fmt.Errorf("%w: the schema history of %q would hold %d bytes, over the per-topic budget of %d bytes (%d stored); register fewer or smaller versions",
			errs.ErrSchemaHistoryFull, name, have+add, m.schemaBudget.topic, have)
	}
	cluster, err := m.clusterSchemaBytes(ctx)
	if err != nil {
		return err
	}
	if cluster > m.schemaBudget.cluster {
		return fmt.Errorf("%w: the cluster's schemas already hold %d bytes, over the cluster budget of %d bytes (stored before the budget applied), so no topic takes a new schema version until topics are deleted to bring the total under the budget",
			errs.ErrSchemaHistoryFull, cluster, m.schemaBudget.cluster)
	}
	if total := add * int64(max(copies, 1)); cluster+total > m.schemaBudget.cluster {
		return fmt.Errorf("%w: the cluster's schemas would hold %d bytes, over the cluster budget of %d bytes (%d stored); delete unused topics or register smaller schemas",
			errs.ErrSchemaHistoryFull, cluster+total, m.schemaBudget.cluster, cluster)
	}
	return nil
}

// checkAdoptSchemaBudget applies the budgets to the copy of parent's
// history that child adopts when it is linked with no schema of its
// own (create-as-child, attach). A parent whose history is already over
// the per-topic budget (stored before the budget applied) can take no
// new schema-less child: the child would have to copy all of it, and a
// child with a schema of its own must hold that same history.
func (m *Manager) checkAdoptSchemaBudget(ctx context.Context, parent, child string) error {
	adopt, err := m.topicSchemaBytes(ctx, parent)
	if err != nil {
		return err
	}
	if adopt > m.schemaBudget.topic {
		return fmt.Errorf("%w: %q cannot become a child of %q: a child keeps a copy of its parent's schema history, and the history of %q already holds %d bytes, over the per-topic budget of %d bytes (a history this large was stored before the budget applied); fan out from a new topic whose schema history is within the budget",
			errs.ErrSchemaHistoryFull, child, parent, parent, adopt, m.schemaBudget.topic)
	}
	return m.checkSchemaBudget(ctx, child, 0, adopt, 1)
}
