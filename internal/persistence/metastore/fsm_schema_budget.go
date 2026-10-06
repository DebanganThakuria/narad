package metastore

// Schema history byte budgets in the state machine. The topic manager
// refuses a schema write that would pass a budget before it proposes
// one; the entry types that store schemas and are newer than 3.0.x
// (opCreateTopicWith, opPutSchemaIf, and the adoption in
// opAttachChildIf) check the same budgets again when they apply, as a
// pure function of the replicated schemas bucket, so every replica
// refuses the same write. opPutSchema and opAttachChild keep their 3.0.x
// semantics.
//
// The budgets are part of those entry types' meaning: a release that
// changed them would apply the same log differently, so they never
// change.

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

const (
	// SchemaTopicBudgetBytes caps the stored bytes of one topic's schema
	// history, every version as stored. A fan-out child's copy of its
	// parent's history counts against the child.
	SchemaTopicBudgetBytes int64 = 4 << 20
	// SchemaClusterBudgetBytes caps the stored bytes of every schema
	// version of every topic together.
	SchemaClusterBudgetBytes int64 = 256 << 20
)

// schemaBudgets are the budgets an FSM enforces: the constants above,
// held per FSM so tests can exercise them without writing hundreds of
// MiB.
type schemaBudgets struct {
	topic, cluster int64
}

func defaultSchemaBudgets() schemaBudgets {
	return schemaBudgets{topic: SchemaTopicBudgetBytes, cluster: SchemaClusterBudgetBytes}
}

// topicSchemaBytes sums the stored bytes of the topic's schema
// versions. Topic names cannot contain ':', so the prefix scan is exact.
func topicSchemaBytes(tx *bolt.Tx, topicName string) int64 {
	var total int64
	prefix := []byte(topicName + ":")
	c := tx.Bucket(bucketSchemas).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		total += int64(len(v))
	}
	return total
}

// clusterSchemaBytes sums the stored bytes of every schema version. It
// walks keys only: a value's length is read from its leaf element, so
// large values' overflow pages are not touched.
func clusterSchemaBytes(tx *bolt.Tx) int64 {
	var total int64
	c := tx.Bucket(bucketSchemas).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		total += int64(len(v))
	}
	return total
}

// check refuses a write that adds added[name] bytes to each
// named topic's history when a topic would pass its budget or the
// cluster would pass its own. Topics are checked in name order so the
// refusal names the same topic on every replica. It matches
// errs.ErrSchemaHistoryFull (409).
func (b schemaBudgets) check(tx *bolt.Tx, added map[string]int64) error {
	var sum int64
	for _, name := range slices.Sorted(maps.Keys(added)) {
		n := added[name]
		if n <= 0 {
			continue
		}
		sum += n
		if have := topicSchemaBytes(tx, name); have+n > b.topic {
			return fmt.Errorf("%w: the schema history of %q would hold %d bytes, over the per-topic budget of %d bytes (%d stored)",
				errs.ErrSchemaHistoryFull, name, have+n, b.topic, have)
		}
	}
	if sum == 0 {
		return nil
	}
	if have := clusterSchemaBytes(tx); have+sum > b.cluster {
		return fmt.Errorf("%w: the cluster's schemas would hold %d bytes, over the cluster budget of %d bytes (%d stored)",
			errs.ErrSchemaHistoryFull, have+sum, b.cluster, have)
	}
	return nil
}
