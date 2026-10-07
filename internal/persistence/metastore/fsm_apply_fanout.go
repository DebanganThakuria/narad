package metastore

// Fan-out link handlers. Attach and detach mutate the parent and child
// topic records inside one bbolt transaction so the role invariants —
// exclusive roles, depth 1, single parent, child cap — can never be
// observed half-applied. Like every apply* handler these run on Raft's
// FSM goroutine on every node and must stay deterministic.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// checkDelayAgainstRetention enforces the delay-buffer invariant: the
// parent must retain records for the child's delay PLUS the minimum
// outage-tolerance floor, so a delay child always has at least the
// floor's worth of slack before drop-behind can eat a due record.
// Keep-forever parents (retention zero) buffer any delay.
func checkDelayAgainstRetention(delayMs, parentRetentionMs int64, parentName string) error {
	if delayMs <= 0 || parentRetentionMs == 0 {
		return nil
	}
	if parentRetentionMs < delayMs+topic.MinRetentionMs {
		return fmt.Errorf("%w: parent %q retention (%dms) must be at least delay (%dms) + %dms",
			errs.ErrFanoutDelayTooLong, parentName, parentRetentionMs, delayMs, topic.MinRetentionMs)
	}
	return nil
}

func getTopicRecord(tx *bolt.Tx, name string) (topic.Topic, error) {
	raw := tx.Bucket(bucketTopics).Get([]byte(name))
	if raw == nil {
		return topic.Topic{}, fmt.Errorf("%w: topic %q", ErrNotFound, name)
	}
	var t topic.Topic
	if err := json.Unmarshal(raw, &t); err != nil {
		return topic.Topic{}, err
	}
	return t, nil
}

func putTopicRecord(tx *bolt.Tx, t topic.Topic) error {
	v, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return tx.Bucket(bucketTopics).Put([]byte(t.Name), v)
}

// applyAttachChild links child under parent. On success the parent
// gains the child (becoming a parent if it was standalone) and the
// child records its parent. A child with no schema of its own adopts
// the parent's full schema history in the same transaction.
func (f *fsmState) applyAttachChild(data []byte) error {
	var p childLinkPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	schemaAdopted := false
	err := f.update(func(tx *bolt.Tx) error {
		var err error
		schemaAdopted, err = attachChildTx(tx, p, attachRules{})
		return err
	})
	if err == nil {
		f.bumpAttached(p.Parent, p.Child, schemaAdopted)
	}
	return err
}

// bumpAttached advances the versions an attach changed.
func (f *fsmState) bumpAttached(parent, child string, schemaAdopted bool) {
	f.versions.bumpTopic(parent)
	f.versions.bumpTopic(child)
	if schemaAdopted {
		f.versions.bumpSchema(child)
	}
}

// attachRules are what an attach checks beyond the fan-out invariants.
// opAttachChild checks nothing more (its 3.0.x meaning); the newer
// entry types compare schema histories by JSON value and count an
// adopted history against the schema byte budgets.
type attachRules struct {
	// byValue compares the parent's and child's schema histories as
	// JSON values instead of byte for byte.
	byValue bool
	// budgets, when set, refuses an adoption that would pass them.
	budgets *schemaBudgets
}

// attachChildTx links p.Child under p.Parent inside tx, enforcing every
// fan-out invariant, and reports whether the child adopted the parent's
// schema history.
func attachChildTx(tx *bolt.Tx, p childLinkPayload, rules attachRules) (adopted bool, err error) {
	if p.Parent == p.Child {
		return false, fmt.Errorf("%w: a topic cannot be its own child", errs.ErrFanoutRoleConflict)
	}
	parent, err := getTopicRecord(tx, p.Parent)
	if err != nil {
		return false, err
	}
	child, err := getTopicRecord(tx, p.Child)
	if err != nil {
		return false, err
	}

	if parent.IsChild() {
		return false, fmt.Errorf("%w: %q is a child of %q and cannot become a parent",
			errs.ErrFanoutRoleConflict, p.Parent, parent.Parent)
	}
	switch {
	case child.IsParent():
		return false, fmt.Errorf("%w: %q is a parent and cannot become a child (fan-out is depth 1)",
			errs.ErrFanoutRoleConflict, p.Child)
	case child.IsChild() && child.Parent == p.Parent:
		return false, fmt.Errorf("%w: %q is already attached to %q", ErrAlreadyExists, p.Child, p.Parent)
	case child.IsChild():
		return false, fmt.Errorf("%w: %q is already attached to parent %q",
			errs.ErrFanoutRoleConflict, p.Child, child.Parent)
	}
	if len(parent.Children) >= topic.MaxChildrenPerParent {
		return false, fmt.Errorf("%w: %q already has %d children",
			errs.ErrFanoutChildLimit, p.Parent, len(parent.Children))
	}
	if p.DelayMs < 0 {
		return false, fmt.Errorf("%w: delay_ms must be >= 0", errs.ErrFanoutRoleConflict)
	}
	if p.DelayMs > topic.MaxFanoutDelayMs {
		return false, fmt.Errorf("%w: delay_ms (%d) exceeds the maximum of %d (1 year)",
			errs.ErrFanoutDelayTooLong, p.DelayMs, topic.MaxFanoutDelayMs)
	}
	if err := checkDelayAgainstRetention(p.DelayMs, parent.RetentionMs, p.Parent); err != nil {
		return false, err
	}

	adopted, err = reconcileSchemasForAttach(tx, p.Parent, p.Child, rules)
	if err != nil {
		return false, err
	}

	parent.Role = topic.RoleParent
	parent.Children = append(parent.Children, p.Child)
	child.Role = topic.RoleChild
	child.Parent = p.Parent
	child.AttachEpoch = p.Epoch
	child.FanoutDelayMs = p.DelayMs
	child.AttachOffsets = p.Offsets
	if err := putTopicRecord(tx, parent); err != nil {
		return false, err
	}
	return adopted, putTopicRecord(tx, child)
}

// applyDetachChild unlinks child from parent. The child keeps whatever
// records and schema it already has and becomes standalone; a parent
// whose last child detaches reverts to standalone. A remote child's
// stub is deleted instead: standalone it would be a topic with no
// partitions and no purpose, and its cursor files go with the link.
func (f *fsmState) applyDetachChild(data []byte) error {
	var p childLinkPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	stubDeleted := false
	err := f.update(func(tx *bolt.Tx) error {
		parent, err := getTopicRecord(tx, p.Parent)
		if err != nil {
			return err
		}
		child, err := getTopicRecord(tx, p.Child)
		if err != nil {
			return err
		}
		stubDeleted, err = detachChildTx(tx, p.Parent, p.Child, parent, child)
		return err
	})
	if err == nil {
		f.bumpDetached(p.Parent, p.Child, stubDeleted)
	}
	return err
}

// bumpDetached advances the versions a detach changed: a remote child's
// stub went with its link and is retired.
func (f *fsmState) bumpDetached(parent, child string, stubDeleted bool) {
	f.versions.bumpTopic(parent)
	if stubDeleted {
		f.versions.retireTopic(child)
	} else {
		f.versions.bumpTopic(child)
	}
}

// detachChildTx unlinks childName from parentName, whose records
// parent and child were read inside tx, and reports whether the child
// was a remote child's stub, which is deleted with its link.
func detachChildTx(tx *bolt.Tx, parentName, childName string, parent, child topic.Topic) (stubDeleted bool, err error) {
	if !child.IsChild() || child.Parent != parentName {
		return false, fmt.Errorf("%w: %q is not attached to %q", ErrNotFound, childName, parentName)
	}
	unlinkChild(&parent, childName)
	if err := putTopicRecord(tx, parent); err != nil {
		return false, err
	}
	if child.IsRemoteChild() {
		return true, deleteTopicRecords(tx, childName)
	}
	child.Role = topic.RoleStandalone
	child.Parent = ""
	child.AttachEpoch = ""
	child.FanoutDelayMs = 0
	child.AttachOffsets = nil
	return false, putTopicRecord(tx, child)
}

// reconcileSchemasForAttach enforces the attach-time schema gate: the
// child's schema must be absent or identical (every version) to the
// parent's, byte for byte or, under rules.byValue, as JSON values. A
// schema-less child under a schema'd parent adopts the parent's full
// history so parent and child validate identically from the attach
// point on; under rules.budgets the copy must fit the schema byte
// budgets. Reports whether an adoption happened.
func reconcileSchemasForAttach(tx *bolt.Tx, parentName, childName string, rules attachRules) (adopted bool, err error) {
	parentSchemas, err := loadSchemaHistory(tx, parentName)
	if err != nil {
		return false, err
	}
	childSchemas, err := loadSchemaHistory(tx, childName)
	if err != nil {
		return false, err
	}

	switch {
	case len(childSchemas) == 0 && len(parentSchemas) == 0:
		return false, nil
	case len(childSchemas) == 0:
		if rules.budgets != nil {
			var adopt int64
			for _, schema := range parentSchemas {
				adopt += int64(len(schema))
			}
			if err := rules.budgets.check(tx, map[string]int64{childName: adopt}); err != nil {
				return false, fmt.Errorf("%q cannot adopt the schema history of %q: %w", childName, parentName, err)
			}
		}
		b := tx.Bucket(bucketSchemas)
		for _, version := range sortedVersions(parentSchemas) {
			if err := b.Put(schemaKey(childName, version), parentSchemas[version]); err != nil {
				return false, err
			}
		}
		return true, nil
	case len(parentSchemas) == 0:
		return false, fmt.Errorf("%w: child %q has a schema but parent %q does not",
			errs.ErrFanoutSchemaMismatch, childName, parentName)
	default:
		equal := schemaHistoriesEqual
		if rules.byValue {
			equal = schemaHistoriesValueEqual
		}
		if !equal(parentSchemas, childSchemas) {
			return false, fmt.Errorf("%w: schema of %q differs from parent %q; align or clear it, then re-attach",
				errs.ErrFanoutSchemaMismatch, childName, parentName)
		}
		return false, nil
	}
}

// loadSchemaHistory returns every persisted schema version for the
// topic, keyed by version number. Topic names cannot contain ':', so
// the prefix scan is unambiguous.
func loadSchemaHistory(tx *bolt.Tx, topicName string) (map[int][]byte, error) {
	out := map[int][]byte{}
	prefix := []byte(topicName + ":")
	c := tx.Bucket(bucketSchemas).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		version, err := strconv.Atoi(strings.TrimPrefix(string(k), string(prefix)))
		if err != nil {
			return nil, fmt.Errorf("metastore: malformed schema key %q: %w", k, err)
		}
		schema := make([]byte, len(v))
		copy(schema, v)
		out[version] = schema
	}
	return out, nil
}

func sortedVersions(schemas map[int][]byte) []int {
	versions := make([]int, 0, len(schemas))
	for v := range schemas {
		versions = append(versions, v)
	}
	slices.Sort(versions)
	return versions
}

func schemaHistoriesEqual(a, b map[int][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for version, schema := range a {
		other, ok := b[version]
		if !ok || !bytes.Equal(schema, other) {
			return false
		}
	}
	return true
}
