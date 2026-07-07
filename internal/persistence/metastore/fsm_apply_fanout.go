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
		if p.Parent == p.Child {
			return fmt.Errorf("%w: a topic cannot be its own child", errs.ErrFanoutRoleConflict)
		}
		parent, err := getTopicRecord(tx, p.Parent)
		if err != nil {
			return err
		}
		child, err := getTopicRecord(tx, p.Child)
		if err != nil {
			return err
		}

		if parent.IsChild() {
			return fmt.Errorf("%w: %q is a child of %q and cannot become a parent",
				errs.ErrFanoutRoleConflict, p.Parent, parent.Parent)
		}
		switch {
		case child.IsParent():
			return fmt.Errorf("%w: %q is a parent and cannot become a child (fan-out is depth 1)",
				errs.ErrFanoutRoleConflict, p.Child)
		case child.IsChild() && child.Parent == p.Parent:
			return fmt.Errorf("%w: %q is already attached to %q", ErrAlreadyExists, p.Child, p.Parent)
		case child.IsChild():
			return fmt.Errorf("%w: %q is already attached to parent %q",
				errs.ErrFanoutRoleConflict, p.Child, child.Parent)
		}
		if len(parent.Children) >= topic.MaxChildrenPerParent {
			return fmt.Errorf("%w: %q already has %d children",
				errs.ErrFanoutChildLimit, p.Parent, len(parent.Children))
		}
		if p.DelayMs < 0 {
			return fmt.Errorf("%w: delay_ms must be >= 0", errs.ErrFanoutRoleConflict)
		}
		if err := checkDelayAgainstRetention(p.DelayMs, parent.RetentionMs, p.Parent); err != nil {
			return err
		}

		schemaAdopted, err = reconcileSchemasForAttach(tx, p.Parent, p.Child)
		if err != nil {
			return err
		}

		parent.Role = topic.RoleParent
		parent.Children = append(parent.Children, p.Child)
		child.Role = topic.RoleChild
		child.Parent = p.Parent
		child.AttachEpoch = p.Epoch
		child.FanoutDelayMs = p.DelayMs
		if err := putTopicRecord(tx, parent); err != nil {
			return err
		}
		return putTopicRecord(tx, child)
	})
	if err == nil {
		f.versions.bumpTopic(p.Parent)
		f.versions.bumpTopic(p.Child)
		if schemaAdopted {
			f.versions.bumpSchema(p.Child)
		}
	}
	return err
}

// applyDetachChild unlinks child from parent. The child keeps whatever
// records and schema it already has and becomes standalone; a parent
// whose last child detaches reverts to standalone.
func (f *fsmState) applyDetachChild(data []byte) error {
	var p childLinkPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		parent, err := getTopicRecord(tx, p.Parent)
		if err != nil {
			return err
		}
		child, err := getTopicRecord(tx, p.Child)
		if err != nil {
			return err
		}
		if !child.IsChild() || child.Parent != p.Parent {
			return fmt.Errorf("%w: %q is not attached to %q", ErrNotFound, p.Child, p.Parent)
		}

		parent.Children = slices.DeleteFunc(parent.Children, func(c string) bool { return c == p.Child })
		if len(parent.Children) == 0 {
			parent.Children = nil
			parent.Role = topic.RoleStandalone
		}
		child.Role = topic.RoleStandalone
		child.Parent = ""
		child.AttachEpoch = ""
		child.FanoutDelayMs = 0
		if err := putTopicRecord(tx, parent); err != nil {
			return err
		}
		return putTopicRecord(tx, child)
	})
	if err == nil {
		f.versions.bumpTopic(p.Parent)
		f.versions.bumpTopic(p.Child)
	}
	return err
}

// reconcileSchemasForAttach enforces the attach-time schema gate: the
// child's schema must be absent or byte-identical (every version) to
// the parent's. A schema-less child under a schema'd parent adopts the
// parent's full history so parent and child validate identically from
// the attach point on. Reports whether an adoption happened.
func reconcileSchemasForAttach(tx *bolt.Tx, parentName, childName string) (adopted bool, err error) {
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
		if !schemaHistoriesEqual(parentSchemas, childSchemas) {
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
