package metastore

// opCreateTopicWith: a topic create in one Raft entry. A create used to
// be up to three entries (the record, then schema version 1, then the
// fan-out link) with a rollback that needs leadership. A leader change
// between them left a topic that accepted unvalidated payloads forever,
// or a "child" that copied nothing, and the client's retry answered 409,
// which reads as success. A placement pass between the record and the
// link also put the child's partitions on the parent's nodes, since the
// child had no parent yet. Here the record, its first schema version and
// its link commit in one transaction, or nothing does.

import (
	"encoding/json"
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

func (f *fsmState) applyCreateTopicWith(data []byte) error {
	var p createTopicWithPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if p.Topic.Name == "" {
		return fmt.Errorf("%w: topic name required", errs.ErrInvalidArgument)
	}
	var out createdTopic
	err := f.update(func(tx *bolt.Tx) error {
		var err error
		out, err = f.createTopicWithTx(tx, p)
		return err
	})
	if err != nil {
		return err
	}
	name := p.Topic.Name
	f.versions.bumpTopic(name)
	if out.rowsCleared {
		f.versions.bumpAssignment(name)
	}
	if out.schemaChanged {
		f.versions.bumpSchema(name)
	}
	if p.Link != nil {
		f.versions.bumpTopic(p.Link.Parent)
	}
	return nil
}

// createdTopic is what a create changed besides the topic record.
type createdTopic struct {
	// rowsCleared: assignment rows an earlier topic of the name left
	// behind were deleted.
	rowsCleared bool
	// schemaChanged: a first schema version was stored or adopted, or a
	// history an earlier topic left behind was deleted.
	schemaChanged bool
}

// createTopicWithTx writes the topic, its first schema version and its
// fan-out link. Any refusal returns before the transaction commits, so
// nothing of the topic is left behind.
func (f *fsmState) createTopicWithTx(tx *bolt.Tx, p createTopicWithPayload) (createdTopic, error) {
	var out createdTopic
	t := p.Topic
	if tx.Bucket(bucketTopics).Get([]byte(t.Name)) != nil {
		return out, ErrAlreadyExists
	}
	if existing, found := foldedTopicName(tx, t.Name); found {
		return out, fmt.Errorf("%w: topic name %q differs from existing topic %q only in letter case; both would share one directory on a case-insensitive filesystem, so choose another name",
			errs.ErrTopicAlreadyExists, t.Name, existing)
	}
	if err := putTopicRecord(tx, t); err != nil {
		return out, err
	}
	// Assignment rows or schema versions under a name with no topic
	// belong to no topic (a release before 3.1.0 could leave rows behind
	// a delete). The new topic starts from none of them: it is placed
	// from scratch and its history starts at version 1.
	n, err := deletePrefix(tx.Bucket(bucketAssignments), t.Name+":")
	if err != nil {
		return out, err
	}
	out.rowsCleared = n > 0
	if n, err = deletePrefix(tx.Bucket(bucketSchemas), t.Name+":"); err != nil {
		return out, err
	}
	out.schemaChanged = n > 0
	if len(p.Schema) > 0 {
		if err := f.schemaBudget.check(tx, map[string]int64{t.Name: int64(len(p.Schema))}); err != nil {
			return out, err
		}
		if err := tx.Bucket(bucketSchemas).Put(schemaKey(t.Name, 1), p.Schema); err != nil {
			return out, err
		}
		out.schemaChanged = true
	}
	if p.Link == nil {
		return out, nil
	}
	parent, err := getTopicRecord(tx, p.Link.Parent)
	if err != nil {
		return out, err
	}
	if parent.ID != p.Link.ParentID {
		return out, topicChanged(p.Link.Parent, p.Link.ParentID, parent.ID)
	}
	link := childLinkPayload{
		Parent:  p.Link.Parent,
		Child:   t.Name,
		Epoch:   p.Link.Epoch,
		DelayMs: p.Link.DelayMs,
		Offsets: p.Link.Offsets,
	}
	budgets := f.schemaBudget
	adopted, err := attachChildTx(tx, link, attachRules{byValue: true, budgets: &budgets})
	if err != nil {
		return out, err
	}
	out.schemaChanged = out.schemaChanged || adopted
	return out, nil
}

// foldedTopicName returns an existing topic whose name equals name
// except for letter case (and is not name itself). Topic names are
// ASCII ([A-Za-z0-9._-]), so strings.EqualFold is exactly the folding a
// case-insensitive filesystem applies. It walks the keys without
// decoding the records.
func foldedTopicName(tx *bolt.Tx, name string) (string, bool) {
	c := tx.Bucket(bucketTopics).Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if len(k) != len(name) || string(k) == name {
			continue
		}
		if strings.EqualFold(string(k), name) {
			return string(k), true
		}
	}
	return "", false
}

// topicChanged refuses a write checked against incarnation expected of
// name when found is on record.
func topicChanged(name, expected, found string) error {
	return fmt.Errorf("%w: topic %q is incarnation %q, not %q the request was checked against: it was deleted and recreated since; read it again",
		errs.ErrTopicChanged, name, found, expected)
}
