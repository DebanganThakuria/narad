package metastore

// The state machine side of the topic entry types newer than 3.0.x:
// single-entry creates, compare-and-set writes, JSON-value schema
// comparison and the schema byte budgets. Entries go through Apply, as
// committed log entries do.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// entryLog applies entries to an FSM at increasing indexes.
type entryLog struct {
	t     *testing.T
	f     *fsmState
	index uint64
}

func newEntryLog(t *testing.T) *entryLog {
	t.Helper()
	return &entryLog{t: t, f: newTestFSM(t)}
}

func (l *entryLog) apply(op opCode, payload any) error {
	l.t.Helper()
	l.index++
	return applyEntry(l.t, l.f, l.index, op, payload)
}

func (l *entryLog) must(op opCode, payload any) {
	l.t.Helper()
	if err := l.apply(op, payload); err != nil {
		l.t.Fatalf("apply entry type %d: %v", op, err)
	}
}

// topicRecord is a standalone topic record with incarnation id.
func topicRecord(name, id string) topic.Topic {
	return topic.Topic{Name: name, ID: id, Partitions: 3, RetentionMs: 3_600_000, Owner: "alice", CreatedAt: 1000, VisibilityTimeoutMs: 30_000}
}

// schemaHistory is a topic's schema versions, printable.
type schemaHistory map[int][]byte

func (h schemaHistory) String() string {
	var b strings.Builder
	for _, v := range sortedVersions(h) {
		fmt.Fprintf(&b, "v%d=%s ", v, h[v])
	}
	return b.String()
}

func (l *entryLog) history(name string) schemaHistory {
	l.t.Helper()
	var out map[int][]byte
	if err := l.f.view(func(tx *bolt.Tx) error {
		var err error
		out, err = loadSchemaHistory(tx, name)
		return err
	}); err != nil {
		l.t.Fatal(err)
	}
	return out
}

func (l *entryLog) rows(name string) int {
	l.t.Helper()
	n := 0
	if err := l.f.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketAssignments).Cursor()
		prefix := []byte(name + ":")
		for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
			n++
		}
		return nil
	}); err != nil {
		l.t.Fatal(err)
	}
	return n
}

func TestCreateRefusesCaseFoldedNameInTheStateMachine(t *testing.T) {
	l := newEntryLog(t)
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders", "00000000000000a1")})

	err := l.apply(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("Orders", "00000000000000a2")})
	if !errors.Is(err, errs.ErrTopicAlreadyExists) || !strings.Contains(err.Error(), `"orders"`) {
		t.Fatalf("create of Orders beside orders = %v, want a topic-exists refusal naming orders", err)
	}
	if hasTopic(t, l.f, "Orders") {
		t.Fatal("the refused create left Orders behind")
	}
	if err := l.apply(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders", "00000000000000a3")}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second create of orders = %v, want ErrAlreadyExists", err)
	}
	if got := fsmGetTopic(t, l.f, "orders"); got.ID != "00000000000000a1" {
		t.Fatalf("orders is incarnation %s after a refused create, want 00000000000000a1", got.ID)
	}
}

// The record, the first schema version and the link commit together;
// any refusal leaves nothing of the topic behind.
func TestCreateWithCommitsRecordSchemaAndLinkTogether(t *testing.T) {
	l := newEntryLog(t)
	parentSchema := []byte(`{"type":"object","required":["id"]}`)
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders", "00000000000000b1"), Schema: parentSchema})
	if h := l.history("orders"); len(h) != 1 || string(h[1]) != string(parentSchema) {
		t.Fatalf("orders history = %s, want version 1 only", h)
	}

	link := &createLinkPayload{Parent: "orders", ParentID: "00000000000000b1", Epoch: "e1", Offsets: []int64{4, 5, 6}}
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders-copy", "00000000000000c1"), Link: link})
	child := fsmGetTopic(t, l.f, "orders-copy")
	if !child.IsChild() || child.Parent != "orders" || child.AttachEpoch != "e1" || len(child.AttachOffsets) != 3 {
		t.Fatalf("child = %+v, want attached to orders with epoch e1 and offsets", child)
	}
	if parent := fsmGetTopic(t, l.f, "orders"); !parent.IsParent() || len(parent.Children) != 1 {
		t.Fatalf("parent = %+v, want it to list the child", parent)
	}
	if h := l.history("orders-copy"); len(h) != 1 || string(h[1]) != string(parentSchema) {
		t.Fatalf("child history = %s, want the parent's adopted", h)
	}

	refused := []struct {
		name string
		p    createTopicWithPayload
		want error
	}{
		{"a parent recreated since the check", createTopicWithPayload{
			Topic: topicRecord("stale-child", "00000000000000c2"), Schema: parentSchema,
			Link: &createLinkPayload{Parent: "orders", ParentID: "00000000000000b0"},
		}, errs.ErrTopicChanged},
		{"a parent that is gone", createTopicWithPayload{
			Topic: topicRecord("orphan-child", "00000000000000c3"), Schema: parentSchema,
			Link: &createLinkPayload{Parent: "gone", ParentID: "00000000000000b1"},
		}, ErrNotFound},
		{"a parent that is itself a child", createTopicWithPayload{
			Topic: topicRecord("grandchild", "00000000000000c4"),
			Link:  &createLinkPayload{Parent: "orders-copy", ParentID: "00000000000000c1"},
		}, errs.ErrFanoutRoleConflict},
		{"a schema that differs from the parent's", createTopicWithPayload{
			Topic: topicRecord("other-child", "00000000000000c5"), Schema: []byte(`{"type":"string"}`),
			Link: &createLinkPayload{Parent: "orders", ParentID: "00000000000000b1"},
		}, errs.ErrFanoutSchemaMismatch},
	}
	for _, tc := range refused {
		err := l.apply(opCreateTopicWith, tc.p)
		if !errors.Is(err, tc.want) {
			t.Fatalf("create with %s = %v, want %v", tc.name, err, tc.want)
		}
		if hasTopic(t, l.f, tc.p.Topic.Name) || len(l.history(tc.p.Topic.Name)) != 0 {
			t.Fatalf("create with %s left %s or its schema behind", tc.name, tc.p.Topic.Name)
		}
	}
	if parent := fsmGetTopic(t, l.f, "orders"); len(parent.Children) != 1 {
		t.Fatalf("parent children = %v after refused creates, want only orders-copy", parent.Children)
	}
}

// Assignment rows and schema versions an earlier topic of the name left
// behind belong to no topic: a create starts from none of them.
func TestCreateWithStartsFromNoLeftovers(t *testing.T) {
	l := newEntryLog(t)
	if err := l.f.db.Update(func(tx *bolt.Tx) error {
		for p := range 12 {
			if err := tx.Bucket(bucketAssignments).Put(assignmentKey("orders", p), fmt.Appendf(nil, `{"topic":"orders","partition":%d,"owner_id":"gone"}`, p)); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketSchemas).Put(schemaKey("orders", 1), []byte(`{"type":"string"}`))
	}); err != nil {
		t.Fatal(err)
	}
	schema := []byte(`{"type":"object"}`)
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders", "00000000000000d1"), Schema: schema})
	if n := l.rows("orders"); n != 0 {
		t.Fatalf("%d leftover assignment rows remain after the create, want 0", n)
	}
	if h := l.history("orders"); len(h) != 1 || string(h[1]) != string(schema) {
		t.Fatalf("history after the create = %s, want only its own version 1", h)
	}
}

func TestUpdateTopicIfAppliesOnlyToTheIncarnationRead(t *testing.T) {
	l := newEntryLog(t)
	stored := topicRecord("orders", "00000000000000e1")
	stored.Partitions = 12
	l.must(opCreateTopic, stored)

	stale := topicRecord("orders", "00000000000000e0")
	stale.RetentionMs = 7_200_000
	if err := l.apply(opUpdateTopicIf, updateTopicIfPayload{Topic: stale, ExpectID: "00000000000000e0"}); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("update of another incarnation = %v, want ErrTopicChanged", err)
	}
	shrink := stored
	shrink.Partitions = 3
	if err := l.apply(opUpdateTopicIf, updateTopicIfPayload{Topic: shrink, ExpectID: stored.ID}); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("update that shrinks the partitions = %v, want ErrTopicChanged", err)
	}
	if got := fsmGetTopic(t, l.f, "orders"); got.Partitions != 12 || got.RetentionMs != stored.RetentionMs {
		t.Fatalf("topic after refused updates = %+v, want it unchanged", got)
	}

	next := stored
	next.Partitions, next.RetentionMs, next.MaxInFlightPerPartition = 16, 7_200_000, 99
	next.Owner, next.CreatedAt, next.VisibilityTimeoutMs, next.ID = "mallory", 1, 1, "ffffffffffffffff"
	l.must(opUpdateTopicIf, updateTopicIfPayload{Topic: next, ExpectID: stored.ID})
	got := fsmGetTopic(t, l.f, "orders")
	if got.Partitions != 16 || got.RetentionMs != 7_200_000 || got.MaxInFlightPerPartition != 99 {
		t.Fatalf("updated topic = %+v, want 16 partitions, retention 7200000, in-flight cap 99", got)
	}
	if got.ID != stored.ID || got.Owner != "alice" || got.CreatedAt != 1000 || got.VisibilityTimeoutMs != 30_000 {
		t.Fatalf("updated topic = %+v, want the incarnation, owner, creation time and visibility timeout kept", got)
	}
}

func TestDeleteTopicIfSparesAnotherIncarnation(t *testing.T) {
	l := newEntryLog(t)
	l.must(opCreateTopic, topicRecord("orders", "00000000000000f1"))
	if err := l.apply(opDeleteTopicIf, deleteTopicIfPayload{Name: "orders", ExpectID: "00000000000000f0"}); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("delete of another incarnation = %v, want ErrTopicChanged", err)
	}
	if !hasTopic(t, l.f, "orders") {
		t.Fatal("a delete checked against another incarnation removed the topic")
	}
	l.must(opDeleteTopicIf, deleteTopicIfPayload{Name: "orders", ExpectID: "00000000000000f1"})
	if hasTopic(t, l.f, "orders") {
		t.Fatal("the delete of the incarnation read did not remove it")
	}
	if err := l.apply(opDeleteTopicIf, deleteTopicIfPayload{Name: "orders", ExpectID: "00000000000000f1"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete of a gone topic = %v, want ErrNotFound", err)
	}
}

func TestSchemaPutAndDetachCheckTheIncarnation(t *testing.T) {
	l := newEntryLog(t)
	l.must(opCreateTopic, topicRecord("orders", "0000000000000011"))
	l.must(opCreateTopic, topicRecord("orders-copy", "0000000000000012"))
	l.must(opAttachChildIf, attachChildIfPayload{Link: childLinkPayload{Parent: "orders", Child: "orders-copy"}, ParentID: "0000000000000011", ChildID: "0000000000000012"})

	v1 := []byte(`{"type":"object"}`)
	if err := l.apply(opPutSchemaIf, putSchemaIfPayload{Topic: "orders", Version: 1, Schema: v1, ExpectID: "0000000000000010"}); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("schema put on another incarnation = %v, want ErrTopicChanged", err)
	}
	if len(l.history("orders")) != 0 {
		t.Fatal("a refused schema put stored a version")
	}
	l.must(opPutSchemaIf, putSchemaIfPayload{Topic: "orders", Version: 1, Schema: v1, ExpectID: "0000000000000011"})
	if h := l.history("orders-copy"); len(h) != 1 {
		t.Fatalf("child history = %s, want the parent's version 1 copied", h)
	}
	if err := l.apply(opPutSchemaIf, putSchemaIfPayload{Topic: "orders-copy", Version: 2, Schema: v1, ExpectID: "0000000000000012"}); !errors.Is(err, errs.ErrFanoutSchemaManaged) {
		t.Fatalf("schema put on an attached child = %v, want ErrFanoutSchemaManaged", err)
	}

	if err := l.apply(opDetachChildIf, detachChildIfPayload{Parent: "orders", Child: "orders-copy", ParentID: "0000000000000011", ChildID: "0000000000000010"}); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("detach of another child incarnation = %v, want ErrTopicChanged", err)
	}
	if c := fsmGetTopic(t, l.f, "orders-copy"); !c.IsChild() {
		t.Fatal("a refused detach unlinked the child")
	}
	l.must(opDetachChildIf, detachChildIfPayload{Parent: "orders", Child: "orders-copy", ParentID: "0000000000000011", ChildID: "0000000000000012"})
	if c := fsmGetTopic(t, l.f, "orders-copy"); c.IsChild() {
		t.Fatalf("child after the detach = %+v, want standalone", c)
	}
}

// opAttachChildIf compares schema histories as JSON values and checks
// both incarnations; opAttachChild keeps comparing bytes, as every
// 3.0.x replica does.
func TestAttachIfComparesSchemaValuesAndIncarnations(t *testing.T) {
	l := newEntryLog(t)
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders", "0000000000000021"), Schema: []byte("{\n  \"type\": \"object\", \"maxProperties\": 10,\n  \"required\": [\"id\"]\n}")})
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders-copy", "0000000000000022"), Schema: []byte(`{"required":["id"],"maxProperties":1e1,"type":"object"}`)})

	if err := l.apply(opAttachChild, childLinkPayload{Parent: "orders", Child: "orders-copy"}); !errors.Is(err, errs.ErrFanoutSchemaMismatch) {
		t.Fatalf("byte-comparing attach = %v, want ErrFanoutSchemaMismatch: opAttachChild keeps its 3.0.x meaning", err)
	}
	if err := l.apply(opAttachChildIf, attachChildIfPayload{Link: childLinkPayload{Parent: "orders", Child: "orders-copy"}, ParentID: "0000000000000020", ChildID: "0000000000000022"}); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("attach checked against another parent incarnation = %v, want ErrTopicChanged", err)
	}
	l.must(opAttachChildIf, attachChildIfPayload{Link: childLinkPayload{Parent: "orders", Child: "orders-copy"}, ParentID: "0000000000000021", ChildID: "0000000000000022"})
	if c := fsmGetTopic(t, l.f, "orders-copy"); !c.IsChild() {
		t.Fatalf("child = %+v, want attached: the histories hold the same JSON values", c)
	}
}

func TestSchemaBudgetIsEnforcedInTheStateMachine(t *testing.T) {
	l := newEntryLog(t)
	l.f.schemaBudget = schemaBudgets{topic: 100, cluster: 250}
	schema := func(n int) []byte {
		return fmt.Appendf(nil, `{"description":"%s"}`, strings.Repeat("x", n-len(`{"description":""}`)))
	}

	if err := l.apply(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("big", "0000000000000031"), Schema: schema(101)}); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("create with a schema over the topic budget = %v, want ErrSchemaHistoryFull", err)
	}
	if hasTopic(t, l.f, "big") {
		t.Fatal("a create refused for its schema left the topic behind")
	}
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("orders", "0000000000000032"), Schema: schema(60)})
	if err := l.apply(opPutSchemaIf, putSchemaIfPayload{Topic: "orders", Version: 2, Schema: schema(41), ExpectID: "0000000000000032"}); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("schema put past the topic budget = %v, want ErrSchemaHistoryFull", err)
	}
	l.must(opPutSchemaIf, putSchemaIfPayload{Topic: "orders", Version: 2, Schema: schema(40), ExpectID: "0000000000000032"})

	// A child's adopted copy counts against the child and the cluster:
	// 100 stored, plus a 100-byte copy, plus 60 is past 250.
	l.must(opCreateTopicWith, createTopicWithPayload{Topic: topicRecord("other", "0000000000000033"), Schema: schema(60)})
	if err := l.apply(opCreateTopicWith, createTopicWithPayload{
		Topic: topicRecord("orders-copy", "0000000000000034"),
		Link:  &createLinkPayload{Parent: "orders", ParentID: "0000000000000032"},
	}); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("create-as-child adopting past the cluster budget = %v, want ErrSchemaHistoryFull", err)
	}
	if hasTopic(t, l.f, "orders-copy") {
		t.Fatal("a create refused for its adopted history left the child behind")
	}
	l.must(opCreateTopic, topicRecord("orders-copy", "0000000000000034"))
	if err := l.apply(opAttachChildIf, attachChildIfPayload{Link: childLinkPayload{Parent: "orders", Child: "orders-copy"}, ParentID: "0000000000000032", ChildID: "0000000000000034"}); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("attach adopting past the cluster budget = %v, want ErrSchemaHistoryFull", err)
	}

	// The 3.0.x schema entry keeps its meaning: no budget.
	if err := l.apply(opPutSchema, schemaPayload{Topic: "other", Version: 2, Schema: schema(200)}); err != nil {
		t.Fatalf("opPutSchema past the budget = %v; it must apply as every 3.0.x replica applies it", err)
	}
}

// The entry types every release applies keep their meaning, so a 3.0.x
// replica and a newer one apply the same log alike: opUpdateTopic still
// overwrites the stored record.
func TestEntriesEveryReleaseAppliesKeepTheirMeaning(t *testing.T) {
	l := newEntryLog(t)
	stored := topicRecord("orders", "0000000000000041")
	stored.Partitions = 12
	l.must(opCreateTopic, stored)
	stale := topicRecord("orders", "0000000000000041")
	stale.Owner = "mallory"
	l.must(opUpdateTopic, stale)
	if got := fsmGetTopic(t, l.f, "orders"); got.Partitions != 3 || got.Owner != "mallory" {
		t.Fatalf("opUpdateTopic applied as %+v; it must overwrite as every 3.0.x replica does", got)
	}
}
