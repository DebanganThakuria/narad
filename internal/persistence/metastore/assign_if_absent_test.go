package metastore

// Partition placement is insert-only: once every member applies the
// entry type for it, a placement never replaces an owner on record, and
// never writes a row for a deleted topic, an out-of-range partition or a
// removed member.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// stallFSM holds bbolt's single writer lock so s's FSM cannot apply
// committed entries until release is called: a replica lagging its Raft
// log. Reads keep working (bbolt readers never wait for a writer).
func stallFSM(t *testing.T, s *Store) (release func()) {
	t.Helper()
	s.fsm.mu.RLock()
	tx, err := s.fsm.db.Begin(true)
	s.fsm.mu.RUnlock()
	if err != nil {
		t.Fatalf("stall the FSM: %v", err)
	}
	released := false
	release = func() {
		if !released {
			released = true
			_ = tx.Rollback()
		}
	}
	t.Cleanup(release)
	return release
}

// waitLogPast waits until s's Raft log holds an entry past index.
func waitLogPast(t *testing.T, s *Store, index uint64) {
	t.Helper()
	waitUntil(t, 5*time.Second, "a new raft log entry", func() bool { return s.r.LastIndex() > index })
}

// registerCurrent registers alive members that report this release's
// Raft entry types.
func registerCurrent(t *testing.T, s *Store, ids ...string) {
	t.Helper()
	for _, id := range ids {
		m := Member{ID: id, Addr: id + ":7942", Status: MemberAlive, LastHeartbeat: time.Now().Unix(), Build: "narad test", EntryTypes: MaxEntryType}
		if err := s.RegisterMember(context.Background(), m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
}

// A placement pass computed from a replica that had not applied the
// previous leader's placement of partition 0 must not replace that
// owner: the partition's records may already be on it.
func TestPlacementNeverReplacesAnOwner(t *testing.T) {
	ctx := context.Background()
	s, _ := singleVoter(t, "pl-0")
	registerCurrent(t, s, "a", "b", "c")
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "00000000000000a1", Partitions: 3, RetentionMs: 3_600_000}); err != nil {
		t.Fatal(err)
	}

	release := stallFSM(t, s)
	committed := s.r.LastIndex()
	earlier := make(chan error, 1)
	go func() { earlier <- s.AssignPartition(ctx, "orders", 0, "c") }()
	waitLogPast(t, s, committed)
	pass := make(chan error, 1)
	go func() { pass <- s.AssignNewPartitions(ctx, "orders", 0, 3) }()
	waitLogPast(t, s, committed+1)
	release()

	if err := <-earlier; err != nil {
		t.Fatalf("the earlier placement: %v", err)
	}
	if err := <-pass; err != nil {
		t.Fatalf("placement pass: %v", err)
	}
	owners := map[int]string{}
	rows, err := s.ListAssignments("orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range rows {
		owners[a.Partition] = a.OwnerID
	}
	if owners[0] != "c" || len(owners) != 3 {
		t.Fatalf("owners after the pass = %v, want partition 0 still on c and every partition placed", owners)
	}
}

func TestAssignIfAbsentRefusesDeletedTopicOutOfRangeAndExistingRow(t *testing.T) {
	l := newEntryLog(t)
	l.must(opCreateTopic, topicRecord("orders", "0000000000000051"))
	l.must(opMemberJoin, Member{ID: "gone", Status: MemberAlive})
	l.must(opRemoveMember, memberRemovalPayload{ID: "gone", At: 1})

	refused := []struct {
		name string
		p    assignIfAbsentPayload
		want error
	}{
		{"a deleted topic", assignIfAbsentPayload{Topic: "deleted", Partition: 0, OwnerID: "a", ExpectID: "0000000000000051"}, ErrNotFound},
		{"another incarnation", assignIfAbsentPayload{Topic: "orders", Partition: 0, OwnerID: "a", ExpectID: "0000000000000050"}, errs.ErrTopicChanged},
		{"a partition past the count", assignIfAbsentPayload{Topic: "orders", Partition: 3, OwnerID: "a", ExpectID: "0000000000000051"}, errs.ErrInvalidArgument},
		{"a negative partition", assignIfAbsentPayload{Topic: "orders", Partition: -1, OwnerID: "a", ExpectID: "0000000000000051"}, errs.ErrInvalidArgument},
		{"no owner", assignIfAbsentPayload{Topic: "orders", Partition: 0, ExpectID: "0000000000000051"}, errs.ErrInvalidArgument},
		{"a removed member", assignIfAbsentPayload{Topic: "orders", Partition: 0, OwnerID: "gone", ExpectID: "0000000000000051"}, errs.ErrInvalidArgument},
	}
	for _, tc := range refused {
		if err := l.apply(opAssignPartitionIfAbsent, tc.p); !errors.Is(err, tc.want) {
			t.Fatalf("placement on %s = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := l.rows("orders") + l.rows("deleted"); n != 0 {
		t.Fatalf("%d rows written by refused placements, want 0", n)
	}

	l.must(opAssignPartitionIfAbsent, assignIfAbsentPayload{Topic: "orders", Partition: 0, OwnerID: "a", ExpectID: "0000000000000051"})
	if err := l.apply(opAssignPartitionIfAbsent, assignIfAbsentPayload{Topic: "orders", Partition: 0, OwnerID: "b", ExpectID: "0000000000000051"}); !errors.Is(err, ErrPartitionAssigned) {
		t.Fatalf("second placement of orders/0 = %v, want ErrPartitionAssigned", err)
	}
	var a Assignment
	if err := l.f.view(func(tx *bolt.Tx) error {
		return json.Unmarshal(tx.Bucket(bucketAssignments).Get(assignmentKey("orders", 0)), &a)
	}); err != nil {
		t.Fatal(err)
	}
	if a.OwnerID != "a" {
		t.Fatalf("orders/0 owner = %q after a second placement, want a", a.OwnerID)
	}
}

func TestPruneRemovesOnlyOrphanRows(t *testing.T) {
	ctx := context.Background()
	s, _ := singleVoter(t, "pr-0")
	registerCurrent(t, s, "a", "b", "c")
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0000000000000061", Partitions: 3, RetentionMs: 3_600_000}); err != nil {
		t.Fatal(err)
	}
	if err := s.AssignNewPartitions(ctx, "orders", 0, 3); err != nil {
		t.Fatal(err)
	}
	// Rows an earlier release could leave: past a topic's count, and of
	// a topic that is gone.
	for _, a := range []Assignment{{Topic: "orders", Partition: 5}, {Topic: "gone", Partition: 0}, {Topic: "gone", Partition: 1}} {
		if err := s.AssignPartition(ctx, a.Topic, a.Partition, "a"); err != nil {
			t.Fatal(err)
		}
	}

	orphans, err := s.OrphanAssignments()
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, a := range orphans {
		keys = append(keys, fmt.Sprintf("%s/%d", a.Topic, a.Partition))
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"gone/0", "gone/1", "orders/5"}) {
		t.Fatalf("orphan rows = %v, want gone/0, gone/1 and orders/5", keys)
	}
	if err := s.PruneAssignment(ctx, "orders", 1); !errors.Is(err, ErrAssignmentLive) {
		t.Fatalf("prune of a live row = %v, want ErrAssignmentLive", err)
	}
	for _, a := range orphans {
		if err := s.PruneAssignment(ctx, a.Topic, a.Partition); err != nil {
			t.Fatalf("prune %s/%d: %v", a.Topic, a.Partition, err)
		}
	}
	if err := s.PruneAssignment(ctx, "gone", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prune of a row already gone = %v, want ErrNotFound", err)
	}
	if orphans, err := s.OrphanAssignments(); err != nil || len(orphans) != 0 {
		t.Fatalf("orphan rows after the prune = %v, %v; want none", orphans, err)
	}
	rows, err := s.ListAssignments("orders")
	if err != nil || len(rows) != 3 {
		t.Fatalf("orders rows after the prune = %v, %v; want its 3 partitions", rows, err)
	}
}
