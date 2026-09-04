package runtime

import (
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// listingFakeMetastore adds the one-call-per-topic ListAssignments
// capability on top of the point-lookup fake, counting each call so
// the test can assert the poller no longer pays one read per partition.
type listingFakeMetastore struct {
	*runtimeFakeMetastore
	listCalls int
	getCalls  int
	listErr   error
}

func (f *listingFakeMetastore) ListAssignments(topicName string) ([]metastore.Assignment, error) {
	f.listCalls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []metastore.Assignment
	for _, a := range f.assignments {
		if a.Topic == topicName {
			out = append(out, a)
		}
	}
	return out, nil
}

func (f *listingFakeMetastore) GetAssignment(topicName string, partition int) (metastore.Assignment, error) {
	f.getCalls++
	return f.runtimeFakeMetastore.GetAssignment(topicName, partition)
}

func ownedSet(owned func(int) bool, partitions int) []int {
	var out []int
	for i := range partitions {
		if owned(i) {
			out = append(out, i)
		}
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestSnapshotterOwnedPartitionsListsOncePerTopic pins the ownership
// filter's semantics on the listing path: locally owned partitions are
// reported, unassigned and foreign ones are not, and the metastore is
// read once per topic rather than once per partition.
func TestSnapshotterOwnedPartitionsListsOncePerTopic(t *testing.T) {
	base := newRuntimeFakeMetastore()
	base.setAssignment("orders", 0, "n1")
	base.setAssignment("orders", 1, "n2")
	base.setAssignment("orders", 3, "n1")
	base.setAssignment("orders", 9, "n1") // beyond the partition count: ignored
	ms := &listingFakeMetastore{runtimeFakeMetastore: base}

	s := &Snapshotter{metastore: ms, selfID: "n1"}
	owned := s.ownedPartitions("orders", 4)
	if got, want := ownedSet(owned, 4), []int{0, 3}; !equalInts(got, want) {
		t.Fatalf("owned partitions = %v, want %v", got, want)
	}
	if ms.listCalls != 1 || ms.getCalls != 0 {
		t.Fatalf("metastore reads: list=%d get=%d, want one list and no point lookups", ms.listCalls, ms.getCalls)
	}

	// A listing error omits every partition, as per-partition lookup
	// errors did before.
	ms.listErr = errors.New("replica closed")
	if got := ownedSet(s.ownedPartitions("orders", 4), 4); len(got) != 0 {
		t.Fatalf("owned partitions on list error = %v, want none", got)
	}
}

// TestSnapshotterOwnedPartitionsFallsBackToPointLookups covers a
// metastore that only offers GetAssignment, and the no-identity case
// where every partition counts as local.
func TestSnapshotterOwnedPartitionsFallsBackToPointLookups(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.setAssignment("orders", 1, "n1")
	ms.setAssignment("orders", 2, "n2")

	s := &Snapshotter{metastore: ms, selfID: "n1"}
	if got, want := ownedSet(s.ownedPartitions("orders", 3), 3), []int{1}; !equalInts(got, want) {
		t.Fatalf("owned partitions via GetAssignment = %v, want %v", got, want)
	}

	anonymous := &Snapshotter{metastore: ms}
	if got, want := ownedSet(anonymous.ownedPartitions("orders", 3), 3), []int{0, 1, 2}; !equalInts(got, want) {
		t.Fatalf("owned partitions without selfID = %v, want %v", got, want)
	}
}
