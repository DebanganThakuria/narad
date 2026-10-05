package topics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// assignmentLockMetastore exposes the metastore's assignment lock and
// reports when a topic delete reaches it.
type assignmentLockMetastore struct {
	*fakeMetastore
	assignMu      sync.Mutex
	deleteReached chan string
}

func (f *assignmentLockMetastore) LockAssignments() (unlock func()) {
	f.assignMu.Lock()
	return f.assignMu.Unlock
}

func (f *assignmentLockMetastore) DeleteTopic(ctx context.Context, name string) error {
	f.deleteReached <- name
	return f.fakeMetastore.DeleteTopic(ctx, name)
}

// A topic delete takes the assignment lock around its metastore write,
// so a controller placement pass that re-read the topic under that lock
// cannot write owner rows for it after the delete (audit M2, L6: master
// deleted while the pass held the lock, leaving rows a later same-named
// topic inherited).
func TestDeleteHoldsTheAssignmentLock(t *testing.T) {
	ms := &assignmentLockMetastore{fakeMetastore: newFakeMetastore(), deleteReached: make(chan string, 1)}
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3}
	m := newTestManagerForMetastore(t, ms, nil, nil, "")

	unlockSweep := ms.LockAssignments() // a placement pass is running
	done := make(chan error, 1)
	go func() { done <- m.DeleteTopic(context.Background(), "orders") }()

	select {
	case name := <-ms.deleteReached:
		t.Fatalf("the metastore delete of %q ran while a placement pass held the assignment lock", name)
	case <-time.After(100 * time.Millisecond):
	}
	unlockSweep()
	select {
	case <-ms.deleteReached:
	case <-time.After(5 * time.Second):
		t.Fatal("the delete never reached the metastore after the pass released the lock")
	}
	if err := <-done; err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}
