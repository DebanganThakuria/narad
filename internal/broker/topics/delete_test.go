package topics

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

// DeleteTopicID names the incarnation it actually deleted (audit M8).
// The HTTP delete read the incarnation for the purge broadcast before
// the delete took the name lock, so a delete and recreate interleaved
// between the two made the broadcast name the wrong incarnation, and
// every other node kept the deleted one's files until it restarted.
func TestDeleteTopicIDNamesTheDeletedIncarnation(t *testing.T) {
	ms := newFakeMetastore()
	m := newTestManager(t, ms, nil)
	ctx := context.Background()

	first, err := m.CreateTopic(ctx, CreateOpts{Name: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	readBefore := first.ID // what a caller read before its delete ran
	if err := m.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	second, err := m.CreateTopic(ctx, CreateOpts{Name: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic(again): %v", err)
	}

	deleted, err := m.DeleteTopicID(ctx, "orders")
	if err != nil {
		t.Fatalf("DeleteTopicID: %v", err)
	}
	if deleted != second.ID {
		t.Fatalf("DeleteTopicID returned %q, want the incarnation it deleted %q (the earlier read was %q)", deleted, second.ID, readBefore)
	}
	if _, ok := ms.topics["orders"]; ok {
		t.Fatal("the topic still exists after DeleteTopicID")
	}
	if _, err := m.DeleteTopicID(ctx, "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteTopicID of a missing topic = %v, want ErrNotFound", err)
	}
}

// When the metadata delete committed but the local purge failed, the ID
// still comes back with the PurgeError: the topic is gone and the other
// nodes must still purge that incarnation.
func TestDeleteTopicIDReturnsTheIDWithAPurgeError(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0123456789abcdef", Partitions: 3}
	m := newTestManager(t, ms, nil)
	// A file where the topics directory belongs makes the purge fail.
	if err := os.WriteFile(filepath.Join(m.dataDir, "topics"), []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	id, err := m.DeleteTopicID(context.Background(), "orders")
	if _, ok := errors.AsType[PurgeError](err); !ok {
		t.Fatalf("DeleteTopicID error = %v, want a PurgeError", err)
	}
	if id != "0123456789abcdef" {
		t.Fatalf("DeleteTopicID returned %q with the PurgeError, want the deleted incarnation", id)
	}
	if _, ok := ms.topics["orders"]; ok {
		t.Fatal("the metadata delete did not stand")
	}
}
