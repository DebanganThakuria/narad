package cluster

// The move runner is the destination-side reconcile loop. These tests
// drive one full move to completion (copy → install → guarded flip) and
// the abort path (a rejected flip rolls the install back and clears the
// target), against a fake store and a source served off a real partition
// directory — the same bytes the RPC serve side would stream.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// movePeerFake serves a source partition dir and freezes as a no-op (the
// source dir is already static in the test).
type movePeerFake struct {
	dirFetcher
}

func (f movePeerFake) PrepareHandoff(_ context.Context, _, _ string, _ int, _ time.Duration) (messaging.PartitionTransferInfo, error) {
	return messaging.PartitionTransferInfo{
		Segments:        mustSegs(f.dir),
		HighWatermark:   f.hwm,
		CommittedOffset: f.committed,
		HasCommitted:    f.hasCommitted,
	}, nil
}

func mustSegs(dir string) []storage.SegmentInfo {
	segs, _ := storage.ListPartitionSegments(dir)
	return segs
}

type fakeMoveStore struct {
	mu           sync.Mutex
	assignment   metastore.Assignment
	member       metastore.Member
	completeErr  error
	completeArgs []string
	abortArgs    []string
}

func (s *fakeMoveStore) AppliedCaughtUp() bool { return true }

func (s *fakeMoveStore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return []topic.Topic{{Name: "orders", Partitions: 1}}, "", nil
}

func (s *fakeMoveStore) ListAssignments(string) ([]metastore.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []metastore.Assignment{s.assignment}, nil
}

func (s *fakeMoveStore) GetMember(string) (metastore.Member, error) { return s.member, nil }

func (s *fakeMoveStore) CompleteMove(_ context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeArgs = []string{topicName, expectedOwner, targetID}
	if s.completeErr == nil {
		s.assignment.OwnerID = targetID
		s.assignment.TargetID = ""
	}
	return s.completeErr
}

func (s *fakeMoveStore) AbortMove(_ context.Context, topicName string, partition int, expectedTarget string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abortArgs = []string{topicName, expectedTarget}
	return nil
}

func newMoveTestRunner(t *testing.T, store *fakeMoveStore, srcDir string, hwm int64) (*MoveRunner, string) {
	t.Helper()
	peer := movePeerFake{dirFetcher{dir: srcDir, hwm: hwm, committed: 5, hasCommitted: true}}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, MoveConfig{})
	return r, dataDir
}

func TestMoveRunnerCompletesMove(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 20)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	r, dataDir := newMoveTestRunner(t, store, src, wantHWM)

	r.Reconcile(context.Background())
	r.wg.Wait()

	if got := store.completeArgs; len(got) != 3 || got[0] != "orders" || got[1] != "narad-src" || got[2] != "narad-dst" {
		t.Fatalf("CompleteMove args = %v, want [orders narad-src narad-dst]", got)
	}
	// The partition is installed at its real location and recovers to the
	// source's HWM with byte-identical records.
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	log, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatalf("recover installed partition: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("installed NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
	for off, want := range payloads {
		if _, _, got, err := log.ReadKeyed(off); err != nil || string(got) != string(want) {
			t.Fatalf("offset %d = %q (err %v), want %q", off, got, err, want)
		}
	}
}

func TestMoveRunnerAbortsOnFlipReject(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	store := &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		completeErr: context.DeadlineExceeded, // CAS guard rejects the flip
	}
	r, dataDir := newMoveTestRunner(t, store, src, wantHWM)

	r.Reconcile(context.Background())
	r.wg.Wait()

	// A rejected flip must NOT clear the target (the source stays owner and
	// a retry is legitimate), but must roll the install back so a non-owner
	// never keeps a phantom copy.
	if len(store.abortArgs) != 0 {
		t.Fatalf("flip-reject must not AbortMove, got %v", store.abortArgs)
	}
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if _, err := storage.ListPartitionSegments(dir); err == nil {
		if segs, _ := storage.ListPartitionSegments(dir); len(segs) != 0 {
			t.Fatalf("install not rolled back: %d segments remain at %s", len(segs), dir)
		}
	}
}
