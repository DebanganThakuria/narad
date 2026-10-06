package metastore

// White-box tests for the remote child FSM handlers: every invariant of
// the attach, the field-scoped state op and its epoch check, and how a
// stub behaves under detach, parent delete, topic update and schema put.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// fsmPutRemoteRecord writes a remote record into the registry bucket
// directly, as package A's put would leave it.
func fsmPutRemoteRecord(t *testing.T, f *fsmState, name string) {
	t.Helper()
	raw, err := json.Marshal(domremote.Record{Name: name, ID: "id-" + name, URL: "https://" + name + ".example"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.update(func(tx *bolt.Tx) error { return tx.Bucket(bucketRemotes).Put([]byte(name), raw) }); err != nil {
		t.Fatal(err)
	}
}

func fsmDeleteRemoteRecord(t *testing.T, f *fsmState, name string) {
	t.Helper()
	if err := f.update(func(tx *bolt.Tx) error { return tx.Bucket(bucketRemotes).Delete([]byte(name)) }); err != nil {
		t.Fatal(err)
	}
}

func fsmCreateTopicRetention(t *testing.T, f *fsmState, name string, retentionMs int64) {
	t.Helper()
	data, err := json.Marshal(topic.Topic{Name: name, ID: "id-" + name, Partitions: 3, RetentionMs: retentionMs, Owner: "olivia"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.applyCreateTopic(data); err != nil {
		t.Fatalf("applyCreateTopic(%s): %v", name, err)
	}
}

func remoteAttachOp(parent, stub, remoteName, remoteTopic string) AttachRemoteChildOp {
	return AttachRemoteChildOp{
		Parent: parent, Stub: stub, StubID: "sid-" + stub, Epoch: "ep-" + stub,
		Offsets: []int64{10, 20, 30}, CreatedAt: 1790640000,
		Remote: topic.RemoteLink{Name: remoteName, Topic: remoteTopic, TargetID: "tid", CreatedBy: "alice"},
	}
}

func fsmAttachRemote(t *testing.T, f *fsmState, op AttachRemoteChildOp) error {
	t.Helper()
	data, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	return f.applyAttachRemoteChild(data)
}

func fsmSetRemoteState(t *testing.T, f *fsmState, op RemoteChildStateOp) error {
	t.Helper()
	data, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	return f.applySetRemoteChildState(data)
}

func fsmTopicExists(t *testing.T, f *fsmState, name string) bool {
	t.Helper()
	found := false
	_ = f.view(func(tx *bolt.Tx) error {
		found = tx.Bucket(bucketTopics).Get([]byte(name)) != nil
		return nil
	})
	return found
}

// remoteChildFSM is a parent "orders" with 24h retention and a remote
// "b" in the registry.
func remoteChildFSM(t *testing.T) *fsmState {
	t.Helper()
	f := newFanoutFSM(t)
	fsmCreateTopicRetention(t, f, "orders", topic.MinRemoteSourceRetentionMs)
	fsmPutRemoteRecord(t, f, "b")
	return f
}

func TestRemoteChildAttachCreatesTheStubAndLinksIt(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatalf("attach: %v", err)
	}
	stub := fsmGetTopic(t, f, "orders-to-b")
	if !stub.IsRemoteChild() || stub.Parent != "orders" || stub.Partitions != 0 {
		t.Fatalf("stub = %+v, want a zero-partition remote child of orders", stub)
	}
	if stub.Owner != "" {
		t.Fatalf("stub owner = %q, want none: nobody gains rights over a stub by creating it", stub.Owner)
	}
	if stub.ID != "sid-orders-to-b" || stub.AttachEpoch != "ep-orders-to-b" || stub.CreatedAt != 1790640000 {
		t.Fatalf("stub identity = %q/%q/%d, want the proposer's", stub.ID, stub.AttachEpoch, stub.CreatedAt)
	}
	if !slices.Equal(stub.AttachOffsets, []int64{10, 20, 30}) {
		t.Fatalf("attach offsets = %v", stub.AttachOffsets)
	}
	r := stub.Remote
	if r.Name != "b" || r.Topic != "orders" || r.TargetID != "tid" || r.From != topic.RemoteFromAttach || r.Lanes != 1 || r.CreatedBy != "alice" {
		t.Fatalf("remote link = %+v", r)
	}
	parent := fsmGetTopic(t, f, "orders")
	if !parent.IsParent() || !slices.Equal(parent.Children, []string{"orders-to-b"}) {
		t.Fatalf("parent = %+v, want it to list the stub", parent)
	}
}

// A proposal cannot smuggle pause or skip state in with the attach.
func TestRemoteChildAttachStartsUnpausedWithNoSkip(t *testing.T) {
	f := remoteChildFSM(t)
	op := remoteAttachOp("orders", "orders-to-b", "b", "orders")
	op.Remote.Paused, op.Remote.PausedBy, op.Remote.Skip = true, "mallory", map[int][]int64{0: {5}}
	if err := fsmAttachRemote(t, f, op); err != nil {
		t.Fatal(err)
	}
	r := fsmGetTopic(t, f, "orders-to-b").Remote
	if r.Paused || r.PausedBy != "" || r.Skip != nil {
		t.Fatalf("remote link = %+v, want no pause and no skip", r)
	}
}

func TestRemoteChildAttachRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, f *fsmState)
		op     func() AttachRemoteChildOp
		sentry error
		want   string
	}{
		{
			name:   "stub name taken",
			setup:  func(t *testing.T, f *fsmState) { fsmCreateTopicRetention(t, f, "orders-to-b", 0) },
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders-to-b", "b", "orders") },
			sentry: ErrAlreadyExists,
		},
		{
			name:   "remote deleted meanwhile",
			setup:  func(t *testing.T, f *fsmState) { fsmDeleteRemoteRecord(t, f, "b") },
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders-to-b", "b", "orders") },
			sentry: errs.ErrRemoteChildConflict,
			want:   "does not exist",
		},
		{
			name: "parent is itself a child",
			setup: func(t *testing.T, f *fsmState) {
				fsmCreateTopicRetention(t, f, "root", 0)
				if err := fsmAttach(t, f, "root", "orders"); err != nil {
					t.Fatal(err)
				}
			},
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders-to-b", "b", "orders") },
			sentry: errs.ErrFanoutRoleConflict,
		},
		{
			name:   "parent missing",
			op:     func() AttachRemoteChildOp { return remoteAttachOp("nope", "orders-to-b", "b", "orders") },
			sentry: ErrNotFound,
		},
		{
			name: "108 children",
			setup: func(t *testing.T, f *fsmState) {
				for i := range topic.MaxChildrenPerParent {
					name := fmt.Sprintf("c%03d", i)
					fsmCreateTopicRetention(t, f, name, 0)
					if err := fsmAttach(t, f, "orders", name); err != nil {
						t.Fatal(err)
					}
				}
			},
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders-to-b", "b", "orders") },
			sentry: errs.ErrFanoutChildLimit,
		},
		{
			name: "16 remote children",
			setup: func(t *testing.T, f *fsmState) {
				for i := range topic.MaxRemoteChildrenPerParent {
					name := fmt.Sprintf("r%02d", i)
					if err := fsmAttachRemote(t, f, remoteAttachOp("orders", name, "b", "t"+name)); err != nil {
						t.Fatal(err)
					}
				}
			},
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders-to-b", "b", "orders") },
			sentry: errs.ErrRemoteChildLimit,
		},
		{
			name: "retention below the floor",
			setup: func(t *testing.T, f *fsmState) {
				fsmCreateTopicRetention(t, f, "short", topic.MinRemoteSourceRetentionMs-1)
			},
			op:     func() AttachRemoteChildOp { return remoteAttachOp("short", "short-to-b", "b", "short") },
			sentry: errs.ErrRemoteRetentionFloor,
		},
		{
			name: "delay does not fit the retention",
			op: func() AttachRemoteChildOp {
				op := remoteAttachOp("orders", "orders-to-b", "b", "orders")
				op.DelayMs = topic.MinRemoteSourceRetentionMs
				return op
			},
			sentry: errs.ErrFanoutDelayTooLong,
		},
		{
			name: "same remote topic linked twice",
			setup: func(t *testing.T, f *fsmState) {
				fsmCreateTopicRetention(t, f, "other", 0)
				if err := fsmAttachRemote(t, f, remoteAttachOp("other", "other-to-b", "b", "orders")); err != nil {
					t.Fatal(err)
				}
			},
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders-to-b", "b", "orders") },
			sentry: errs.ErrRemoteTargetLinked,
			want:   "other-to-b",
		},
		{
			name: "lanes out of range",
			op: func() AttachRemoteChildOp {
				op := remoteAttachOp("orders", "orders-to-b", "b", "orders")
				op.Remote.Lanes = 9
				return op
			},
			sentry: errs.ErrInvalidArgument,
		},
		{
			name: "unknown start point",
			op: func() AttachRemoteChildOp {
				op := remoteAttachOp("orders", "orders-to-b", "b", "orders")
				op.Remote.From = "latest"
				return op
			},
			sentry: errs.ErrInvalidArgument,
		},
		{
			name:   "own child",
			op:     func() AttachRemoteChildOp { return remoteAttachOp("orders", "orders", "b", "orders") },
			sentry: errs.ErrFanoutRoleConflict,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := remoteChildFSM(t)
			if tc.setup != nil {
				tc.setup(t, f)
			}
			op := tc.op()
			before := fsmTopicExists(t, f, op.Stub)
			err := fsmAttachRemote(t, f, op)
			if !errors.Is(err, tc.sentry) {
				t.Fatalf("attach error = %v, want %v", err, tc.sentry)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("attach error = %q, want it to mention %q", err, tc.want)
			}
			if !before && fsmTopicExists(t, f, op.Stub) {
				t.Fatalf("a refused attach created %q", op.Stub)
			}
		})
	}
}

// Retention 0 keeps records forever, so it satisfies the floor.
func TestRemoteChildAttachAcceptsRetentionForever(t *testing.T) {
	f := remoteChildFSM(t)
	fsmCreateTopicRetention(t, f, "forever", 0)
	if err := fsmAttachRemote(t, f, remoteAttachOp("forever", "forever-to-b", "b", "forever")); err != nil {
		t.Fatalf("attach under a keep-forever parent: %v", err)
	}
}

// Both orders of the attach-versus-remote-delete race: once the remote
// is gone the attach is refused, and once a stub names the remote the
// registry's delete sees it (remoteChildrenNaming).
func TestRemoteChildAndRemoteDeleteExcludeEachOther(t *testing.T) {
	f := remoteChildFSM(t)
	fsmDeleteRemoteRecord(t, f, "b")
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); !errors.Is(err, errs.ErrRemoteChildConflict) {
		t.Fatalf("attach after the remote was deleted: %v, want a 409", err)
	}

	fsmPutRemoteRecord(t, f, "b")
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	var naming []string
	if err := f.view(func(tx *bolt.Tx) error {
		var err error
		naming, err = remoteChildrenNaming(tx, "b")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(naming, []string{"orders/orders-to-b"}) {
		t.Fatalf("remoteChildrenNaming(b) = %v, want the stub", naming)
	}
}

func TestRemoteChildDetachDeletesTheStub(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	if err := fsmDetach(t, f, "orders", "orders-to-b"); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if fsmTopicExists(t, f, "orders-to-b") {
		t.Fatal("the stub survived its detach")
	}
	parent := fsmGetTopic(t, f, "orders")
	if parent.IsParent() || len(parent.Children) != 0 {
		t.Fatalf("parent = %+v, want standalone again", parent)
	}
}

func TestRemoteChildParentDeleteDeletesStubsAndFreesLocalChildren(t *testing.T) {
	f := remoteChildFSM(t)
	fsmCreateTopicRetention(t, f, "local", 0)
	if err := fsmAttach(t, f, "orders", "local"); err != nil {
		t.Fatal(err)
	}
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal("orders")
	if err := f.applyDeleteTopic(data); err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	if fsmTopicExists(t, f, "orders-to-b") {
		t.Fatal("the stub survived its parent")
	}
	if local := fsmGetTopic(t, f, "local"); local.IsChild() {
		t.Fatalf("local child = %+v, want standalone", local)
	}
}

func TestRemoteChildStubDeleteUnlinksItFromTheParent(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal("orders-to-b")
	if err := f.applyDeleteTopic(data); err != nil {
		t.Fatal(err)
	}
	if parent := fsmGetTopic(t, f, "orders"); len(parent.Children) != 0 || parent.IsParent() {
		t.Fatalf("parent = %+v, want the stub unlinked", parent)
	}
}

func fsmUpdateTopic(t *testing.T, f *fsmState, tp topic.Topic) error {
	t.Helper()
	data, err := json.Marshal(tp)
	if err != nil {
		t.Fatal(err)
	}
	return f.applyUpdateTopic(data)
}

func TestRemoteChildStubRefusesEveryTopicChange(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	stub := fsmGetTopic(t, f, "orders-to-b")
	for _, change := range []func(*topic.Topic){
		func(t *topic.Topic) { t.Partitions = 3 },
		func(t *topic.Topic) { t.RetentionMs = 7_200_000 },
		func(t *topic.Topic) { t.MaxInFlightPerPartition = 5 },
		func(t *topic.Topic) { t.Owner = "mallory" },
	} {
		next := stub
		change(&next)
		if err := fsmUpdateTopic(t, f, next); !errors.Is(err, errs.ErrRemoteStubImmutable) {
			t.Fatalf("update %+v: %v, want ErrRemoteStubImmutable", next, err)
		}
	}
	// An update that drops the link fields is refused too: it could not
	// remove the link anyway, and it would change the record.
	bare := stub
	bare.Remote = nil
	bare.Partitions = 1
	if err := fsmUpdateTopic(t, f, bare); !errors.Is(err, errs.ErrRemoteStubImmutable) {
		t.Fatalf("update without the link: %v", err)
	}
	// An update that changes nothing applies.
	if err := fsmUpdateTopic(t, f, stub); err != nil {
		t.Fatalf("no-op update: %v", err)
	}
	if got := fsmGetTopic(t, f, "orders-to-b"); got.Partitions != 0 || !got.IsRemoteChild() {
		t.Fatalf("stub after updates = %+v", got)
	}
}

func TestRemoteChildParentUpdatePreservesTheFloor(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	parent := fsmGetTopic(t, f, "orders")

	shrunk := parent
	shrunk.RetentionMs = topic.MinRemoteSourceRetentionMs - 1
	if err := fsmUpdateTopic(t, f, shrunk); !errors.Is(err, errs.ErrRemoteRetentionFloor) {
		t.Fatalf("shrink below the floor: %v", err)
	}
	grown := parent
	grown.RetentionMs = 3 * topic.MinRemoteSourceRetentionMs
	if err := fsmUpdateTopic(t, f, grown); err != nil {
		t.Fatalf("grow: %v", err)
	}
	forever := parent
	forever.RetentionMs = 0
	if err := fsmUpdateTopic(t, f, forever); err != nil {
		t.Fatalf("keep forever: %v", err)
	}
	back := parent
	back.RetentionMs = topic.MinRemoteSourceRetentionMs
	if err := fsmUpdateTopic(t, f, back); err != nil {
		t.Fatalf("shrink to the floor: %v", err)
	}
	// The link fields survive every parent update.
	if got := fsmGetTopic(t, f, "orders"); !slices.Equal(got.Children, []string{"orders-to-b"}) {
		t.Fatalf("parent children = %v", got.Children)
	}
}

func TestRemoteChildParentSchemaSkipsTheStub(t *testing.T) {
	f := remoteChildFSM(t)
	fsmCreateTopicRetention(t, f, "local", 0)
	if err := fsmAttach(t, f, "orders", "local"); err != nil {
		t.Fatal(err)
	}
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	for v := 1; v <= 2; v++ {
		if err := fsmPutSchema(t, f, "orders", v, []byte(`{"type":"object"}`)); err != nil {
			t.Fatalf("put schema v%d: %v", v, err)
		}
	}
	if err := f.view(func(tx *bolt.Tx) error {
		stub, err := loadSchemaHistory(tx, "orders-to-b")
		if err != nil {
			return err
		}
		local, err := loadSchemaHistory(tx, "local")
		if err != nil {
			return err
		}
		if len(stub) != 0 || len(local) != 2 {
			return fmt.Errorf("stub has %d versions, local child %d; want 0 and 2", len(stub), len(local))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteChildStateOpIsFieldScopedAndEpochChecked(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	base := RemoteChildStateOp{Parent: "orders", Stub: "orders-to-b", Epoch: "ep-orders-to-b"}

	pause := base
	pause.Pause = &RemotePauseState{Paused: true, Reason: "B maintenance", By: "alice", AtMs: 42}
	if err := fsmSetRemoteState(t, f, pause); err != nil {
		t.Fatalf("pause: %v", err)
	}
	r := fsmGetTopic(t, f, "orders-to-b").Remote
	if !r.Paused || r.PauseReason != "B maintenance" || r.PausedBy != "alice" || r.PausedAtMs != 42 || r.TargetID != "tid" {
		t.Fatalf("after pause: %+v", r)
	}

	lanes, target := 4, "tid-2"
	other := base
	other.Lanes, other.TargetID, other.Skip = &lanes, &target, &RemoteSkip{Partition: 2, Offset: 99}
	if err := fsmSetRemoteState(t, f, other); err != nil {
		t.Fatalf("lanes, target, skip: %v", err)
	}
	r = fsmGetTopic(t, f, "orders-to-b").Remote
	if !r.Paused || r.Lanes != 4 || r.TargetID != "tid-2" || !r.Skipped(2, 99) {
		t.Fatalf("after field-scoped change: %+v (the pause must survive)", r)
	}

	resume := base
	resume.Pause = &RemotePauseState{Paused: false}
	if err := fsmSetRemoteState(t, f, resume); err != nil {
		t.Fatal(err)
	}
	if r = fsmGetTopic(t, f, "orders-to-b").Remote; r.Paused || r.PauseReason != "" || r.PausedBy != "" || r.PausedAtMs != 0 {
		t.Fatalf("after resume: %+v", r)
	}

	stale := base
	stale.Epoch = "an-older-epoch"
	stale.Lanes = &lanes
	if err := fsmSetRemoteState(t, f, stale); !errors.Is(err, errs.ErrRemoteChildStale) {
		t.Fatalf("stale epoch: %v, want ErrRemoteChildStale", err)
	}

	bad := 0
	badLanes := base
	badLanes.Lanes = &bad
	if err := fsmSetRemoteState(t, f, badLanes); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("lanes 0: %v", err)
	}
	badReason := base
	badReason.Pause = &RemotePauseState{Paused: true, Reason: "line\nbreak"}
	if err := fsmSetRemoteState(t, f, badReason); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("control character in reason: %v", err)
	}

	local := base
	fsmCreateTopicRetention(t, f, "plain", 0)
	local.Stub = "plain"
	if err := fsmSetRemoteState(t, f, local); !errors.Is(err, ErrNotFound) {
		t.Fatalf("state op on a topic that is no stub: %v", err)
	}
}

// A local attach can neither take a stub as its child nor hang a child
// under a stub.
func TestRemoteChildStubCannotJoinALocalLink(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	fsmCreateTopicRetention(t, f, "other", 0)
	if err := fsmAttach(t, f, "other", "orders-to-b"); !errors.Is(err, errs.ErrFanoutRoleConflict) {
		t.Fatalf("stub as a local child: %v", err)
	}
	if err := fsmAttach(t, f, "orders-to-b", "other"); !errors.Is(err, errs.ErrFanoutRoleConflict) {
		t.Fatalf("stub as a parent: %v", err)
	}
}

// Through Raft: the store mints the stub ID and epoch, and lists the
// stubs a remote has.
func TestRemoteChildStoreAttachMintsIdentity(t *testing.T) {
	s := newRemoteChildTestStore(t)
	ctx := context.Background()
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, RetentionMs: topic.MinRemoteSourceRetentionMs}); err != nil {
		t.Fatal(err)
	}
	fsmPutRemoteRecord(t, s.fsm, "b")
	op := AttachRemoteChildOp{Parent: "orders", Stub: "orders-to-b", Remote: topic.RemoteLink{Name: "b", Topic: "orders"}}
	if err := s.AttachRemoteChild(ctx, op); err != nil {
		t.Fatalf("AttachRemoteChild: %v", err)
	}
	stub, err := s.GetTopic(ctx, "orders-to-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(stub.ID) != 16 || len(stub.AttachEpoch) != 16 || stub.CreatedAt == 0 {
		t.Fatalf("stub identity = %q/%q/%d, want minted values", stub.ID, stub.AttachEpoch, stub.CreatedAt)
	}
	links, err := s.RemoteChildrenOf("b")
	if err != nil || !slices.Equal(links, []string{"orders/orders-to-b"}) {
		t.Fatalf("RemoteChildrenOf(b) = %v, %v", links, err)
	}
	stubs, err := s.RemoteChildren()
	if err != nil || len(stubs) != 1 || stubs[0].Name != "orders-to-b" {
		t.Fatalf("RemoteChildren() = %v, %v", stubs, err)
	}
	if err := s.SetRemoteChildState(ctx, RemoteChildStateOp{
		Parent: "orders", Stub: "orders-to-b", Epoch: stub.AttachEpoch,
		Pause: &RemotePauseState{Paused: true},
	}); err != nil {
		t.Fatalf("SetRemoteChildState: %v", err)
	}
	if got, _ := s.GetTopic(ctx, "orders-to-b"); !got.Remote.Paused {
		t.Fatal("the pause did not apply")
	}
}

// newRemoteChildTestStore is a single-node store with an elected leader.
func newRemoteChildTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{NodeID: "rc-0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = s.DeleteTopic(context.Background(), "__probe__")
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return nil
}

// Two skips on one partition both hold: a second skip must not re-arm
// the first record, which a re-read, a restart or a partition move
// would otherwise send and block on again.
func TestRemoteChildSkipsOnOnePartitionAccumulate(t *testing.T) {
	f := remoteChildFSM(t)
	if err := fsmAttachRemote(t, f, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatal(err)
	}
	base := RemoteChildStateOp{Parent: "orders", Stub: "orders-to-b", Epoch: "ep-orders-to-b"}
	for _, off := range []int64{10, 12, 10} {
		op := base
		op.Skip = &RemoteSkip{Partition: 3, Offset: off}
		if err := fsmSetRemoteState(t, f, op); err != nil {
			t.Fatal(err)
		}
	}
	r := fsmGetTopic(t, f, "orders-to-b").Remote
	if !r.Skipped(3, 10) || !r.Skipped(3, 12) || r.Skipped(3, 11) || r.Skipped(2, 10) {
		t.Fatalf("skip = %v, want offsets 10 and 12 on partition 3 only", r.Skip)
	}
	if n := len(r.Skip[3]); n != 2 {
		t.Fatalf("skip = %v, want each offset once", r.Skip)
	}
	// Many skips in one slab all hold: the cap (one full slab, trimmed
	// lowest first by topic.WithSkip) is far above them.
	for off := int64(100); off < 150; off++ {
		op := base
		op.Skip = &RemoteSkip{Partition: 3, Offset: off}
		if err := fsmSetRemoteState(t, f, op); err != nil {
			t.Fatal(err)
		}
	}
	r = fsmGetTopic(t, f, "orders-to-b").Remote
	if n := len(r.Skip[3]); n != 52 || !r.Skipped(3, 10) || !r.Skipped(3, 100) || !r.Skipped(3, 149) {
		t.Fatalf("partition 3 keeps %d skips (%v), want all 52", n, r.Skip[3])
	}
}
