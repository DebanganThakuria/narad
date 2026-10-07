package metastore

// Apply skips every entry at or below the database's applied index,
// whatever its op. A guard on the remote ops alone let a restart replay
// the plain ops around them (a stub delete is opDeleteTopic) on top of
// the final state and then skip the remote op that followed: a detach
// and re-attach under the same name lost the new stub on that node only.

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func reattachOp() AttachRemoteChildOp {
	op := remoteAttachOp("orders", "orders-to-b", "b", "orders")
	op.StubID, op.Epoch = "sid-2", "ep-2"
	return op
}

// requireReplayKeeps replays the log once and requires the same topics
// and registry, and the named stubs still there.
func requireReplayKeeps(t *testing.T, l *raftLog, stubs ...string) {
	t.Helper()
	before := dump(t, l.f)
	l.replay()
	after := dump(t, l.f)
	for _, name := range stubs {
		if !fsmTopicExists(t, l.f, name) {
			t.Fatalf("after one replay %s is gone\nbefore %s\nafter  %s", name, before, after)
		}
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("replay changed state\nbefore %s\nafter  %s", before, after)
	}
}

// Detach a remote child (child.delete deletes the stub with
// opDeleteTopic) and attach it again under the same name.
func TestStubDeleteThenReattachSurvivesLogReplay(t *testing.T) {
	l := &raftLog{t: t, f: remoteChildFSM(t)}
	if r := l.apply(opAttachRemoteChild, remoteAttachOp("orders", "orders-to-b", "b", "orders")); r != nil {
		t.Fatalf("attach: %v", r)
	}
	if r := l.apply(opDeleteTopic, "orders-to-b"); r != nil {
		t.Fatalf("delete stub: %v", r)
	}
	if r := l.apply(opAttachRemoteChild, reattachOp()); r != nil {
		t.Fatalf("reattach: %v", r)
	}
	requireReplayKeeps(t, l, "orders-to-b")
	if stub, err := getStub(l.f, "orders-to-b"); err != nil || stub.AttachEpoch != "ep-2" {
		t.Fatalf("stub after replay: %+v %v, want the re-attach's epoch ep-2", stub, err)
	}
}

// The failback shape (DESIGN ch. 10.6): the parent is deleted and
// recreated, then a remote child attached again, under the old name and
// under a new one.
func TestParentRecreateAndReattachSurvivesLogReplay(t *testing.T) {
	l := &raftLog{t: t, f: remoteChildFSM(t)}
	if r := l.apply(opAttachRemoteChild, remoteAttachOp("orders", "orders-to-b", "b", "orders")); r != nil {
		t.Fatalf("attach: %v", r)
	}
	if r := l.apply(opDeleteTopic, "orders-to-b"); r != nil {
		t.Fatalf("delete stub: %v", r)
	}
	if r := l.apply(opDeleteTopic, "orders"); r != nil {
		t.Fatalf("delete parent: %v", r)
	}
	if r := l.apply(opCreateTopic, topic.Topic{Name: "orders", ID: "id-orders-2", Partitions: 3, RetentionMs: topic.MinRemoteSourceRetentionMs, Owner: "olivia"}); r != nil {
		t.Fatalf("recreate parent: %v", r)
	}
	reattach := reattachOp()
	reattach.ParentID = "id-orders-2"
	if r := l.apply(opAttachRemoteChild, reattach); r != nil {
		t.Fatalf("reattach: %v", r)
	}
	dr := remoteAttachOp("orders", "orders-dr", "b", "orders-dr")
	dr.ParentID = "id-orders-2"
	if r := l.apply(opAttachRemoteChild, dr); r != nil {
		t.Fatalf("attach dr: %v", r)
	}
	requireReplayKeeps(t, l, "orders-to-b", "orders-dr")
}

// A plain topic deleted, then a stub created under its name.
func TestPlainTopicReplacedByAStubSurvivesLogReplay(t *testing.T) {
	l := &raftLog{t: t, f: remoteChildFSM(t)}
	if r := l.apply(opCreateTopic, topic.Topic{Name: "orders-to-b", ID: "id-plain", Partitions: 1, RetentionMs: topic.MinRemoteSourceRetentionMs}); r != nil {
		t.Fatalf("create plain: %v", r)
	}
	if r := l.apply(opDeleteTopic, "orders-to-b"); r != nil {
		t.Fatalf("delete plain: %v", r)
	}
	if r := l.apply(opAttachRemoteChild, reattachOp()); r != nil {
		t.Fatalf("attach: %v", r)
	}
	requireReplayKeeps(t, l, "orders-to-b")
}

// Plain ops and remote ops interleaved on the same names, with refused
// entries of both kinds: a replay after them changes nothing.
func TestMixedPlainAndRemoteOpsSurviveLogReplay(t *testing.T) {
	l := &raftLog{t: t, f: remoteChildFSM(t)}
	steps := []struct {
		op      opCode
		body    any
		refused bool
	}{
		{opAttachRemoteChild, remoteAttachOp("orders", "orders-to-b", "b", "orders"), false},
		{opCreateTopic, topic.Topic{Name: "audit", ID: "id-audit", Partitions: 1, RetentionMs: topic.MinRemoteSourceRetentionMs}, false},
		{opAttachChild, childLinkPayload{Parent: "orders", Child: "audit"}, false},
		{opDetachChild, childLinkPayload{Parent: "orders", Child: "audit"}, false},
		// Refused: the name is taken by the stub.
		{opCreateTopic, topic.Topic{Name: "orders-to-b", ID: "id-dup", Partitions: 1}, true},
		{opDeleteTopic, "orders-to-b", false},
		{opAttachRemoteChild, reattachOp(), false},
		{opUpdateTopic, topic.Topic{Name: "audit", ID: "id-audit", Partitions: 1, RetentionMs: topic.MinRemoteSourceRetentionMs + 1}, false},
		{opDeleteTopic, "audit", false},
		// Refused while audit is gone; applied against a later create it
		// would attach a child no replica has.
		{opAttachChild, childLinkPayload{Parent: "orders", Child: "audit"}, true},
		{opCreateTopic, topic.Topic{Name: "audit", ID: "id-audit-2", Partitions: 1, RetentionMs: topic.MinRemoteSourceRetentionMs}, false},
	}
	for i, s := range steps {
		r := l.apply(s.op, s.body)
		if (r != nil) != s.refused {
			t.Fatalf("step %d (op %d): result %v, want refused=%v", i, s.op, r, s.refused)
		}
	}
	requireReplayKeeps(t, l, "orders-to-b", "audit")
	if parent := fsmGetTopic(t, l.f, "orders"); len(parent.Children) != 1 || parent.Children[0] != "orders-to-b" {
		t.Fatalf("parent children after replay = %v, want [orders-to-b]", parent.Children)
	}
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return "127.0.0.1:" + strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}

func openLeaderStore(t *testing.T, dir, addr string) *Store {
	t.Helper()
	s, err := New(Config{NodeID: "replay-0", DataDir: dir, BindAddr: addr, AdvertiseAddr: addr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !s.AppliedCaughtUp() || !s.IsLeader() {
		if time.Now().After(deadline) {
			_ = s.Close()
			t.Fatal("store never caught up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return s
}

// The same through a real single-node Store: no snapshot yet, so the
// restart replays the whole log onto fsm.db.
func TestStubReattachSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	dir, addr := t.TempDir(), freeLoopbackAddr(t)
	s := openLeaderStore(t, dir, addr)
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "id-orders", Partitions: 3, RetentionMs: topic.MinRemoteSourceRetentionMs}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutRemote(ctx, PutRemoteOp{Record: record("b"), Salt: testSalt, SealedAtMs: 1, Actor: "alice"}); err != nil {
		t.Fatalf("put remote: %v", err)
	}
	if err := s.AttachRemoteChild(ctx, remoteAttachOp("orders", "orders-to-b", "b", "orders")); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := s.DeleteTopic(ctx, "orders-to-b"); err != nil {
		t.Fatalf("delete stub: %v", err)
	}
	if err := s.AttachRemoteChild(ctx, reattachOp()); err != nil {
		t.Fatalf("reattach: %v", err)
	}
	if snap := s.r.Stats()["last_snapshot_index"]; snap != "0" {
		t.Fatalf("last_snapshot_index = %s, want 0: the restart must replay from index 1", snap)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openLeaderStore(t, dir, addr)
	defer s2.Close()
	stub, err := s2.GetTopic(ctx, "orders-to-b")
	if err != nil || stub.AttachEpoch != "ep-2" {
		parent, _ := s2.GetTopic(ctx, "orders")
		t.Fatalf("after a restart: stub %+v err %v (parent children %v), want the re-attach's stub", stub, err, parent.Children)
	}
	if parent, err := s2.GetTopic(ctx, "orders"); err != nil || len(parent.Children) != 1 {
		t.Fatalf("after a restart: parent %+v err %v, want one remote child", parent, err)
	}
}
