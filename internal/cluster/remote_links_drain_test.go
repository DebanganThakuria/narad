package cluster

// The unshipped check a delete runs must see every record accepted
// (202) before the delete was asked for, whenever the dispatcher moves
// it between the two halves of the check, and whatever an earlier check
// read.

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// hookedStatsBroker is the broker as the leader's unshipped check sees
// it, with a hook that runs once, right after the cursor stats are read.
type hookedStatsBroker struct {
	rigBroker
	once  sync.Once
	after func()
}

func (b *hookedStatsBroker) FanoutCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, error) {
	stats, err := b.rigBroker.FanoutCursorStats(ctx, parent)
	b.once.Do(b.after)
	return stats, err
}

// drainRig ships 5 records, so the link's lag is 0, then takes the
// target away and accepts one more record that nothing dispatches: it
// is only in the ingress backlog.
func drainRig(t *testing.T) *remoteRig {
	t.Helper()
	rg := deleteRig(t)
	s := rg.src
	s.start()
	t.Cleanup(s.stop)
	rg.waitDelivered(t, s.produce(t, 0, 5, 2, 0), 15*time.Second)
	rigWait(t, "lag 0", 10*time.Second, func() bool { return s.cursorOffset(t, 0) == 5 })
	rg.target.faults.set("down")
	if _, err := s.broker.AcceptProduce(context.Background(), "orders", "k", []byte(`{"late":1}`)); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := s.ingress.PendingForTopic("src-orders-id", "orders", 100); n != 1 {
		t.Fatalf("backlog = %d, want the late record", n)
	}
	return rg
}

// The dispatcher commits the backlog record right after the cursor
// stats were read. Read backlog-first, the check still counts it.
func TestRemoteLinksDeleteCountsARecordDispatchedMidCheck(t *testing.T) {
	rg := drainRig(t)
	s := rg.src
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.links.d.Broker = &hookedStatsBroker{rigBroker: s.links.d.Broker.(rigBroker), after: func() {
		disp := NewProduceDispatcher(s.ingress, s.store, "node-self", s.broker, nil, rigLogger(), ProduceDispatcherConfig{PollInterval: 5 * time.Millisecond})
		go disp.Run(ctx)
		rigWait(t, "the late record dispatched", 10*time.Second, func() bool {
			n, _, _ := s.ingress.PendingForTopic("src-orders-id", "orders", 100)
			return n == 0
		})
	}}
	res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true})
	if res.Status != http.StatusConflict {
		t.Fatalf("delete with a record dispatched mid-check: %d %s, want 409 (the target holds %d of 6)",
			res.Status, res.Body, len(rg.target.records(t, "orders")))
	}
	backlog, _ := bodyOf(t, res)["dispatch_backlog"].(map[string]any)
	if backlog["node-self"] != float64(1) {
		t.Fatalf("409 body %s, want the record in node-self's backlog", res.Body)
	}
}

// A forced delete runs the unshipped query for its audit counts; a
// later delete without force, within seconds, must scan again rather
// than reuse that answer, taken before the record was accepted.
func TestRemoteLinksDeleteNeverReusesAnEarlierBacklogAnswer(t *testing.T) {
	rg := deleteRig(t)
	s := rg.src
	s.start()
	defer s.stop()
	ctx := context.Background()
	rg.waitDelivered(t, s.produce(t, 0, 5, 2, 0), 15*time.Second)
	rigWait(t, "cursor at 5", 10*time.Second, func() bool { return s.cursorOffset(t, 0) == 5 })

	// A second remote child of the same parent, drained.
	offsets, err := s.runner.AttachOffsetsMode(ctx, "orders", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.AttachRemoteChild(ctx, metastore.AttachRemoteChildOp{
		Parent: "orders", Stub: "orders-to-c", Offsets: offsets,
		Remote: topic.RemoteLink{Name: "b", Topic: "orders2"},
	}); err != nil {
		t.Fatal(err)
	}
	rigWait(t, "the second child's cursor file", 10*time.Second, func() bool {
		_, ok, _ := storage.ReadFanoutCursor(storage.TopicPartitionDir(s.dataDir, "orders", 0), "orders-to-c")
		return ok
	})
	if res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true, "force": true}); res.Status != http.StatusNoContent {
		t.Fatalf("forced delete: %d %s", res.Status, res.Body)
	}
	if _, err := s.broker.AcceptProduce(ctx, "orders", "k", []byte(`{"late":1}`)); err != nil {
		t.Fatal(err)
	}
	res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-c", "expect_remote": true})
	if res.Status != http.StatusConflict {
		t.Fatalf("delete without force with a record in the backlog: %d %s, want 409", res.Status, res.Body)
	}
}

// goroutinesIn counts goroutines whose stack mentions fn.
func goroutinesIn(fn string) int {
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, fn) {
			count++
		}
	}
	return count
}

// Two remote children of orders, both drained, the target down. Delete A
// starts the parent's check; right after its backlog scans, record R is
// accepted (202), and only then delete B is sent. B arrives while A's
// check runs, but that check scanned before R existed: B must not share
// its answer. It waits and runs its own, which finds R.
func TestRemoteLinksDeleteNeverSharesACheckThatStartedBeforeIt(t *testing.T) {
	rg := deleteRig(t)
	s := rg.src
	s.start()
	defer s.stop()
	ctx := context.Background()
	rg.waitDelivered(t, s.produce(t, 0, 5, 2, 0), 15*time.Second)
	rigWait(t, "cursor at 5", 10*time.Second, func() bool { return s.cursorOffset(t, 0) == 5 })

	offsets, err := s.runner.AttachOffsetsMode(ctx, "orders", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.AttachRemoteChild(ctx, metastore.AttachRemoteChildOp{
		Parent: "orders", Stub: "orders-to-c", Offsets: offsets,
		Remote: topic.RemoteLink{Name: "b", Topic: "orders2"},
	}); err != nil {
		t.Fatal(err)
	}
	rigWait(t, "the second child's cursor file", 10*time.Second, func() bool {
		_, ok, _ := storage.ReadFanoutCursor(storage.TopicPartitionDir(s.dataDir, "orders", 0), "orders-to-c")
		return ok
	})
	rg.target.faults.set("down")
	if n, _, _ := s.ingress.PendingForTopic("src-orders-id", "orders", 100); n != 0 {
		t.Fatalf("backlog = %d before the test, want 0", n)
	}

	del := func(child string) (nodewire.Response, error) {
		raw, _ := json.Marshal(map[string]any{"parent": "orders", "child": child, "expect_remote": true})
		return s.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{
			SubOp: nodewire.RemoteSubDetach, Actor: "alice", RequestID: "req-" + child, Body: raw,
		})
	}
	type result struct {
		res nodewire.Response
		err error
	}
	bDone := make(chan result, 1)
	s.links.d.Broker = &hookedStatsBroker{rigBroker: s.links.d.Broker.(rigBroker), after: func() {
		if _, err := s.broker.AcceptProduce(ctx, "orders", "k", []byte(`{"late":1}`)); err != nil {
			t.Error(err)
		}
		go func() {
			res, err := del("orders-to-c")
			bDone <- result{res, err}
		}()
		rigWait(t, "delete B to wait on A's check", 10*time.Second, func() bool {
			return goroutinesIn("(*RemoteLinks).unshippedChecked") >= 2
		})
	}}

	resA, err := del("orders-to-b")
	if err != nil {
		t.Fatal(err)
	}
	if resA.Status != http.StatusNoContent {
		t.Fatalf("delete A (before R): %d %s, want 204", resA.Status, resA.Body)
	}
	b := <-bDone
	if b.err != nil {
		t.Fatal(b.err)
	}
	if b.res.Status != http.StatusConflict || !strings.Contains(string(b.res.Body), `"dispatch_backlog"`) {
		pending, _, _ := s.ingress.PendingForTopic("src-orders-id", "orders", 100)
		t.Fatalf("delete B (sent after R's 202): %d %s, parent backlog %d; want 409 counting R", b.res.Status, b.res.Body, pending)
	}
}
