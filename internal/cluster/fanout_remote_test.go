package cluster

// The remote child engine against a real target: every record lands at
// least once, the cursor never passes an unaccepted record, and every
// failure class stalls the link with its state named.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

// remoteRig is a source linked to a secure target through remote "b".
type remoteRig struct {
	src    *rigSource
	target *rigTarget
	entry  *remote.Entry
	stub   topic.Topic
}

type remoteRigOpts struct {
	rigSourceOpts
	targetPartitions int
	lanes            int
	delayMs          int64
	from             string
	password         string
	targetID         string
	// legacyTarget records no target ID, as an attach to a topic created
	// before topic IDs (v2.1 and earlier) does.
	legacyTarget  bool
	limits        domremote.Limits
	noTargetTopic bool
	grant         []string
}

func newRemoteRig(t *testing.T, o remoteRigOpts) *remoteRig {
	t.Helper()
	if o.targetPartitions == 0 {
		o.targetPartitions = 3
	}
	if o.password == "" {
		o.password = rigReplPass
	}
	if o.grant == nil {
		o.grant = []string{"orders"}
	}
	target := newRigTarget(t, true, o.grant...)
	targetID := o.targetID
	if !o.noTargetTopic {
		tt := target.createTopic(t, "orders", o.targetPartitions)
		if targetID == "" && !o.legacyTarget {
			targetID = tt.ID
		}
	}
	src := newRigSource(t, o.rigSourceOpts)
	e := entryFor(t, target, "b", o.password, 1, o.limits, nil)
	src.register(t, e, target, o.password)
	stub := src.attach(t, "b", "orders", targetID, o.lanes, o.delayMs, o.from)
	return &remoteRig{src: src, target: target, entry: e, stub: stub}
}

func (rg *remoteRig) waitDelivered(t *testing.T, want []topic.KeyedRecord, d time.Duration) []topic.KeyedRecord {
	t.Helper()
	var got []topic.KeyedRecord
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		got = rg.target.records(t, "orders")
		if len(missing(want, got)) == 0 {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	m := missing(want, got)
	t.Fatalf("%d of %d records never reached the target (first: %.60s)", len(m), len(want), m[0])
	return nil
}

func (rg *remoteRig) waitState(t *testing.T, p int, state string, d time.Duration) remoteCursorSnapshot {
	t.Helper()
	var snap remoteCursorSnapshot
	rigWait(t, "cursor state "+state, d, func() bool {
		var ok bool
		snap, ok = rg.src.cursorState(p)
		return ok && snap.state == state
	})
	return snap
}

func TestRemoteChildShipsKeyedAndKeylessRecordsByteForByte(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: 2}})
	rg.src.start()
	defer rg.src.stop()

	var want []topic.KeyedRecord
	want = append(want, rg.src.produce(t, 0, 300, 7, 0)...)
	want = append(want, rg.src.produce(t, 1, 250, 0, 1000)...)
	got := rg.waitDelivered(t, want, 20*time.Second)

	byPayload := map[string]topic.KeyedRecord{}
	for _, r := range got {
		byPayload[string(r.Payload)] = r
	}
	for _, w := range want {
		g := byPayload[string(w.Payload)]
		if g.Key != w.Key || !bytes.Equal(g.Payload, w.Payload) {
			t.Fatalf("record %s arrived as key %q payload %s", w.Payload, g.Key, g.Payload)
		}
	}
	rigWait(t, "cursors at the high watermark", 10*time.Second, func() bool {
		return rg.src.cursorOffset(t, 0) == 300 && rg.src.cursorOffset(t, 1) == 250
	})
	snap := rg.waitState(t, 0, topic.RemoteStateRunning, 5*time.Second)
	if snap.lastSuccessMs == 0 || snap.verifiedMs == 0 {
		t.Fatalf("snapshot = %+v, want a last success and a verified target", snap)
	}
}

// Keys keep their order through lanes: a key always rides one lane, and
// a lane sends one chunk at a time.
func TestRemoteChildLanesKeepPerKeyOrder(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{lanes: 4})
	rg.src.start()
	defer rg.src.stop()
	var want []topic.KeyedRecord
	for round := range 5 {
		want = append(want, rg.src.produce(t, 0, 400, 13, round*400)...)
	}
	got := rg.waitDelivered(t, want, 30*time.Second)
	last := map[string]int{}
	for _, r := range got {
		var seq int
		if _, err := fmt.Sscanf(string(r.Payload), `{"seq":%d`, &seq); err != nil {
			t.Fatal(err)
		}
		if prev, ok := last[r.Key]; ok && seq < prev {
			t.Fatalf("key %s: seq %d after %d", r.Key, seq, prev)
		}
		last[r.Key] = seq
	}
}

func TestRemoteChildHoldsWhileTheTargetIsDownThenDrains(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	first := rg.src.produce(t, 0, 50, 5, 0)
	rg.waitDelivered(t, first, 15*time.Second)

	rg.target.faults.set("down")
	more := rg.src.produce(t, 0, 80, 5, 100)
	snap := rg.waitState(t, 0, topic.RemoteStateUnavailable, 15*time.Second)
	_ = snap
	before := rg.src.cursorOffset(t, 0)
	if before > 50 {
		t.Fatalf("cursor advanced to %d while the target was down", before)
	}
	// Lag keeps moving while the link does not.
	rigWait(t, "lag seconds while down", 10*time.Second, func() bool {
		s, _ := rg.src.cursorState(0)
		return s.lagSeconds > 0 && s.headroom != nil && *s.headroom < float64(topic.MinRemoteSourceRetentionMs)/1000
	})
	rg.target.faults.set("")
	rg.waitDelivered(t, append(first, more...), 60*time.Second)
	rigWait(t, "cursor at the high watermark", 10*time.Second, func() bool { return rg.src.cursorOffset(t, 0) == 130 })
	rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)
}

func TestRemoteChildAuthFailedUntilTheCredentialChanges(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{password: "a-wrong-password-0123456789"})
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 30, 3, 0)
	rg.waitState(t, 0, topic.RemoteStateAuthFailed, 15*time.Second)
	if got := rg.target.records(t, "orders"); len(got) != 0 {
		t.Fatalf("%d records landed with a wrong password", len(got))
	}
	if off := rg.src.cursorOffset(t, 0); off > 0 {
		t.Fatalf("cursor advanced to %d on auth_failed", off)
	}
	// The gate sits at its 30 s ceiling; a new credential version
	// reopens it at once.
	fixed := entryFor(t, rg.target, "b", rigReplPass, 2, domremote.Limits{}, nil)
	start := time.Now()
	rg.src.lookup.Set(fixed)
	rg.waitDelivered(t, want, 10*time.Second)
	if waited := time.Since(start); waited > 8*time.Second {
		t.Fatalf("the corrected password took %s to apply; the gate must reopen at once", waited)
	}
}

func TestRemoteChildForbiddenThenGranted(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{grant: []string{"other"}, rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 20, 3, 0)
	rg.waitState(t, 0, topic.RemoteStateForbidden, 15*time.Second)
	if err := rg.target.store.SetUserGrants(context.Background(), rigReplUser,
		[]userGrant{{Action: "produce", Patterns: []string{"orders"}}}, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	rg.waitDelivered(t, want, 20*time.Second)
	rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)
}

// A missing target stalls the link in target_missing. A topic created
// in its place is a new topic, not the one the link recorded (here none):
// the link stops in target_replaced until an admin accepts it.
func TestRemoteChildTargetMissingThenCreated(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{noTargetTopic: true, rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 20, 3, 0)
	rg.waitState(t, 0, topic.RemoteStateTargetMissing, 15*time.Second)
	tt := rg.target.createTopic(t, "orders", 3)
	rg.waitState(t, 0, topic.RemoteStateTargetReplaced, 15*time.Second)
	if n := len(rg.target.records(t, "orders")); n != 0 {
		t.Fatalf("%d records landed in a target created after the attach", n)
	}
	rg.setState(t, metastore.RemoteChildStateOp{TargetID: &tt.ID})
	rg.waitDelivered(t, want, 20*time.Second)
}

// A record the target refuses blocks its lane at exactly that offset;
// an admin's skip drops it and nothing else.
func TestRemoteChildRejectedRecordBlocksThenSkip(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	schema := []byte(`{"type":"object","required":["seq"],"properties":{"seq":{"type":"integer"}}}`)
	if _, err := rg.target.broker.UpdateTopicSchema(context.Background(), "orders", schema, 0); err != nil {
		t.Fatal(err)
	}
	rg.src.start()
	defer rg.src.stop()
	good1 := rg.src.produce(t, 0, 10, 2, 0)
	rg.src.producePayload(t, 0, "bad", []byte(`{"not_seq":true}`))
	good2 := rg.src.produce(t, 0, 10, 2, 100)

	snap := rg.waitState(t, 0, topic.RemoteStateRejectedRecord, 20*time.Second)
	if snap.blocked == nil || snap.blocked.Offset != 10 || snap.blocked.Partition != 0 {
		t.Fatalf("blocked at %+v, want partition 0 offset 10", snap.blocked)
	}
	rg.waitDelivered(t, good1, 10*time.Second)
	if off := rg.src.cursorOffset(t, 0); off > 10 {
		t.Fatalf("cursor at %d passed the refused record at 10", off)
	}
	// A stale skip (another offset) changes nothing.
	rg.setState(t, metastore.RemoteChildStateOp{Skip: &metastore.RemoteSkip{Partition: 0, Offset: 9}})
	time.Sleep(time.Second)
	if s, _ := rg.src.cursorState(0); s.state != topic.RemoteStateRejectedRecord {
		t.Fatalf("a stale skip moved the lane: %+v", s)
	}
	rg.setState(t, metastore.RemoteChildStateOp{Skip: &metastore.RemoteSkip{Partition: 0, Offset: 10}})
	got := rg.waitDelivered(t, append(good1, good2...), 20*time.Second)
	for _, r := range got {
		if strings.Contains(string(r.Payload), "not_seq") {
			t.Fatal("the skipped record reached the target")
		}
	}
	rigWait(t, "cursor past the skipped record", 10*time.Second, func() bool { return rg.src.cursorOffset(t, 0) == 21 })
	if v := counterValue(rg.src.metrics.RemoteLink.SkippedRecordsTotal.WithLabelValues("orders", "orders-to-b")); v != 1 {
		t.Fatalf("skipped counter = %v, want 1", v)
	}
}

// A record over the target's per-message cap blocks as
// record_too_large: there is no single-produce fallback.
func TestRemoteChildRecordTooLarge(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	good := rg.src.produce(t, 0, 5, 2, 0)
	rg.src.producePayload(t, 0, "", bytes.Repeat([]byte{0xAB}, 1100<<10))
	snap := rg.waitState(t, 0, topic.RemoteStateRecordTooLarge, 20*time.Second)
	if snap.blocked == nil || snap.blocked.Offset != 5 {
		t.Fatalf("blocked at %+v, want offset 5", snap.blocked)
	}
	rg.waitDelivered(t, good, 5*time.Second)
}

func TestRemoteChildPauseStopsSendingResumeContinues(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	first := rg.src.produce(t, 0, 20, 2, 0)
	rg.waitDelivered(t, first, 15*time.Second)
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: true, Reason: "maintenance"}})
	rg.waitState(t, 0, topic.RemoteStatePaused, 10*time.Second)
	more := rg.src.produce(t, 0, 20, 2, 100)
	time.Sleep(time.Second)
	if n := len(rg.target.records(t, "orders")); n != 20 {
		t.Fatalf("%d records on the target while paused, want 20", n)
	}
	if off := rg.src.cursorOffset(t, 0); off != 20 {
		t.Fatalf("cursor %d while paused, want it kept at 20", off)
	}
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: false}})
	rg.waitDelivered(t, append(first, more...), 15*time.Second)
}

// A recreated target topic (a new ID) stops the link before anything
// lands in it; accepting the new target resumes it.
func TestRemoteChildTargetReplaced(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{targetID: "an-id-from-before", rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 10, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateTargetReplaced, 15*time.Second)
	if n := len(rg.target.records(t, "orders")); n != 0 {
		t.Fatalf("%d records landed in a replaced target", n)
	}
	tt, err := rg.target.store.GetTopic(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	id := tt.ID
	rg.setState(t, metastore.RemoteChildStateOp{TargetID: &id})
	rg.waitDelivered(t, want, 15*time.Second)
}

// A target check that errors while the remote's gate is closed is not
// the check the reopening gate owes: when the remote's name then answers
// from another cluster, the next probe checks it first and nothing lands
// there.
func TestRemoteChildErroredCheckLeavesTheReopenCheckDue(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 5, 1, 0), 15*time.Second)
	rg.target.awaitDispatched(t, 10*time.Second)
	landed := len(rg.target.records(t, "orders"))

	// The target goes down. The first check after the gate closed (the
	// probe's) errors; a moment later (after the transport's retry of
	// the GET has errored too), another cluster takes over the name.
	var swapping sync.Once
	swap := func(batch bool) {
		if !batch {
			swapping.Do(func() {
				time.AfterFunc(100*time.Millisecond, func() { rg.target.faults.set("otherID") })
			})
		}
	}
	rg.target.faults.onReset.Store(&swap)
	rg.target.faults.set("reset")
	rg.src.produce(t, 0, 5, 1, 100)
	rigWait(t, "target_replaced or a record in the other cluster", 20*time.Second, func() bool {
		snap, ok := rg.src.cursorState(0)
		return (ok && snap.state == topic.RemoteStateTargetReplaced) || len(rg.target.records(t, "orders")) > landed
	})
	rg.target.awaitDispatched(t, 10*time.Second)
	if n := len(rg.target.records(t, "orders")) - landed; n != 0 {
		t.Fatalf("%d records landed in another cluster before any check of it", n)
	}
}

// While the remote's gate is closed, a probe whose target check fails
// is answered by that check: the lane sends no chunk behind it. A wrong
// password then costs the target one failed login per node per backoff,
// and a dead target one request per probe, not two.
func TestRemoteChildProbeAnsweredByAFailedCheckSendsNoChunk(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: 1}})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 5, 1, 0), 15*time.Second)
	rg.target.awaitDispatched(t, 10*time.Second)

	rs := rg.src.runner.sender().remoteState("b")
	rg.target.faults.set("down")
	rg.src.produce(t, 0, 5, 1, 100)
	rigWait(t, "the gate to close", 15*time.Second, rs.gate.Closed)
	// Chunks already in flight when it closed are answered by now.
	time.Sleep(200 * time.Millisecond)
	posted, listings := rg.target.faults.posted.Load(), rg.target.faults.listings.Load()
	time.Sleep(4 * time.Second)
	if n := rg.target.faults.listings.Load() - listings; n == 0 {
		t.Fatal("no probe reached the target in 4s")
	}
	if n := rg.target.faults.posted.Load() - posted; n != 0 {
		t.Fatalf("%d chunks went out behind failed target checks while the gate was closed", n)
	}
}

// A target topic with a remote child of its own stops the link: no
// loops, no chains.
func TestRemoteChildTargetWithRemoteChildren(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	ctx := context.Background()
	registerRemote(t, rg.target.store, domremote.Record{Name: "c", ID: "rid-c", URL: "https://c.example", Username: "repl"}, rigRandomString(18))
	if err := rg.target.store.UpdateTopic(ctx, withRetention(t, rg.target.store, "orders", topic.MinRemoteSourceRetentionMs)); err != nil {
		t.Fatal(err)
	}
	targetOrders, err := rg.target.store.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if err := rg.target.store.AttachRemoteChild(ctx, metastore.AttachRemoteChildOp{
		Parent: "orders", ParentID: targetOrders.ID, Stub: "orders-to-c", Remote: topic.RemoteLink{Name: "c", Topic: "orders"},
	}); err != nil {
		t.Fatal(err)
	}
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 5, 1, 0)
	rg.waitState(t, 0, topic.RemoteStateTargetHasRemoteChildren, 15*time.Second)
	if n := len(rg.target.records(t, "orders")); n != 0 {
		t.Fatalf("%d records landed in a target with remote children", n)
	}
}

// A target that lacks the batch route answers Go's own 404 there while
// its listing answers 200: no_batch_produce, never edge.
func TestRemoteChildNoBatchProduce(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.target.faults.set("nobatch")
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 5, 1, 0)
	rg.waitState(t, 0, topic.RemoteStateNoBatchProduce, 15*time.Second)
	if v := counterValue(rg.src.metrics.RemoteLink.ErrorsTotal.WithLabelValues("b", topic.RemoteClassEdge)); v != 0 {
		t.Fatalf("edge counted %v times; the Go 404 must resolve to no_batch_produce", v)
	}
}

// An edge answer (an HTML 403 from something in front of the target)
// is retried like unavailable and counted as edge, never read as the
// target's verdict.
func TestRemoteChildEdgeAnswerIsRetried(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.target.faults.set("edge403")
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 10, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateUnavailable, 15*time.Second)
	rigWait(t, "edge counted", 10*time.Second, func() bool {
		return counterValue(rg.src.metrics.RemoteLink.ErrorsTotal.WithLabelValues("b", topic.RemoteClassEdge)) > 0
	})
	rg.target.faults.set("")
	rg.waitDelivered(t, want, 60*time.Second)
}

// With no room in the held budget, a lane that must wait keeps nothing
// and the slab is read again: resends, never loss.
func TestRemoteChildHeldBudgetOverflowRereads(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{heldBudget: -1}})
	rg.src.start()
	defer rg.src.stop()
	first := rg.src.produce(t, 0, 20, 2, 0)
	rg.waitDelivered(t, first, 15*time.Second)
	rg.target.faults.set("reset")
	more := rg.src.produce(t, 0, 40, 2, 100)
	time.Sleep(1500 * time.Millisecond)
	if used := rg.src.runner.remote.held.Used(); used != 0 {
		t.Fatalf("held %d bytes with a zero budget", used)
	}
	rg.target.faults.set("")
	rg.waitDelivered(t, append(first, more...), 60*time.Second)
}

// A delayed remote child sends a record only once its parent commit is
// the delay old.
func TestRemoteChildDelay(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{delayMs: 1500})
	rg.src.start()
	defer rg.src.stop()
	start := time.Now()
	want := rg.src.produce(t, 0, 5, 1, 0)
	time.Sleep(700 * time.Millisecond)
	if n := len(rg.target.records(t, "orders")); n != 0 {
		t.Fatalf("%d records arrived before the delay", n)
	}
	rg.waitDelivered(t, want, 15*time.Second)
	if since := time.Since(start); since < 1500*time.Millisecond {
		t.Fatalf("records arrived after %s, before the 1.5s delay", since)
	}
}

// At-least-once across crashes: the runner is killed right after the
// target accepted its k-th chunk since the restart, for k = 1..8 (a slab
// holds five chunks, so early kills land mid-slab and later ones past a
// persisted slab), then left to finish. The persisted offset never
// passes a record the target has not accepted, nothing is lost, and
// each crash resends at most one slab. The target's 202 means a chunk is
// durable in its ingress WAL; its own dispatcher commits it to the
// partition logs a moment later, so each check first waits for that.
func TestRemoteChildCrashMidSlabLosesNothing(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	var accepted atomic.Int64
	var killAt atomic.Int64
	var killed atomic.Bool
	fn := func() {
		if n := accepted.Add(1); n == killAt.Load() && killed.CompareAndSwap(false, true) {
			go rg.src.stop()
		}
	}
	rg.target.faults.onAccept.Store(&fn)
	// Enough records that every restart has chunks left to kill on: a
	// slab is 500 records, and a target that takes 1,000 messages a batch
	// ships a slab in one chunk.
	var want []topic.KeyedRecord
	for i := range 10 {
		want = append(want, rg.src.produce(t, 0, 4000, 9, i*4000)...)
	}
	crashes := 0
	for k := int64(1); k <= 8; k++ {
		accepted.Store(0)
		killAt.Store(k)
		killed.Store(false)
		rg.src.start()
		rigWait(t, "the kill", 20*time.Second, func() bool { return killed.Load() })
		for rg.src.running() {
			time.Sleep(5 * time.Millisecond)
		}
		crashes++
		persisted := rg.src.cursorOffset(t, 0)
		rg.target.awaitDispatched(t, 10*time.Second)
		have := payloadSet(rg.target.records(t, "orders"))
		for i := range int(max(persisted, 0)) {
			if have[string(want[i].Payload)] == 0 {
				t.Fatalf("after crash %d the cursor at %d passed record %d, which the target never accepted", crashes, persisted, i)
			}
		}
	}
	killAt.Store(-1)
	rg.src.start()
	defer rg.src.stop()
	got := rg.waitDelivered(t, want, 60*time.Second)
	dups := len(got) - len(want)
	if limit := crashes * 500; dups > limit {
		t.Fatalf("%d duplicates across %d crashes, want at most one slab (500) each", dups, crashes)
	}
	t.Logf("%d crashes, %d records, %d duplicates", crashes, len(want), dups)
}

// Decrypt once at the engine level: 1,000 slabs go out on one entry,
// the lookup is read but never rebuilt, and a Changed that carries no
// new credential makes B re-probe and re-check nothing.
func TestRemoteChildEngineReusesTheEntry(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	counting := &countingLookup{Lookup: rg.src.lookup}
	rg.src.runner.SetRemotes(counting, 64<<20)
	rg.src.start()
	defer rg.src.stop()
	var want []topic.KeyedRecord
	for i := range 1000 {
		want = append(want, rg.src.produce(t, 0, 1, 1, i)...)
		if i%100 == 99 {
			rg.waitDelivered(t, want, 20*time.Second)
		}
	}
	rg.waitDelivered(t, want, 20*time.Second)
	probes, listings := rg.target.faults.probes.Load(), rg.target.faults.listings.Load()
	rg.src.lookup.Set(rg.entry) // publish with the same entry
	more := rg.src.produce(t, 0, 10, 1, 5000)
	rg.waitDelivered(t, append(want, more...), 10*time.Second)
	if rg.target.faults.probes.Load() != probes {
		t.Fatalf("a Changed without a new credential re-probed the target (%d -> %d)", probes, rg.target.faults.probes.Load())
	}
	if rg.target.faults.listings.Load() > listings+1 {
		t.Fatalf("a Changed without a new credential re-checked the target (%d -> %d)", listings, rg.target.faults.listings.Load())
	}
	if n := counting.distinct(); n != 1 {
		t.Fatalf("the send path saw %d distinct entries, want the one entry reused", n)
	}
	if counting.gets.Load() < 100 {
		t.Fatalf("the lookup was read %d times; every chunk looks the remote up", counting.gets.Load())
	}
}

// A partition move carries a stub's cursor like a local child's: the
// source lists fanout-<stub>.offset among the sidecars, the destination
// installs it, and the move marker names the stub with its attach epoch
// so a cursor file lost on the way resumes instead of tail-anchoring.
func TestRemoteChildCursorTravelsWithAPartitionMove(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	want := rg.src.produce(t, 0, 300, 5, 0)
	rg.waitDelivered(t, want, 20*time.Second)
	rigWait(t, "the persisted cursor", 10*time.Second, func() bool { return rg.src.cursorOffset(t, 0) == 300 })
	rg.src.stop()

	files, err := storage.ListFanoutCursorFiles(storage.TopicPartitionDir(rg.src.dataDir, "orders", 0))
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := installSidecars(dst, files, 300); err != nil {
		t.Fatal(err)
	}
	cur, ok, err := storage.ReadFanoutCursor(dst, "orders-to-b")
	if err != nil || !ok || cur.NextOffset != 300 || cur.Epoch != rg.stub.AttachEpoch {
		t.Fatalf("installed stub cursor = %+v %v %v, want next 300 epoch %s", cur, ok, err, rg.stub.AttachEpoch)
	}

	mr := &MoveRunner{store: rg.src.store}
	children := mr.linkedChildren(context.Background(), "orders")
	if children["orders-to-b"] != rg.stub.AttachEpoch {
		t.Fatalf("move marker children = %v, want the stub with epoch %s", children, rg.stub.AttachEpoch)
	}
}

// With no room in the held budget, a lane blocked on a refused record
// makes the slab re-read after every stall interval. The other lanes'
// records, already accepted, are not sent again on each re-read: the
// target holds each good record once while the link waits on the skip.
func TestRemoteChildRereadDoesNotResendAcceptedLanes(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{lanes: 4, rigSourceOpts: rigSourceOpts{heldBudget: -1, stallRetry: 200 * time.Millisecond}})
	schema := []byte(`{"type":"object","required":["seq"],"properties":{"seq":{"type":"integer"}}}`)
	if _, err := rg.target.broker.UpdateTopicSchema(context.Background(), "orders", schema, 0); err != nil {
		t.Fatal(err)
	}
	good := rg.src.produce(t, 0, 40, 8, 0)
	rg.src.producePayload(t, 0, "bad", []byte(`{"not_seq":true}`))
	rg.src.start()
	defer rg.src.stop()

	rg.waitState(t, 0, topic.RemoteStateRejectedRecord, 20*time.Second)
	rg.waitDelivered(t, good, 10*time.Second)
	// Several stall intervals, each one a re-read of the slab.
	time.Sleep(2 * time.Second)
	got := rg.target.records(t, "orders")
	if len(got) != len(good) {
		t.Fatalf("the target holds %d records for %d good ones: re-reads sent accepted records again", len(got), len(good))
	}
	if v := counterValue(rg.src.metrics.RemoteLink.RereadsTotal.WithLabelValues("orders", "orders-to-b")); v == 0 {
		t.Fatal("no re-read counted while the held budget was full")
	}
	rg.setState(t, metastore.RemoteChildStateOp{Skip: &metastore.RemoteSkip{Partition: 0, Offset: 40}})
	rigWait(t, "cursor past the skipped record", 10*time.Second, func() bool { return rg.src.cursorOffset(t, 0) == 41 })
	if got := rg.target.records(t, "orders"); len(got) != len(good) {
		t.Fatalf("the target holds %d records after the skip, want %d", len(got), len(good))
	}
}

// A refusal of one record proves the target exists and takes the
// replicator's grant, so it clears a cursor stall such as target_missing:
// the listing then names the refused record, not a missing topic, and a
// narrowing step does not wait out a stall that no longer holds.
func TestRemoteChildRecordRefusalClearsAStaleStall(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{noTargetTopic: true, rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.src.producePayload(t, 0, "bad", []byte(`{"not_seq":true}`))
	rg.src.produce(t, 0, 10, 3, 0)
	rg.src.start()
	defer rg.src.stop()
	rg.waitState(t, 0, topic.RemoteStateTargetMissing, 15*time.Second)
	tt := rg.target.createTopic(t, "orders", 3)
	// A topic created after the attach is a new one: accept it.
	rg.setState(t, metastore.RemoteChildStateOp{TargetID: &tt.ID})
	schema := []byte(`{"type":"object","required":["seq"],"properties":{"seq":{"type":"integer"}}}`)
	if _, err := rg.target.broker.UpdateTopicSchema(context.Background(), "orders", schema, 0); err != nil {
		t.Fatal(err)
	}
	snap := rg.waitState(t, 0, topic.RemoteStateRejectedRecord, 20*time.Second)
	if snap.blocked == nil || snap.blocked.Offset != 0 {
		t.Fatalf("blocked at %+v, want offset 0", snap.blocked)
	}
}

// A pause applied between the read of the stub and the slab's version
// read must still stop the slab: the slab pairs the stub with the
// version read before it, so the first lane pass sees the change.
func TestRemoteChildSlabSeesAChangeAppliedAfterItsStubWasRead(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 5 * time.Second}})
	rg.src.start()
	defer rg.src.stop()
	var cur *remoteCursor
	rigWait(t, "the remote cursor", 10*time.Second, func() bool {
		cur = rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b")
		return cur != nil
	})
	version := rg.src.store.TopicVersion("orders-to-b")
	stub, err := rg.src.store.GetTopic(context.Background(), "orders-to-b")
	if err != nil {
		t.Fatal(err)
	}
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: true, Reason: "maintenance"}})
	recs := []topic.KeyedRecord{{Offset: 1000, Payload: []byte(`{"seq":1000}`), CommittedAtUnixMs: time.Now().UnixMilli()}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rg.src.runner.sender().commit(ctx, cur.key, stub, version, recs)
	rg.target.awaitDispatched(t, 5*time.Second)
	if got := rg.target.records(t, "orders"); len(got) != 0 {
		t.Fatalf("the slab sent %d records on a paused link", len(got))
	}
}

// A lane that looked its entry up before a password rotation reached
// this node must not send with the superseded credential: its 401 would
// close the remote's gate for every link although the cache already
// holds the corrected password.
func TestRemoteChildLaneNeverSendsWithASupersededCredential(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	s := rg.src.runner.sender()
	stale := entryFor(t, rg.target, "b", "an-old-password", 1, domremote.Limits{}, nil)
	rotated := entryFor(t, rg.target, "b", rigReplPass, 2, domremote.Limits{}, nil)
	rg.src.lookup.Set(stale)
	_, rs, _ := s.entry("b")
	rg.src.lookup.Set(rotated)
	if _, _, state := s.entry("b"); state != "" {
		t.Fatalf("lookup state %q", state)
	}
	key := fanoutCursorKey{parent: "orders", partition: 0, child: "orders-to-b", epoch: rg.stub.AttachEpoch, remote: true}
	cur := newRemoteCursor(key)
	sh := &slabShip{s: s, cur: cur, key: key, cancel: func() {}, stopWaits: func() {}, sendCtx: context.Background(), link: rg.stub}
	recs := []topic.KeyedRecord{{Offset: 0, Payload: []byte(`{"seq":0}`), CommittedAtUnixMs: time.Now().UnixMilli()}}
	lane := &laneShip{recs: recs, done: -1, cap: cur.chunkCap(0), backoff: sink.LaneBackoff(), b64: map[int64]bool{}}
	sh.sendChunk(context.Background(), lane, stale, rs, rg.stub.Remote, false, rs.gate.Epoch())
	if rs.gate.Closed() {
		t.Fatal("a send with the superseded credential closed the remote's gate")
	}
}

// A target on v3.1.0 serves no parent_id in its children listing but
// does serve the topic's id: a recreated target still stops the link.
func TestRemoteChildTargetReplacedOnAnOlderTarget(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{targetID: "an-id-from-before", rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.target.faults.set("v310")
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 10, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateTargetReplaced, 15*time.Second)
	if n := len(rg.target.records(t, "orders")); n != 0 {
		t.Fatalf("%d records landed in a replaced target", n)
	}
}
