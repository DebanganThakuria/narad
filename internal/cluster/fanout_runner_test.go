package cluster

// Integration-style tests for the fan-out cursor engine against a real
// metastore, a real messaging engine, and real partition logs — only
// the network is absent (single node, all partitions self-owned).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

type fanoutTestEnv struct {
	store   *metastore.Store
	engine  *messaging.Engine
	logs    *runtime.Logs
	dataDir string
}

func newFanoutTestEnv(t *testing.T) *fanoutTestEnv {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	dataDir := t.TempDir()

	for _, name := range []string{"parent", "child"} {
		if err := store.CreateTopic(ctx, topic.Topic{
			Name: name, Partitions: 3, RetentionMs: 3_600_000,
			VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
		}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
		for p := range 3 {
			if err := store.AssignPartition(ctx, name, p, "node-self"); err != nil {
				t.Fatalf("AssignPartition(%s, %d): %v", name, p, err)
			}
		}
	}

	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	engine := messaging.NewEngine(store, schema.NewAlwaysValid(), partition.NewHashRoundRobin(),
		offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	return &fanoutTestEnv{store: store, engine: engine, logs: logs, dataDir: dataDir}
}

func (env *fanoutTestEnv) newRunner(t *testing.T) *FanoutRunner {
	t.Helper()
	return NewFanoutRunner(env.store, "node-self", env.dataDir, env.engine, nil,
		partition.NewHashRoundRobin(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		FanoutConfig{Linger: time.Millisecond, ReconcileInterval: time.Hour})
}

// produceToParent commits n keyed records to one parent partition and
// returns the produced (key, payload) pairs in order.
func (env *fanoutTestEnv) produceToParent(t *testing.T, partitionIdx, n int, keyspace int, seqBase int) []topic.KeyedRecord {
	t.Helper()
	records := make([]ingress.ProduceRecord, 0, n)
	produced := make([]topic.KeyedRecord, 0, n)
	for i := range n {
		key := fmt.Sprintf("key-%d", i%keyspace)
		payload := fmt.Appendf(nil, `{"seq":%d}`, seqBase+i)
		records = append(records, ingress.ProduceRecord{
			Topic: "parent", Key: key, TargetPartition: partitionIdx, Payload: payload,
		})
		produced = append(produced, topic.KeyedRecord{Key: key, Payload: payload})
	}
	if _, err := env.engine.CommitAcceptedProduceBatch(context.Background(), records); err != nil {
		t.Fatalf("CommitAcceptedProduceBatch(parent/%d): %v", partitionIdx, err)
	}
	return produced
}

// childRecords reads every committed child record grouped by partition.
func (env *fanoutTestEnv) childRecords(t *testing.T) map[int][]topic.KeyedRecord {
	t.Helper()
	out := map[int][]topic.KeyedRecord{}
	for p := range 3 {
		log, err := env.logs.Get("child", p)
		if err != nil {
			t.Fatalf("logs.Get(child, %d): %v", p, err)
		}
		for off := int64(0); off < log.HighWatermark(); off++ {
			key, payload, err := log.ReadKeyed(off)
			if err != nil {
				t.Fatalf("ReadKeyed(child/%d@%d): %v", p, off, err)
			}
			out[p] = append(out[p], topic.KeyedRecord{Key: key, Payload: payload})
		}
	}
	return out
}

func (env *fanoutTestEnv) childTotal(t *testing.T) int {
	t.Helper()
	total := 0
	for _, recs := range env.childRecords(t) {
		total += len(recs)
	}
	return total
}

func (env *fanoutTestEnv) waitChildTotal(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if env.childTotal(t) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d fanned-out records; have %d", want, env.childTotal(t))
}

func TestFanoutRunnerFansOutWithReKeyingAndNoBackfill(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Records produced before the attach must never reach the child.
	env.produceToParent(t, 0, 5, 3, 1000)

	if err := env.store.AttachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, err := env.store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}

	runner := env.newRunner(t)
	runner.Reconcile(ctx)
	defer func() { cancel(); runner.wg.Wait() }()

	// Give the fresh cursors a moment to anchor at the parent tail,
	// then produce the post-attach traffic they must fan out.
	waitForCursorFiles(t, env, child.AttachEpoch, 3)

	wantP0 := env.produceToParent(t, 0, 10, 4, 0)
	wantP1 := env.produceToParent(t, 1, 6, 4, 100)
	env.waitChildTotal(t, 16)

	got := env.childRecords(t)
	total := 0
	byKey := map[string][]string{}
	keyPartition := map[string]int{}
	for p, recs := range got {
		for _, rec := range recs {
			total++
			byKey[rec.Key] = append(byKey[rec.Key], string(rec.Payload))
			if prev, ok := keyPartition[rec.Key]; ok && prev != p {
				t.Fatalf("key %q landed in child partitions %d and %d; per-key order broken", rec.Key, prev, p)
			}
			keyPartition[rec.Key] = p
		}
	}
	if total != 16 {
		t.Fatalf("child records = %d, want exactly 16 (no backfill of the 5 pre-attach records)", total)
	}

	// Per-key ordering: each key's payloads must appear in produced order.
	wantByKey := map[string][]string{}
	for _, rec := range wantP0 {
		wantByKey[rec.Key] = append(wantByKey[rec.Key], string(rec.Payload))
	}
	for _, rec := range wantP1 {
		wantByKey[rec.Key] = append(wantByKey[rec.Key], string(rec.Payload))
	}
	for key, want := range wantByKey {
		gotSeq := byKey[key]
		// Keys produced to both parent partitions may interleave across
		// partitions, but records from ONE parent partition stay ordered.
		// Our keyspace assigns each key to a single parent partition per
		// batch, and the two batches used disjoint seq ranges, so a
		// simple subsequence check per parent batch is exact: filter the
		// child's view down to each batch's seqs and compare.
		var fromP0, fromP1 []string
		for _, payload := range gotSeq {
			var seq int
			if _, err := fmt.Sscanf(payload, `{"seq":%d}`, &seq); err != nil {
				t.Fatalf("bad child payload %q", payload)
			}
			if seq < 100 {
				fromP0 = append(fromP0, payload)
			} else {
				fromP1 = append(fromP1, payload)
			}
		}
		var wantP0Seq, wantP1Seq []string
		for _, payload := range want {
			var seq int
			fmt.Sscanf(payload, `{"seq":%d}`, &seq)
			if seq < 100 {
				wantP0Seq = append(wantP0Seq, payload)
			} else {
				wantP1Seq = append(wantP1Seq, payload)
			}
		}
		if !equalStrings(fromP0, wantP0Seq) || !equalStrings(fromP1, wantP1Seq) {
			t.Fatalf("key %q order broken:\n got p0=%v p1=%v\nwant p0=%v p1=%v", key, fromP0, fromP1, wantP0Seq, wantP1Seq)
		}
	}

	// Cursor files carry the attach epoch and the advanced offset. The
	// file is persisted just AFTER the child commit becomes visible
	// (commit-before-advance), so poll briefly.
	deadline := time.Now().Add(5 * time.Second)
	for {
		cur, ok, err := storage.ReadFanoutCursor(storage.TopicPartitionDir(env.dataDir, "parent", 0), "child")
		if err != nil {
			t.Fatalf("ReadFanoutCursor(parent/0): %v", err)
		}
		if ok && cur.Epoch == child.AttachEpoch && cur.NextOffset == 15 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor = %+v (ok=%v), want epoch=%q next=15 (5 pre-attach + 10 fanned)", cur, ok, child.AttachEpoch)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFanoutRunnerDetachStopsAndCleansUp(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := env.store.AttachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, _ := env.store.GetTopic(ctx, "child")

	runner := env.newRunner(t)
	runner.Reconcile(ctx)
	defer func() { cancel(); runner.wg.Wait() }()
	waitForCursorFiles(t, env, child.AttachEpoch, 3)

	env.produceToParent(t, 0, 4, 2, 0)
	env.waitChildTotal(t, 4)

	if err := env.store.DetachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("DetachChild: %v", err)
	}
	// First pass cancels the cursors; subsequent passes reap them and
	// remove the dead link's cursor files.
	deadline := time.Now().Add(10 * time.Second)
	for {
		runner.Reconcile(ctx)
		runner.mu.Lock()
		remaining := len(runner.cursors)
		runner.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursors still running %d after detach", remaining)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for p := range 3 {
		dir := storage.TopicPartitionDir(env.dataDir, "parent", p)
		if _, ok, _ := storage.ReadFanoutCursor(dir, "child"); ok {
			t.Fatalf("cursor file for parent/%d survived detach", p)
		}
	}

	// Traffic produced while detached must not reach the child, even
	// after a re-attach (fresh epoch ⇒ fresh tail anchor).
	env.produceToParent(t, 0, 3, 2, 500)
	if err := env.store.AttachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("re-AttachChild: %v", err)
	}
	reChild, _ := env.store.GetTopic(ctx, "child")
	if reChild.AttachEpoch == child.AttachEpoch {
		t.Fatal("re-attach kept the old epoch")
	}
	runner.Reconcile(ctx)
	waitForCursorFiles(t, env, reChild.AttachEpoch, 3)

	env.produceToParent(t, 0, 2, 2, 900)
	env.waitChildTotal(t, 6) // 4 from before detach + 2 post-re-attach
	time.Sleep(200 * time.Millisecond)
	if got := env.childTotal(t); got != 6 {
		t.Fatalf("child records = %d, want 6 (the 3 detached-window records must not replay)", got)
	}
}

func TestFanoutRunnerResumesFromPersistedCursor(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())

	if err := env.store.AttachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, _ := env.store.GetTopic(ctx, "child")

	runner := env.newRunner(t)
	runner.Reconcile(ctx)
	waitForCursorFiles(t, env, child.AttachEpoch, 3)
	env.produceToParent(t, 0, 5, 2, 0)
	env.waitChildTotal(t, 5)

	// Stop the runner (clean shutdown), produce while it is down, then
	// start a fresh runner: it must resume from the persisted cursor and
	// fan out exactly the missed records — no loss, no replay.
	cancel()
	runner.wg.Wait()
	env.produceToParent(t, 0, 4, 2, 100)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runner2 := env.newRunner(t)
	runner2.Reconcile(ctx2)
	defer func() { cancel2(); runner2.wg.Wait() }()

	env.waitChildTotal(t, 9)
	time.Sleep(200 * time.Millisecond)
	if got := env.childTotal(t); got != 9 {
		t.Fatalf("child records = %d, want exactly 9 (no duplicates on clean restart)", got)
	}
}

// A cursor that exits on its own (e.g. a transient cursor-file write
// failure) while its link is still desired must be reaped and
// respawned by the next reconcile pass, resuming from its persisted
// offset — not left as a dead map entry that stalls fan-out forever.
func TestFanoutRunnerRespawnsSelfExitedCursor(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := env.store.AttachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, _ := env.store.GetTopic(ctx, "child")

	runner := env.newRunner(t)
	runner.Reconcile(ctx)
	defer func() { cancel(); runner.wg.Wait() }()
	waitForCursorFiles(t, env, child.AttachEpoch, 3)

	env.produceToParent(t, 0, 2, 2, 0)
	env.waitChildTotal(t, 2)

	// Kill the partition-0 cursor as if it had hit a fatal-looking
	// transient error; its done handle stays in the map.
	runner.mu.Lock()
	var victim *fanoutCursorHandle
	for k, h := range runner.cursors {
		if k.partition == 0 {
			victim = h
			h.cancel()
			break
		}
	}
	runner.mu.Unlock()
	if victim == nil {
		t.Fatal("no cursor for parent partition 0")
	}
	<-victim.done

	// The next reconcile must reap the dead handle and respawn.
	runner.Reconcile(ctx)
	env.produceToParent(t, 0, 3, 2, 100)
	env.waitChildTotal(t, 5)
}

func waitForCursorFiles(t *testing.T, env *fanoutTestEnv, epoch string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		found := 0
		for p := range 3 {
			dir := storage.TopicPartitionDir(env.dataDir, "parent", p)
			if cur, ok, _ := storage.ReadFanoutCursor(dir, "child"); ok && cur.Epoch == epoch {
				found++
			}
		}
		if found >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d cursor files with epoch %q", want, epoch)
}

func equalStrings(a, b []string) bool {
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
