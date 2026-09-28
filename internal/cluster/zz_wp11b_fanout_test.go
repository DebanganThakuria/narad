package cluster

// Regression tests and benchmarks for the fan-out slab commit: the
// child-partition buckets of a slab commit concurrently, and a failed
// bucket is retried on its own, never re-sending the buckets that
// already committed.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const zzWP11BRemoteOwner = "node-remote"

// zzWP11BStore is newTestStore for benchmarks too.
func zzWP11BStore(tb testing.TB) *metastore.Store {
	tb.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   "node-self",
		DataDir:  tb.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		tb.Fatalf("metastore.New() error = %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("timed out waiting for leader")
	return nil
}

// zzWP11BEnv is one parent partition owned by this node feeding a child
// whose partitions are all owned by childOwner (this node, or a remote
// member reached through the fake peer).
type zzWP11BEnv struct {
	store   *metastore.Store
	engine  *messaging.Engine
	dataDir string
	key     fanoutCursorKey
	runner  *FanoutRunner
}

func zzWP11BSetup(tb testing.TB, childParts int, childOwner string, peer peerClient, broker fanoutBroker, attach bool) *zzWP11BEnv {
	tb.Helper()
	ctx := context.Background()
	store := zzWP11BStore(tb)
	dataDir := tb.TempDir()
	if childOwner != "node-self" {
		if err := store.RegisterMember(ctx, metastore.Member{ID: childOwner, Addr: "10.0.0.2:1", Status: metastore.MemberAlive}); err != nil {
			tb.Fatalf("RegisterMember: %v", err)
		}
	}
	mk := func(name string, parts int, owner string) {
		if err := store.CreateTopic(ctx, topic.Topic{
			Name: name, Partitions: parts, RetentionMs: 7_200_000,
			VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
		}); err != nil {
			tb.Fatalf("CreateTopic(%s): %v", name, err)
		}
		for p := range parts {
			if err := store.AssignPartition(ctx, name, p, owner); err != nil {
				tb.Fatalf("AssignPartition(%s, %d): %v", name, p, err)
			}
		}
	}
	mk("parent", 1, "node-self")
	mk("child", childParts, childOwner)

	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	tb.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	engine := messaging.NewEngine(store, schema.NewAlwaysValid(), partition.NewHashRoundRobin(),
		offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	if broker == nil {
		broker = engine
	}
	env := &zzWP11BEnv{store: store, engine: engine, dataDir: dataDir}
	env.runner = NewFanoutRunner(store, "node-self", dataDir, broker, peer,
		partition.NewHashRoundRobin(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		FanoutConfig{Linger: time.Millisecond, ReconcileInterval: time.Hour})
	if attach {
		// After the runner exists, so the attach records the parent's
		// tail (0) as the anchor and nothing produced later is skipped.
		if err := store.AttachChild(ctx, "parent", "child", 0); err != nil {
			tb.Fatalf("AttachChild: %v", err)
		}
		child, err := store.GetTopic(ctx, "child")
		if err != nil {
			tb.Fatalf("GetTopic(child): %v", err)
		}
		env.key = fanoutCursorKey{parent: "parent", partition: 0, child: "child", epoch: child.AttachEpoch, anchor: attachAnchor(child, 0)}
	}
	return env
}

func (env *zzWP11BEnv) produce(tb testing.TB, keys []string, seqBase int) {
	tb.Helper()
	records := make([]ingress.ProduceRecord, 0, len(keys))
	for i, key := range keys {
		records = append(records, ingress.ProduceRecord{
			Topic: "parent", Key: key, TargetPartition: 0,
			Payload: fmt.Appendf(nil, `{"key":%q,"seq":%d}`, key, seqBase+i),
		})
	}
	if _, err := env.engine.CommitAcceptedProduceBatch(context.Background(), records); err != nil {
		tb.Fatalf("CommitAcceptedProduceBatch(parent): %v", err)
	}
}

func (env *zzWP11BEnv) waitCursorFile(tb testing.TB) {
	tb.Helper()
	dir := storage.TopicPartitionDir(env.dataDir, "parent", 0)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cur, ok, _ := storage.ReadFanoutCursor(dir, "child"); ok && cur.Epoch == env.key.epoch {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for the cursor file of epoch %q", env.key.epoch)
}

func zzWP11BSlab(n, payloadSize, seed int) []topic.KeyedRecord {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = 'x'
	}
	out := make([]topic.KeyedRecord, n)
	for i := range n {
		out[i] = topic.KeyedRecord{Key: "k-" + strconv.Itoa(seed) + "-" + strconv.Itoa(i), Payload: payload}
	}
	return out
}

// zzWP11BRecordingPeer collects every committed child record per
// partition, failing the first failN calls to failPartition.
type zzWP11BRecordingPeer struct {
	mu            sync.Mutex
	failPartition int
	failN         int
	calls         map[int]int
	got           map[int][]string
}

func (p *zzWP11BRecordingPeer) commit(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
	part := req.Records[0].TargetPartition
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[part]++
	if part == p.failPartition && p.calls[part] <= p.failN {
		return nodewire.Response{}, errors.New("child partition owner down")
	}
	for _, rec := range req.Records {
		if rec.TargetPartition != part {
			return nodewire.Response{}, fmt.Errorf("mixed bucket: record for %d in a batch for %d", rec.TargetPartition, part)
		}
		p.got[part] = append(p.got[part], string(rec.Payload))
	}
	return nodewire.Response{Status: http.StatusOK}, nil
}

func (p *zzWP11BRecordingPeer) snapshot() (map[int][]string, map[int]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	got := make(map[int][]string, len(p.got))
	for k, v := range p.got {
		got[k] = slices.Clone(v)
	}
	calls := make(map[int]int, len(p.calls))
	for k, v := range p.calls {
		calls[k] = v
	}
	return got, calls
}

// A child partition whose owner is down for two full commit rounds must
// not make the cursor re-send the buckets that already committed: each
// parent record reaches the child exactly once, including the records
// produced while the cursor was stuck (a re-read slab would have grown
// with them), and every key keeps its produce order.
func TestZZWP11BFanoutRetryResendsOnlyFailedBuckets(t *testing.T) {
	keys := make([]string, 0, 240)
	for i := range 240 {
		keys = append(keys, "key-"+strconv.Itoa(i%60))
	}
	rec := &zzWP11BRecordingPeer{
		failPartition: partition.NewHashRoundRobin().Pick("child", "key-0", 12),
		failN:         6, // two rounds of commitBucket's three quick attempts
		calls:         map[int]int{},
		got:           map[int][]string{},
	}
	env := zzWP11BSetup(t, 12, zzWP11BRemoteOwner, fakePeerClient{commitProduceBatchFn: rec.commit}, nil, true)

	ctx, cancel := context.WithCancel(context.Background())
	env.runner.Reconcile(ctx)
	defer func() { cancel(); env.runner.wg.Wait() }()
	env.waitCursorFile(t)

	env.produce(t, keys, 0)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, calls := rec.snapshot(); calls[rec.failPartition] >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the dead child partition was never tried")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Produced while the first slab is stuck on the dead partition.
	env.produce(t, keys, 1000)

	const want = 480
	deadline = time.Now().Add(20 * time.Second)
	for {
		got, _ := rec.snapshot()
		total := 0
		for _, v := range got {
			total += len(v)
		}
		if total >= want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child received %d records, want %d", total, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // let any trailing duplicate land

	got, calls := rec.snapshot()
	seen := map[string]int{}
	for _, payloads := range got {
		for _, payload := range payloads {
			seen[payload]++
		}
	}
	var dups []string
	for payload, n := range seen {
		if n > 1 {
			dups = append(dups, fmt.Sprintf("%s x%d", payload, n))
		}
	}
	if len(dups) > 0 {
		slices.Sort(dups)
		t.Fatalf("%d parent records reached the child more than once (committed buckets re-sent on retry), e.g. %s; commits per child partition %v",
			len(dups), strings.Join(dups[:min(len(dups), 5)], ", "), calls)
	}
	if len(seen) != want {
		t.Fatalf("child holds %d distinct records, want %d", len(seen), want)
	}
	keyPartition := map[string]int{}
	lastSeq := map[string]int{}
	for p, payloads := range got {
		for _, payload := range payloads {
			var key string
			var seq int
			if _, err := fmt.Sscanf(payload, `{"key":%q,"seq":%d}`, &key, &seq); err != nil {
				t.Fatalf("bad child payload %q: %v", payload, err)
			}
			if prev, ok := keyPartition[key]; ok && prev != p {
				t.Fatalf("key %q landed in child partitions %d and %d", key, prev, p)
			}
			keyPartition[key] = p
			if prev, ok := lastSeq[key]; ok && seq <= prev {
				t.Fatalf("key %q out of order in child partition %d: seq %d after %d", key, p, seq, prev)
			}
			lastSeq[key] = seq
		}
	}
	if calls[rec.failPartition] <= rec.failN {
		t.Fatalf("dead partition saw %d commits, want more than %d (it must recover)", calls[rec.failPartition], rec.failN)
	}
}

// A retry re-buckets only the records that did not commit, under the
// child's partition count at retry time: after the child grows from 4
// to 8 partitions mid-retry, the failed bucket's records land where the
// partitioner now sends their keys, and nothing that already committed
// is sent again.
func TestZZWP11BFanoutRetryRebucketsUnderNewPartitionCount(t *testing.T) {
	hash := partition.NewHashRoundRobin()
	failPartition := hash.Pick("child", "k-3-0", 4)
	rec := &zzWP11BRecordingPeer{failPartition: failPartition, failN: 3, calls: map[int]int{}, got: map[int][]string{}}
	env := zzWP11BSetup(t, 4, zzWP11BRemoteOwner, fakePeerClient{commitProduceBatchFn: rec.commit}, nil, true)

	batch := zzWP11BSlab(400, 8, 3)
	for i := range batch {
		batch[i].Payload = []byte(batch[i].Key)
	}
	done := make(chan bool, 1)
	go func() { done <- env.runner.commitBatch(context.Background(), env.key, batch) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, calls := rec.snapshot(); calls[failPartition] >= rec.failN {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the failing child partition was never tried three times")
		}
		time.Sleep(time.Millisecond)
	}
	ctx := context.Background()
	child, err := env.store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatal(err)
	}
	child.Partitions = 8
	if err := env.store.UpdateTopic(ctx, child); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	for p := 4; p < 8; p++ {
		if err := env.store.AssignPartition(ctx, "child", p, zzWP11BRemoteOwner); err != nil {
			t.Fatalf("AssignPartition(child, %d): %v", p, err)
		}
	}
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("commitBatch gave up")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("commitBatch never finished")
	}

	got, _ := rec.snapshot()
	seen := map[string]int{}
	for p, keys := range got {
		for _, key := range keys {
			seen[key]++
			old := hash.Pick("child", key, 4)
			switch {
			case old != failPartition && p != old:
				t.Fatalf("record %q committed first time round belongs in partition %d, found in %d", key, old, p)
			case old == failPartition && p != hash.Pick("child", key, 8):
				t.Fatalf("retried record %q in partition %d, want %d under 8 partitions", key, p, hash.Pick("child", key, 8))
			}
		}
	}
	for _, r := range batch {
		if n := seen[r.Key]; n != 1 {
			t.Fatalf("record %q reached the child %d times, want exactly once", r.Key, n)
		}
	}
}

// The child-partition buckets of one slab commit concurrently: a slab
// that touches 12 remote child partitions has all 12 commits in flight
// at once, instead of paying 12 round trips back to back.
func TestZZWP11BFanoutCommitBatchBucketsRunConcurrently(t *testing.T) {
	const childParts = 12
	var inflight, peak atomic.Int64
	peer := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			cur := peak.Load()
			if n <= cur || peak.CompareAndSwap(cur, n) {
				break
			}
		}
		// Hold until every bucket is in flight (or a serial caller
		// gives up waiting): 200ms per bucket when serial.
		deadline := time.Now().Add(200 * time.Millisecond)
		for inflight.Load() < childParts && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
	env := zzWP11BSetup(t, childParts, zzWP11BRemoteOwner, peer, nil, true)

	batch := zzWP11BSlab(4096, 64, 1)
	touched := map[int]bool{}
	hash := partition.NewHashRoundRobin()
	for _, rec := range batch {
		touched[hash.Pick("child", rec.Key, childParts)] = true
	}
	if len(touched) != childParts {
		t.Fatalf("slab touches %d child partitions, want %d", len(touched), childParts)
	}
	start := time.Now()
	if !env.runner.commitBatch(context.Background(), env.key, batch) {
		t.Fatal("commitBatch failed")
	}
	if got := peak.Load(); got != childParts {
		t.Fatalf("peak concurrent child commits = %d, want %d (buckets committed one at a time, took %v)", got, childParts, time.Since(start))
	}
}

// zzWP11BBroker is a local child "owner" that commits instantly (or
// after delay), so the benchmarks measure the slab commit's own work.
type zzWP11BBroker struct {
	delay   time.Duration
	records atomic.Int64
}

// ReadFanoutSlab serves only the attach resolver's tail read: an empty
// parent.
func (b *zzWP11BBroker) ReadFanoutSlab(_ context.Context, _ string, _ int, opts topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	if opts.FromOffset != topic.FanoutTailOffset {
		return topic.FanoutSlab{}, errors.New("zzWP11BBroker serves only tail reads")
	}
	return topic.FanoutSlab{}, nil
}

func (b *zzWP11BBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.records.Add(int64(len(records)))
	return nil, nil
}

func zzWP11BBenchCommit(b *testing.B, env *zzWP11BEnv, n int) {
	b.Helper()
	ctx := context.Background()
	batches := make([][]topic.KeyedRecord, 4)
	for i := range batches {
		batches[i] = zzWP11BSlab(n, 256, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		if !env.runner.commitBatch(ctx, env.key, batches[i%len(batches)]) {
			b.Fatal("commitBatch failed")
		}
		i++
	}
}

// BenchmarkZZWP11BFanoutCommitBatch measures one slab commit.
//   - cpu: instant local child commits, so only the bucketing and
//     dispatch cost shows.
//   - remote: every child partition on a peer answering after 5ms.
//   - local-fsync: the real engine committing to local child partitions.
func BenchmarkZZWP11BFanoutCommitBatch(b *testing.B) {
	for _, c := range []int{1, 12} {
		for _, n := range []int{64, 4096} {
			b.Run(fmt.Sprintf("cpu/C=%d/n=%d", c, n), func(b *testing.B) {
				env := zzWP11BSetup(b, c, "node-self", nil, &zzWP11BBroker{}, true)
				zzWP11BBenchCommit(b, env, n)
			})
		}
	}
	for _, c := range []int{3, 12} {
		b.Run(fmt.Sprintf("remote/C=%d/n=4096", c), func(b *testing.B) {
			peer := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
				time.Sleep(5 * time.Millisecond)
				return nodewire.Response{Status: http.StatusOK}, nil
			}}
			env := zzWP11BSetup(b, c, zzWP11BRemoteOwner, peer, nil, true)
			zzWP11BBenchCommit(b, env, 4096)
		})
	}
	for _, c := range []int{3, 12} {
		b.Run(fmt.Sprintf("local-fsync/C=%d/n=4096", c), func(b *testing.B) {
			env := zzWP11BSetup(b, c, "node-self", nil, nil, true)
			zzWP11BBenchCommit(b, env, 4096)
		})
	}
}
