package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// produce-dispatch-commit#3 / storage-engine#2. Batches for one
// partition that queue behind a commit in progress ride one durable
// cycle together instead of paying one each, back to back. The
// partition's wake notifier fires once per high-watermark advance, so it
// counts the cycles.
func TestZZWP7aQueuedCommitsShareOneDurableCycle(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "first")); err != nil {
		t.Fatal(err)
	}
	log, err := e.logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	var advances atomic.Int32
	log.SetWakeNotifier(func() { advances.Add(1) })

	const callers, each = 5, 3
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	var wg sync.WaitGroup
	offsets := make([][]int64, callers)
	errs := make([]error, callers)
	for c := range callers {
		wg.Go(func() {
			offsets[c], errs[c] = e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, each, fmt.Sprint("c", c)))
		})
	}
	zzWP7aWaitStack(t, "every caller to queue", func(count func(string) int) bool {
		// One waiting for the lock and the rest queued behind it, or
		// every one of them on the lock.
		return count("(*Logs).lockProduce(") == callers ||
			count("(*Logs).lockProduce(") == 1 && count("(*Engine).commitCombined(") == callers
	})
	release()
	wg.Wait()

	seen := map[int64]bool{}
	for c := range callers {
		if errs[c] != nil {
			t.Fatalf("caller %d: %v", c, errs[c])
		}
		if len(offsets[c]) != each {
			t.Fatalf("caller %d: %d offsets, want %d", c, len(offsets[c]), each)
		}
		for i, off := range offsets[c] {
			if i > 0 && off != offsets[c][i-1]+1 {
				t.Fatalf("caller %d: offsets %v are not contiguous", c, offsets[c])
			}
			if seen[off] {
				t.Fatalf("offset %d handed to two callers", off)
			}
			seen[off] = true
		}
	}
	if got, want := log.HighWatermark(), int64(1+callers*each); got != want {
		t.Fatalf("high-watermark = %d, want %d", got, want)
	}
	for off := int64(1); off < log.HighWatermark(); off++ {
		key, _, payload, err := log.ReadKeyed(off)
		if err != nil {
			t.Fatal(err)
		}
		var c, i int
		if _, err := fmt.Sscanf(key, "c%d-%d", &c, &i); err != nil || offsets[c][i] != off {
			t.Fatalf("offset %d holds %s (%s), not the record its caller was told", off, key, payload)
		}
	}
	if got := advances.Load(); got > 1 {
		t.Fatalf("%d queued batches took %d durable cycles, want 1", callers, got)
	}
}

// A combined cycle that fails fails every caller in it, and leaves
// nothing behind: each retries by appending again and every record ends
// up in the log exactly once.
func TestZZWP7aCombinedCommitFailureFailsEveryCaller(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dataDir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond, SegmentBytes: 64}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 100, MaxAckedAhead: 100}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	ctx := context.Background()
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "a")); err != nil {
		t.Fatal(err)
	}
	// Fill the segment with the directory read-only: that commit lands
	// (its roll is deferred), and the next one fails on the roll.
	partitionDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := os.Chmod(partitionDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(partitionDir, 0o700) })
	if _, err := e.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{{Topic: "orders", Key: "b", Payload: []byte(`{"pad":"` + strings.Repeat("x", 64) + `"}`)}}); err != nil {
		t.Fatal(err)
	}

	const callers = 3
	batches := make([][]ingress.ProduceRecord, callers)
	for c := range batches {
		batches[c] = zzWP7aRecords("orders", "", 0, 2, fmt.Sprint("c", c))
	}
	commitAll := func() []error {
		release := zzWP7aHoldProduceLock(t, e, "orders", 0)
		errs := make([]error, callers)
		var wg sync.WaitGroup
		for c := range callers {
			wg.Go(func() { _, errs[c] = e.CommitAcceptedProduceBatch(ctx, batches[c]) })
		}
		zzWP7aWaitStack(t, "every caller to queue", func(count func(string) int) bool {
			return count("(*Logs).lockProduce(") == callers ||
				count("(*Logs).lockProduce(") == 1 && count("(*Engine).commitCombined(") == callers
		})
		release()
		wg.Wait()
		return errs
	}
	for c, err := range commitAll() {
		if err == nil {
			t.Fatalf("caller %d succeeded although the cycle's segment roll failed", c)
		}
	}
	if got := zzWP7aHWM(t, e, "orders", 0); got != 2 {
		t.Fatalf("high-watermark = %d after the failed cycle, want 2", got)
	}

	if err := os.Chmod(partitionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for c, err := range commitAll() {
		if err != nil {
			t.Fatalf("retry of caller %d: %v", c, err)
		}
	}
	partition := 0
	count := map[string]int{}
	for {
		msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{Partition: &partition})
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		count[msg.Key]++
		if err := e.Ack(ctx, "orders", decodeHandleForTest(t, msg.ReceiptHandle)); err != nil {
			t.Fatal(err)
		}
	}
	if len(count) != 2+2*callers {
		t.Fatalf("consumed %d distinct records, want %d: %v", len(count), 2+2*callers, count)
	}
	for key, n := range count {
		if n != 1 {
			t.Fatalf("record %s delivered %d times, want once", key, n)
		}
	}
}

// storage-engine#7. A large batch stamps its commit time and spends its
// encode loop before it reaches the produce lock; a small batch stamped
// a millisecond later gets there first. Commit times must not go
// backwards along the partition, or the fan-out delay gate, which stops
// at the first record not yet due, holds due records back.
func TestZZWP7aCommitTimesNeverGoBackwards(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	payload := make([]byte, 256)
	big := make([]ingress.ProduceRecord, 16384)
	for i := range big {
		big[i] = ingress.ProduceRecord{Topic: "orders", TargetPartition: 0, Key: fmt.Sprint(i), Payload: payload}
	}
	small := zzWP7aRecords("orders", "", 0, 1, "small")
	log, err := e.logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	for trial := range 5 {
		start := log.HighWatermark()
		bigDone := make(chan error, 1)
		go func() { _, err := e.CommitAcceptedProduceBatch(ctx, big); bigDone <- err }()
		time.Sleep(time.Millisecond)
		if _, err := e.CommitAcceptedProduceBatch(ctx, small); err != nil {
			t.Fatal(err)
		}
		if err := <-bigDone; err != nil {
			t.Fatal(err)
		}
		var prev int64
		for off := start; off < log.HighWatermark(); off++ {
			_, ts, _, err := log.ReadKeyed(off)
			if err != nil {
				t.Fatal(err)
			}
			if off > start && ts < prev {
				t.Fatalf("trial %d: offset %d committedAt %d < offset %d committedAt %d", trial, off, ts, off-1, prev)
			}
			prev = ts
		}
	}
}

// Batches queued into one cycle are appended oldest commit time first,
// and a later cycle never stamps below the newest time already in the
// partition: its envelopes are rewritten under the lock.
func TestZZWP7aCombinedCycleOrdersAndClampsCommitTimes(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	req := func(key string, committedAt int64) *commitRequest {
		return &commitRequest{
			ctx:         context.Background(),
			payloads:    [][]byte{storage.EncodeKeyedRecord(key, committedAt, []byte(`{}`)), storage.EncodeKeyedRecord(key, committedAt, []byte(`{}`))},
			committedAt: committedAt,
		}
	}
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	reqs := []*commitRequest{req("late", 2000), req("early", 1000), req("mid", 1500)}
	var wg sync.WaitGroup
	for i, r := range reqs {
		wg.Go(func() {
			if err := e.commitCombined("orders", 0, r); err != nil {
				t.Errorf("request %d: %v", i, err)
			}
		})
		// Queue them in this order.
		zzWP7aWaitStack(t, "the request to queue", func(count func(string) int) bool {
			return count("(*Engine).commitCombined(") == i+1
		})
	}
	release()
	wg.Wait()
	// A cycle of its own, stamped before everything already committed.
	if err := e.commitCombined("orders", 0, req("stale", 500)); err != nil {
		t.Fatal(err)
	}

	log, err := e.logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		key string
		ts  int64
	}{{"early", 1000}, {"early", 1000}, {"mid", 1500}, {"mid", 1500}, {"late", 2000}, {"late", 2000}, {"stale", 2000}, {"stale", 2000}}
	if got := log.HighWatermark(); got != int64(len(want)) {
		t.Fatalf("high-watermark = %d, want %d", got, len(want))
	}
	for off, w := range want {
		key, ts, _, err := log.ReadKeyed(int64(off))
		if err != nil {
			t.Fatal(err)
		}
		if key != w.key || ts != w.ts {
			t.Fatalf("offset %d = (%s, %d), want (%s, %d)", off, key, ts, w.key, w.ts)
		}
	}
	if reqs[1].first != 0 || reqs[2].first != 2 || reqs[0].first != 4 {
		t.Fatalf("first offsets early=%d mid=%d late=%d, want 0, 2, 4", reqs[1].first, reqs[2].first, reqs[0].first)
	}
}

// One caller in a combined cycle whose context ended is left out; the
// others still commit.
func TestZZWP7aCombinedCycleLeavesOutCallerThatGaveUp(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	cancelled, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var liveErr, goneErr error
	wg.Go(func() {
		_, liveErr = e.CommitAcceptedProduceBatch(context.Background(), zzWP7aRecords("orders", "", 0, 2, "live"))
	})
	zzWP7aWaitStack(t, "the first caller to queue", func(count func(string) int) bool {
		return count("(*Engine).commitCombined(") == 1
	})
	wg.Go(func() {
		_, goneErr = e.CommitAcceptedProduceBatch(cancelled, zzWP7aRecords("orders", "", 0, 2, "gone"))
	})
	zzWP7aWaitStack(t, "the second caller to queue", func(count func(string) int) bool {
		return count("(*Engine).commitCombined(") == 2
	})
	cancel()
	release()
	wg.Wait()
	if liveErr != nil {
		t.Fatalf("live caller: %v", liveErr)
	}
	if !errors.Is(goneErr, context.Canceled) {
		t.Fatalf("caller that gave up: err = %v, want context.Canceled", goneErr)
	}
	if got := zzWP7aHWM(t, e, "orders", 0); got != 2 {
		t.Fatalf("high-watermark = %d, want 2", got)
	}
}

// Many committers on a few partitions, under the race detector: every
// record lands once, at the offset its caller was told.
func TestZZWP7aGroupCommitUnderContention(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "inc", Partitions: 2}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	for p := range 2 { // warm the metadata caches before going concurrent
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc", p, 1, "warm")); err != nil {
			t.Fatal(err)
		}
	}
	const callers, rounds, each = 8, 10, 4
	type landed struct {
		p   int
		off int64
		key string
	}
	var mu sync.Mutex
	var all []landed
	var wg sync.WaitGroup
	for c := range callers {
		wg.Go(func() {
			p := c % 2
			for r := range rounds {
				recs := zzWP7aRecords("orders", "inc", p, each, "c"+string(rune('a'+c))+string(rune('a'+r)))
				offs, err := e.CommitAcceptedProduceBatch(ctx, recs)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for i, off := range offs {
					all = append(all, landed{p, off, recs[i].Key})
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	for p := range 2 {
		if got, want := zzWP7aHWM(t, e, "orders", p), int64(1+callers/2*rounds*each); got != want {
			t.Fatalf("partition %d high-watermark = %d, want %d", p, got, want)
		}
	}
	for _, l := range all {
		log, err := e.logs.Get("orders", l.p)
		if err != nil {
			t.Fatal(err)
		}
		key, _, _, err := log.ReadKeyed(l.off)
		if err != nil || key != l.key {
			t.Fatalf("partition %d offset %d holds %q (%v), caller was told %q", l.p, l.off, key, err, l.key)
		}
	}
}

func TestZZWP7aRestampKeyedRecords(t *testing.T) {
	keys := []string{"", "k", string(make([]byte, 300))} // a two-byte uvarint key length too
	payloads := make([][]byte, len(keys))
	for i, key := range keys {
		payloads[i] = storage.EncodeKeyedRecord(key, 1000, []byte(`{"v":1}`))
	}
	restampKeyedRecords(payloads, 2345)
	for i, env := range payloads {
		key, ts, payload, err := storage.DecodeKeyedRecord(env)
		if err != nil {
			t.Fatal(err)
		}
		if key != keys[i] || ts != 2345 || string(payload) != `{"v":1}` {
			t.Fatalf("record %d after restamp: key %q ts %d payload %s", i, key, ts, payload)
		}
	}
}

// ForgetTopic drops the topic's commit combiners too.
func TestZZWP7aForgetTopicDropsCombiners(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2}
	ms.topics["other"] = topic.Topic{Name: "other", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	for _, target := range []struct {
		name string
		p    int
	}{{"orders", 0}, {"orders", 1}, {"other", 0}} {
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords(target.name, "", target.p, 1, "x")); err != nil {
			t.Fatal(err)
		}
	}
	e.ForgetTopic("orders")
	e.combineMu.RLock()
	defer e.combineMu.RUnlock()
	if len(e.combiners) != 1 || e.combiners[partitionKey{"other", 0}] == nil {
		t.Fatalf("combiners after forgetting orders: %v, want only other/0", e.combiners)
	}
}
