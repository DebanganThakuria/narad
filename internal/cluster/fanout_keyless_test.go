package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

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

// A keyless record has no key to hash into the child, so fan-out keeps
// it on its parent partition's index when the child has the parent's
// partition count, the replica pattern's shape: create-as-child places
// child partition p away from the owner of parent partition p, so the
// record's two copies land on different nodes, as a keyed record's do
// (its key hashes to the same index in both topics). Keyed records in
// the same slab still hash. Placing keyless records round-robin in the
// child, independently of the parent partition, could put a record's
// child copy on the node that holds its parent copy.
func TestFanoutKeepsAKeylessRecordOnItsParentPartitionIndex(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx := context.Background()
	if err := env.store.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, err := env.store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}
	runner := env.newRunner(t)

	const partitions = 3
	hash := partition.NewHashRoundRobin()
	wantKeyed := map[string]int{}
	for p := range partitions {
		var slab []topic.KeyedRecord
		for i := range 9 {
			rec := topic.KeyedRecord{Payload: fmt.Appendf(nil, `{"parent":%d,"seq":%d}`, p, i)}
			if i%3 == 2 {
				rec.Key = fmt.Sprintf("key-%d-%d", p, i)
				wantKeyed[rec.Key] = hash.Pick("child", rec.Key, partitions)
			}
			slab = append(slab, rec)
		}
		key := fanoutCursorKey{parent: "parent", partition: p, child: "child", epoch: child.AttachEpoch}
		if !runner.commitBatch(ctx, key, slab) {
			t.Fatalf("commitBatch(parent partition %d) failed", p)
		}
	}

	keyless := 0
	for p, recs := range env.childRecords(t) {
		for _, rec := range recs {
			if rec.Key != "" {
				if want := wantKeyed[rec.Key]; p != want {
					t.Fatalf("keyed record %q in child partition %d, want %d (its hash)", rec.Key, p, want)
				}
				continue
			}
			keyless++
			var parent, seq int
			if _, err := fmt.Sscanf(string(rec.Payload), `{"parent":%d,"seq":%d}`, &parent, &seq); err != nil {
				t.Fatalf("bad child payload %q: %v", rec.Payload, err)
			}
			if parent != p {
				t.Fatalf("keyless record %s from parent partition %d landed in child partition %d", rec.Payload, parent, p)
			}
		}
	}
	if keyless != 6*partitions {
		t.Fatalf("child holds %d keyless records, want %d", keyless, 6*partitions)
	}
}

// When the child's partition count differs from the parent's there is
// no index to keep (and no placement promise to keep it for): keyless
// records are spread round-robin over all the child's partitions, not
// piled onto the parent partition's index.
func TestFanoutSpreadsKeylessRecordsWhenPartitionCountsDiffer(t *testing.T) {
	const childParts = 4
	rec := &recordingChildPeer{failPartition: -1, calls: map[int]int{}, got: map[int][]string{}}
	env := newKeylessFanoutEnv(t, childParts, keylessRemoteOwner, fakePeerClient{commitProduceBatchFn: rec.commit}, nil, true)

	slab := make([]topic.KeyedRecord, 40)
	for i := range slab {
		slab[i] = topic.KeyedRecord{Payload: fmt.Appendf(nil, `{"seq":%d}`, i)}
	}
	if !env.runner.commitBatch(context.Background(), env.key, slab) {
		t.Fatal("commitBatch failed")
	}
	got, _ := rec.snapshot()
	for p := range childParts {
		if n := len(got[p]); n != len(slab)/childParts {
			t.Fatalf("child partition %d got %d of %d keyless records, want %d each",
				p, n, len(slab), len(slab)/childParts)
		}
	}
}

const keylessRemoteOwner = "node-remote"

// keylessFanoutStore is a single-node metastore that has elected itself leader.
func keylessFanoutStore(tb testing.TB) *metastore.Store {
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

// keylessFanoutEnv is one parent partition owned by this node feeding a child
// whose partitions are all owned by childOwner (this node, or a remote
// member reached through the fake peer).
type keylessFanoutEnv struct {
	store   *metastore.Store
	engine  *messaging.Engine
	dataDir string
	key     fanoutCursorKey
	runner  *FanoutRunner
}

func newKeylessFanoutEnv(tb testing.TB, childParts int, childOwner string, peer peerClient, broker fanoutBroker, attach bool) *keylessFanoutEnv {
	tb.Helper()
	ctx := context.Background()
	store := keylessFanoutStore(tb)
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
	env := &keylessFanoutEnv{store: store, engine: engine, dataDir: dataDir}
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

// recordingChildPeer collects every committed child record per
// partition, failing the first failN calls to failPartition.
type recordingChildPeer struct {
	mu            sync.Mutex
	failPartition int
	failN         int
	calls         map[int]int
	got           map[int][]string
}

func (p *recordingChildPeer) commit(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
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

func (p *recordingChildPeer) snapshot() (map[int][]string, map[int]int) {
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
