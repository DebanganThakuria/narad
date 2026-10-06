package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// replicaCaps stands in for a node's metastore replica: per-topic caps,
// per-topic versions and a metadata version that moves after any of
// them, as *metastore.Store's do.
type replicaCaps struct {
	mu       sync.Mutex
	caps     map[string]Caps
	versions map[string]uint64
	latest   atomic.Uint64
	resolves atomic.Int64
	fail     atomic.Bool
}

func newReplicaCaps() *replicaCaps {
	return &replicaCaps{caps: map[string]Caps{}, versions: map[string]uint64{}}
}

// apply records a caps change the way the Raft apply does: the record
// first, then the topic version, then the metadata version.
func (f *replicaCaps) apply(topic string, c Caps) {
	f.mu.Lock()
	f.caps[topic] = c
	v := f.latest.Load() + 1
	f.versions[topic] = v
	f.latest.Store(v)
	f.mu.Unlock()
}

func (f *replicaCaps) TopicVersion(topic string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.versions[topic]
}

func (f *replicaCaps) MetadataVersion() uint64 { return f.latest.Load() }

func (f *replicaCaps) resolve(_ context.Context, topic string) (Caps, error) {
	f.resolves.Add(1)
	if f.fail.Load() {
		return Caps{}, errors.New("replica read failed")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.caps[topic]
	if !ok {
		return Caps{}, errors.New("no such topic")
	}
	return c, nil
}

// followingInFlight is an InFlight on a non-leader owner: its caps come
// from the replica, which also tells it when a topic's record moved.
func followingInFlight(t *testing.T, maxInFlight int) (*InFlight, *replicaCaps) {
	t.Helper()
	replica := newReplicaCaps()
	replica.apply(testTopic, Caps{MaxInFlight: maxInFlight, MaxAckedAhead: 100})
	f := NewInFlight(replica.resolve, nil)
	f.SetCapsVersions(replica)
	withClock(f, 1000)
	return f, replica
}

func reserveOn(t *testing.T, f *InFlight, partition int) ReserveResult {
	t.Helper()
	r, err := f.ReserveNext(context.Background(), testTopic, partition, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext: %v", err)
	}
	return r
}

// A caps alter reaches this owner only through its replica, and
// RefreshCaps runs only on the node that ran the alter. A raise from 1
// to 10 must still reach the live shard at its next reserve, and a lower
// back to 1 too.
func TestCapsAlterReachesLiveShardsWithoutRefreshCaps(t *testing.T) {
	f, replica := followingInFlight(t, 1)
	if r := reserveOn(t, f, 0); !r.Reserved {
		t.Fatalf("first reserve: %+v", r)
	}
	if r := reserveOn(t, f, 0); r.SkipReason != "cap" {
		t.Fatalf("second reserve under cap 1: %+v", r)
	}
	replica.apply(testTopic, Caps{MaxInFlight: 10, MaxAckedAhead: 100})
	if r := reserveOn(t, f, 0); !r.Reserved {
		t.Fatalf("the live shard is still capped at 1 after the replica applied cap 10: %+v", r)
	}
	replica.apply(testTopic, Caps{MaxInFlight: 1, MaxAckedAhead: 100})
	if r := reserveOn(t, f, 0); r.SkipReason != "cap" {
		t.Fatalf("the live shard kept cap 10 after the replica applied cap 1: %+v", r)
	}
}

// While nothing moved a reserve resolves nothing, and a change to
// another topic is covered without resolving this one.
func TestCapsFollowResolvesNothingWhileTheTopicHoldsStill(t *testing.T) {
	f, replica := followingInFlight(t, 1000)
	reserveOn(t, f, 0)
	base := replica.resolves.Load()
	for range 100 {
		reserveOn(t, f, 0)
	}
	if got := replica.resolves.Load() - base; got != 0 {
		t.Fatalf("%d resolves over 100 reserves with nothing changed", got)
	}
	replica.apply("unrelated", Caps{MaxInFlight: 1, MaxAckedAhead: 1})
	for range 10 {
		reserveOn(t, f, 0)
	}
	if got := replica.resolves.Load() - base; got != 0 {
		t.Fatalf("%d resolves after another topic changed", got)
	}
	if f.capsChecked.Load() != replica.MetadataVersion() {
		t.Fatal("the pass did not record the latest metadata version as covered")
	}
}

// A refresh that could not read the record is retried after a pause.
func TestCapsFollowRetriesAFailedRead(t *testing.T) {
	f, replica := followingInFlight(t, 1)
	reserveOn(t, f, 0)
	replica.fail.Store(true)
	replica.apply(testTopic, Caps{MaxInFlight: 10, MaxAckedAhead: 100})
	if r := reserveOn(t, f, 0); r.Reserved {
		t.Fatalf("reserved past cap 1 while the replica read failed: %+v", r)
	}
	replica.fail.Store(false)
	f.capsRetryAt.Store(0) // the pause has passed
	if r := reserveOn(t, f, 0); !r.Reserved {
		t.Fatalf("a failed refresh was not retried: %+v", r)
	}
}

// A shard created after the alter resolves the new caps, and the shards
// created before it are refreshed.
func TestCapsFollowNewAndOldShardsAgree(t *testing.T) {
	f, replica := followingInFlight(t, 1)
	reserveOn(t, f, 0)
	replica.apply(testTopic, Caps{MaxInFlight: 2, MaxAckedAhead: 100})
	for _, p := range []int{1, 0} {
		if r := reserveOn(t, f, p); !r.Reserved {
			t.Fatalf("partition %d: %+v", p, r)
		}
	}
	for _, p := range []int{0, 1} {
		sh := f.shard(testTopic, p)
		sh.mu.Lock()
		got := sh.maxInFlight
		sh.mu.Unlock()
		if got != 2 {
			t.Fatalf("partition %d cap %d, want 2", p, got)
		}
	}
}

// Dropping a topic forgets the version its caps were resolved at, so a
// same-named successor starts clean.
func TestCapsFollowForgetsADroppedTopic(t *testing.T) {
	f, _ := followingInFlight(t, 1)
	reserveOn(t, f, 0)
	if _, ok := f.capsSeen.Load(testTopic); !ok {
		t.Fatal("setup: no version recorded for the topic")
	}
	f.DropTopic(testTopic)
	if _, ok := f.capsSeen.Load(testTopic); ok {
		t.Fatal("DropTopic kept the topic's caps version")
	}
}

// The reserve path stays allocation-free with the version source wired.
func TestCapsFollowKeepsTheReservePathAllocationFree(t *testing.T) {
	f, _ := followingInFlight(t, 1<<20)
	ctx := context.Background()
	reserveOn(t, f, 0)
	allocs := testing.AllocsPerRun(1000, func() {
		r, err := f.ReserveNext(ctx, testTopic, 0, testVT, 0)
		if err != nil || r.Reserved {
			t.Fatalf("%+v %v", r, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ReserveNext allocates %.1f per call with caps versions wired", allocs)
	}
}
