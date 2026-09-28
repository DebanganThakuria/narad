package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/partition"
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
	rec := &zzWP11BRecordingPeer{failPartition: -1, calls: map[int]int{}, got: map[int][]string{}}
	env := zzWP11BSetup(t, childParts, zzWP11BRemoteOwner, fakePeerClient{commitProduceBatchFn: rec.commit}, nil, true)

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
