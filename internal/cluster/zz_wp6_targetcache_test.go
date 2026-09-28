package cluster

import (
	"context"
	"fmt"
	"testing"
)

// produce-dispatch-commit#7: the dispatcher's target cache kept one entry
// per topic name it ever dispatched, deleted topics included. A deleted
// topic has no assignments, and an empty table is not worth caching.
func TestZZWP6TargetCacheDropsDeletedTopics(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	ctx := context.Background()
	d := NewProduceDispatcher(nil, store, "node-self", &fakeProduceCommitter{}, nil, nil, ProduceDispatcherConfig{})
	if _, err := d.dispatchTargetsForTopic("orders"); err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		name := fmt.Sprintf("tmp-%d", i)
		seedNamedProduceDispatchTopic(t, store, name, "node-self", 2)
		if _, err := d.dispatchTargetsForTopic(name); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteTopic(ctx, name); err != nil {
			t.Fatal(err)
		}
		// A record of the deleted topic still in the WAL looks it up
		// again after the delete.
		_, _ = d.dispatchTargetsForTopic(name)
	}
	d.targetMu.RLock()
	size := len(d.targetCache)
	d.targetMu.RUnlock()
	if size != 1 {
		t.Fatalf("target cache holds %d topics after 50 create and delete cycles, want 1 (the live one)", size)
	}
}
