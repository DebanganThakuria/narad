package messaging

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// The dispatcher's estimate for a partition with no shard yet takes the
// frontier from consumer.ahead as well as consumer.offset, as shard
// recovery does: the offset committer may carry the frontier in the
// ahead record alone.
func TestZZWP7aConsumableReadsPersistedAheadRecord(t *testing.T) {
	dataDir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngineWithDir(t, dataDir, ms, nil, nil)
	if _, err := e.CommitAcceptedProduceBatch(context.Background(), zzWP7aRecords("orders", "", 0, 20, "r")); err != nil {
		t.Fatal(err)
	}
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	// Frontier 14, with 16 and 17 acked ahead of the hole at 15.
	if err := storage.WriteConsumerAhead(dir, 0, 1, 14, []int64{16, 17}); err != nil {
		t.Fatal(err)
	}
	if got, want := e.dispatch.consumable("orders", []int{0}), 20-15-2; got != want {
		t.Fatalf("consumable = %d, want %d (records 15, 18 and 19)", got, want)
	}
	// A consumer.offset behind the ahead record's frontier does not pull
	// the estimate back.
	if err := storage.WriteConsumerOffset(dir, 9); err != nil {
		t.Fatal(err)
	}
	if got, want := e.dispatch.consumable("orders", []int{0}), 20-15-2; got != want {
		t.Fatalf("consumable with a lagging consumer.offset = %d, want %d", got, want)
	}
}

func TestZZWP7aPersistedFrontier(t *testing.T) {
	dir := t.TempDir()
	if next, ahead := persistedFrontier(dir); next != 0 || ahead != 0 {
		t.Fatalf("no files: (%d, %d), want (0, 0)", next, ahead)
	}
	if err := storage.WriteConsumerOffset(dir, 9); err != nil {
		t.Fatal(err)
	}
	if next, ahead := persistedFrontier(dir); next != 10 || ahead != 0 {
		t.Fatalf("consumer.offset only: (%d, %d), want (10, 0)", next, ahead)
	}
	if err := storage.WriteConsumerAhead(dir, 0, 1, 7, []int64{9, 12, 13}); err != nil {
		t.Fatal(err)
	}
	// The ahead record lags the offset file: its frontier is ignored and
	// so is its entry at 9, which the frontier already covers.
	if next, ahead := persistedFrontier(dir); next != 10 || ahead != 2 {
		t.Fatalf("lagging ahead record: (%d, %d), want (10, 2)", next, ahead)
	}
	if err := storage.WriteConsumerAhead(dir, 1, 2, 14, []int64{16, 17}); err != nil {
		t.Fatal(err)
	}
	if next, ahead := persistedFrontier(dir); next != 15 || ahead != 2 {
		t.Fatalf("fresher ahead record: (%d, %d), want (15, 2)", next, ahead)
	}
}
