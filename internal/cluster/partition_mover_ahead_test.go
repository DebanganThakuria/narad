package cluster

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// aheadFetcher is a dirFetcher whose listing also reports the source's
// acked-ahead set, as the serve side does from its live shard.
type aheadFetcher struct {
	dirFetcher
	ackedAhead []int64
}

func (f aheadFetcher) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	info.AckedAhead = f.ackedAhead
	return info, err
}

// TestPartitionMoverCarriesAckedAhead pins that the staged copy gets a
// consumer.ahead record next to the frontier when the source reported
// acked-ahead offsets, and none when the source had no frontier at all.
func TestPartitionMoverCarriesAckedAhead(t *testing.T) {
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 6)

	fetcher := aheadFetcher{dirFetcher{dir: src, hwm: hwm, committed: 1, hasCommitted: true}, []int64{3, 4}}
	staging := filepath.Join(t.TempDir(), "staging")
	if _, err := NewPartitionMover(fetcher, 7, nil).Copy(context.Background(), "source-addr", "orders", 0, staging); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	rec, ok, err := storage.ReadConsumerAhead(staging)
	if err != nil || !ok {
		t.Fatalf("staged consumer.ahead: ok %v err %v, want a record", ok, err)
	}
	if rec.Committed != 1 || !slices.Equal(rec.Offsets, []int64{3, 4}) {
		t.Fatalf("staged consumer.ahead = committed %d offsets %v, want 1 [3 4]", rec.Committed, rec.Offsets)
	}

	// No frontier on the source: nothing to anchor the set to, no file.
	noFrontier := aheadFetcher{dirFetcher{dir: src, hwm: hwm}, []int64{3}}
	staging2 := filepath.Join(t.TempDir(), "staging")
	if _, err := NewPartitionMover(noFrontier, 7, nil).Copy(context.Background(), "source-addr", "orders", 0, staging2); err != nil {
		t.Fatalf("Copy (no frontier): %v", err)
	}
	if _, ok, err := storage.ReadConsumerAhead(staging2); err != nil || ok {
		t.Fatalf("staged consumer.ahead without a frontier: ok %v err %v, want none", ok, err)
	}
}
