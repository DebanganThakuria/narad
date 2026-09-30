package ingress

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

func zzWP18OpenManager(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m, err := OpenManager(dir, DefaultWALOptions())
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, dir
}

func zzWP18Records(n int) []BatchRecord {
	records := make([]BatchRecord, n)
	for i := range records {
		// Every third record shares a key, so order within a key is
		// observable as well as order across the batch.
		key := fmt.Sprintf("k-%d", i%3)
		records[i] = BatchRecord{Key: key, TargetPartition: i % 4, Payload: []byte(fmt.Sprintf(`{"i":%d}`, i))}
	}
	return records
}

func zzWP18Replay(t *testing.T, m *Manager) []ProduceRecord {
	t.Helper()
	var got []ProduceRecord
	if err := m.ReplayProduce(0, func(r ProduceRecord) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduce: %v", err)
	}
	return got
}

// TestZZWP18AcceptProduceBatchAppendsInOrder checks a batch lands in the
// WAL exactly as a caller sees it: every record, in batch order (so
// same-key records keep their order), consecutive sequence numbers,
// one accept time and the topic incarnation, durable before the call
// returns.
func TestZZWP18AcceptProduceBatchAppendsInOrder(t *testing.T) {
	m, _ := zzWP18OpenManager(t)
	records := zzWP18Records(50)
	accepted, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", records)
	if err != nil {
		t.Fatalf("AcceptProduceBatch: %v", err)
	}
	if len(accepted) != len(records) {
		t.Fatalf("got %d receipts, want %d", len(accepted), len(records))
	}
	if got := m.DurableProduceNext(); got != uint64(len(records)) {
		t.Fatalf("DurableProduceNext = %d, want %d", got, len(records))
	}
	replayed := zzWP18Replay(t, m)
	if len(replayed) != len(records) {
		t.Fatalf("replayed %d records, want %d", len(replayed), len(records))
	}
	for i, r := range replayed {
		want := records[i]
		if r.Topic != "orders" || r.TopicID != "id-7" || r.Key != want.Key ||
			r.TargetPartition != want.TargetPartition || string(r.Payload) != string(want.Payload) {
			t.Fatalf("record %d = %+v, want %+v", i, r, want)
		}
		if r.WAL.Seq != uint64(i) || r.WAL != accepted[i].WAL {
			t.Fatalf("record %d at %+v, receipt says %+v, want seq %d", i, r.WAL, accepted[i].WAL, i)
		}
		if r.CreatedAtUnixMs != accepted[0].CreatedAtUnixMs || accepted[i].TargetPartition != want.TargetPartition {
			t.Fatalf("record %d: created %d, receipt %+v", i, r.CreatedAtUnixMs, accepted[i])
		}
	}
}

// TestZZWP18AcceptProduceBatchValidatesFirst checks that one invalid
// record fails the batch before any record is appended.
func TestZZWP18AcceptProduceBatchValidatesFirst(t *testing.T) {
	m, _ := zzWP18OpenManager(t)
	records := zzWP18Records(5)
	records[3].Payload = nil
	if _, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", records); err == nil || !strings.Contains(err.Error(), "record 3") {
		t.Fatalf("AcceptProduceBatch error = %v, want one naming record 3", err)
	}
	records = zzWP18Records(5)
	records[4].TargetPartition = -1
	if _, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", records); err == nil || !strings.Contains(err.Error(), "record 4") {
		t.Fatalf("AcceptProduceBatch error = %v, want one naming record 4", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.AcceptProduceBatch(ctx, "orders", "id-7", zzWP18Records(5)); !errors.Is(err, context.Canceled) {
		t.Fatalf("AcceptProduceBatch with a cancelled context = %v, want context.Canceled", err)
	}
	if got := zzWP18Replay(t, m); len(got) != 0 {
		t.Fatalf("a refused batch left %d records in the WAL", len(got))
	}
	if got := m.DurableProduceNext(); got != 0 {
		t.Fatalf("DurableProduceNext = %d after refused batches, want 0", got)
	}
}

// zzWP18SlowSyncs installs a fault hook that counts the data syncs of
// files under dir and makes each take delay, a disk slow enough that
// staging a whole batch always takes less than one sync.
func zzWP18SlowSyncs(t *testing.T, dir string, delay time.Duration, fail *atomic.Bool) *atomic.Int64 {
	t.Helper()
	var syncs atomic.Int64
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op != syncfile.OpSyncData || !strings.HasPrefix(path, dir+string(filepath.Separator)) {
			return nil
		}
		syncs.Add(1)
		time.Sleep(delay)
		if fail != nil && fail.Load() {
			return errors.New("injected sync failure")
		}
		return nil
	})
	t.Cleanup(restore)
	return &syncs
}

// TestZZWP18AcceptProduceBatchSharesGroupCommits checks that a batch
// waits for its records' group commits together rather than for one
// fsync per record: on a disk where one sync outlasts staging the whole
// batch, 100 records cost at most two syncs (the first record can start
// a flush on its own before the rest are staged).
func TestZZWP18AcceptProduceBatchSharesGroupCommits(t *testing.T) {
	m, dir := zzWP18OpenManager(t)
	syncs := zzWP18SlowSyncs(t, dir, 20*time.Millisecond, nil)
	if _, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", zzWP18Records(100)); err != nil {
		t.Fatalf("AcceptProduceBatch: %v", err)
	}
	t.Logf("a batch of 100 cost %d WAL syncs", syncs.Load())
	if got := syncs.Load(); got > 2 {
		t.Fatalf("a batch of 100 cost %d WAL syncs, want at most 2", got)
	}
	if got := len(zzWP18Replay(t, m)); got != 100 {
		t.Fatalf("replayed %d records, want 100", got)
	}
}

// TestZZWP18AcceptProduceBatchSyncFailure checks that a failed sync
// fails the batch and that the call returns rather than leaving the
// records staged behind it waiting.
func TestZZWP18AcceptProduceBatchSyncFailure(t *testing.T) {
	m, dir := zzWP18OpenManager(t)
	var fail atomic.Bool
	fail.Store(true)
	zzWP18SlowSyncs(t, dir, time.Millisecond, &fail)
	done := make(chan error, 1)
	go func() {
		_, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", zzWP18Records(20))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("AcceptProduceBatch succeeded over a failing sync")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AcceptProduceBatch did not return after a sync failure")
	}
	if m.Healthy() {
		t.Fatal("the WAL still reports healthy after a failed sync")
	}
	if got := m.DurableProduceNext(); got != 0 {
		t.Fatalf("DurableProduceNext = %d after a failed batch, want 0", got)
	}
}
