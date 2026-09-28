package ingress

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// TestZZWP19AcceptProduceBatchOneGroupCommit checks that a batch waits
// for exactly one group commit. Its records used to be staged one WAL
// append at a time, so the sync loop could detach the first record on
// its own and the batch then paid for two fsyncs (more under scheduler
// load). With every record staged under one hold of the WAL's lock no
// flush can fall between two of them.
func TestZZWP19AcceptProduceBatchOneGroupCommit(t *testing.T) {
	m, dir := zzWP18OpenManager(t)
	syncs := zzWP18SlowSyncs(t, dir, 5*time.Millisecond, nil)
	const batches, size = 5, 100
	for b := range batches {
		before := syncs.Load()
		accepted, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", zzWP18Records(size))
		if err != nil {
			t.Fatalf("batch %d: %v", b, err)
		}
		if got := syncs.Load() - before; got != 1 {
			t.Fatalf("batch %d of %d records cost %d WAL syncs, want 1", b, size, got)
		}
		if got, want := m.DurableProduceNext(), accepted[size-1].WAL.Seq+1; got != want {
			t.Fatalf("DurableProduceNext = %d after batch %d, want %d", got, b, want)
		}
	}
	replayed := zzWP18Replay(t, m)
	if len(replayed) != batches*size {
		t.Fatalf("replayed %d records, want %d", len(replayed), batches*size)
	}
	want := zzWP18Records(size)
	for i, r := range replayed {
		if r.WAL.Seq != uint64(i) || string(r.Payload) != string(want[i%size].Payload) || r.Key != want[i%size].Key {
			t.Fatalf("record %d = %+v, want seq %d and %+v", i, r, i, want[i%size])
		}
	}
}

// zzWP19BigRecords is n records of about 300 bytes each.
func zzWP19BigRecords(n int) []BatchRecord {
	records := make([]BatchRecord, n)
	for i := range records {
		payload := fmt.Appendf(nil, `{"i":%d,"pad":"%s"}`, i, bytes.Repeat([]byte("p"), 280))
		records[i] = BatchRecord{Key: fmt.Sprintf("k-%d", i%3), TargetPartition: i % 4, Payload: payload}
	}
	return records
}

// TestZZWP19AcceptProduceBatchAcrossSegments checks a batch larger than
// the room left in a segment: it rolls the WAL as single appends do, no
// segment holds more than SegmentBytes, and the batch replays whole, in
// order, with the receipts it was given.
func TestZZWP19AcceptProduceBatchAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	const segmentBytes = 4 << 10
	m, err := OpenManager(dir, wal.Options{SegmentBytes: segmentBytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	records := zzWP19BigRecords(60)
	accepted, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", records)
	if err != nil {
		t.Fatalf("AcceptProduceBatch: %v", err)
	}
	replayed := zzWP18Replay(t, m)
	if len(replayed) != len(records) {
		t.Fatalf("replayed %d records, want %d", len(replayed), len(records))
	}
	bases := map[uint64]bool{}
	for i, r := range replayed {
		if r.WAL != accepted[i].WAL || r.WAL.Seq != uint64(i) || string(r.Payload) != string(records[i].Payload) {
			t.Fatalf("record %d at %+v (receipt %+v), want seq %d", i, r.WAL, accepted[i].WAL, i)
		}
		bases[r.WAL.SegmentBase] = true
	}
	if len(bases) < 3 {
		t.Fatalf("the batch landed in %d segments, want it to span at least 3", len(bases))
	}
	segments, err := filepath.Glob(filepath.Join(dir, "ingress", "produce", "*.wal"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("list segments: %v (%d)", err, len(segments))
	}
	for _, path := range segments {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > segmentBytes {
			t.Fatalf("segment %s holds %d bytes, over SegmentBytes %d", filepath.Base(path), info.Size(), segmentBytes)
		}
	}
}

// TestZZWP19AcceptProduceBatchRollFailure checks a batch whose segment
// roll fails part way: the call fails, the log is not latched (the next
// batch rolls and succeeds), and the WAL then holds a prefix of the
// failed batch (synced before the roll, so the dispatcher delivers it:
// at least once) followed by the whole next batch, with no gap.
func TestZZWP19AcceptProduceBatchRollFailure(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, wal.Options{SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	inj := faulttest.New(t)
	rule := inj.FailNth(syncfile.OpOpen, filepath.Join(dir, "ingress", "produce"), 1, syscall.ENOSPC)

	failed := zzWP19BigRecords(40)
	if _, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", failed); err == nil {
		t.Fatal("AcceptProduceBatch succeeded over a failed segment roll")
	}
	if rule.Fired() != 1 {
		t.Fatalf("roll fault fired %d times, want 1", rule.Fired())
	}
	if got := m.DurableProduceNext(); got != 0 {
		t.Fatalf("DurableProduceNext = %d after a failed batch, want 0", got)
	}
	if !m.Healthy() {
		t.Fatal("a failed roll latched the WAL")
	}
	next := zzWP19BigRecords(20)
	for i := range next {
		next[i].Payload = append([]byte("next-"), next[i].Payload...)
	}
	accepted, err := m.AcceptProduceBatch(context.Background(), "orders", "id-7", next)
	if err != nil {
		t.Fatalf("AcceptProduceBatch after the disk recovered: %v", err)
	}

	replayed := zzWP18Replay(t, m)
	prefix := len(replayed) - len(next)
	if prefix < 0 || prefix >= len(failed) {
		t.Fatalf("replayed %d records: want a strict prefix of the failed batch then %d", len(replayed), len(next))
	}
	for i, r := range replayed {
		want := next[max(i-prefix, 0)].Payload
		if i < prefix {
			want = failed[i].Payload
		}
		if r.WAL.Seq != uint64(i) || string(r.Payload) != string(want) {
			t.Fatalf("record %d = seq %d %q, want seq %d %q", i, r.WAL.Seq, r.Payload, i, want)
		}
	}
	if got := accepted[0].WAL.Seq; got != uint64(prefix) {
		t.Fatalf("next batch starts at seq %d, want %d", got, prefix)
	}
}
