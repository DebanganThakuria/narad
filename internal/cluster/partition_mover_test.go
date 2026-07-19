package cluster

// The partition mover copies a source owner's segments into a staging
// dir and must reproduce the partition exactly: same offsets, same HWM,
// same committed consumer offset, byte-identical records. The fake
// fetcher serves real segment bytes off disk (the same reads the RPC
// serve side performs), so this exercises the mover's copy + tail-
// termination + verify against genuine storage.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// dirFetcher serves a partition directory as a segmentFetcher, exactly
// as the owning node's RPC handlers do.
type dirFetcher struct {
	dir          string
	hwm          int64
	committed    int64
	hasCommitted bool
}

func (d dirFetcher) ListPartitionSegments(_ context.Context, _, _ string, _ int) (messaging.PartitionTransferInfo, error) {
	segs, err := storage.ListPartitionSegments(d.dir)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	return messaging.PartitionTransferInfo{
		Segments:        segs,
		HighWatermark:   d.hwm,
		CommittedOffset: d.committed,
		HasCommitted:    d.hasCommitted,
	}, nil
}

func (d dirFetcher) FetchSegmentChunk(_ context.Context, _, _ string, _ int, base, at, length int64) ([]byte, error) {
	return storage.ReadSegmentRange(d.dir, base, at, length)
}

func buildSourcePartition(t *testing.T, dir string, n int) (int64, map[int64][]byte) {
	t.Helper()
	log, err := storage.NewLog(dir, storage.Options{FlushInterval: time.Millisecond, SegmentBytes: 1})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	payloads := map[int64][]byte{}
	for i := range n {
		p := []byte{byte('a' + i%26), byte('0' + i%10), byte('!' + i%15)}
		off, err := log.Append(storage.EncodeKeyedRecord("k", int64(i), p))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := log.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		payloads[off] = p
	}
	next := log.NextOffset()
	if err := log.AdvanceHighWatermark(next); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return next, payloads
}

func TestPartitionMoverCopiesIdentically(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 25)

	fetcher := dirFetcher{dir: src, hwm: wantHWM, committed: 9, hasCommitted: true}
	mover := NewPartitionMover(fetcher, 7, nil) // tiny chunks exercise the streaming loop

	staging := filepath.Join(t.TempDir(), "staging")
	res, err := mover.Copy(context.Background(), "source-addr", "orders", 0, staging)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if res.HighWatermark != wantHWM {
		t.Fatalf("copy HWM = %d, want %d", res.HighWatermark, wantHWM)
	}
	if !res.HasCommitted || res.CommittedOffset != 9 {
		t.Fatalf("committed = %d (has %v), want 9", res.CommittedOffset, res.HasCommitted)
	}
	if res.BytesCopied <= 0 {
		t.Fatalf("copied 0 bytes")
	}

	// The staged copy must recover into an identical log.
	log, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("recover staging: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("staged NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
	if log.HighWatermark() != wantHWM {
		t.Fatalf("staged HWM = %d, want %d", log.HighWatermark(), wantHWM)
	}
	for off, want := range payloads {
		_, _, got, err := log.ReadKeyed(off)
		if err != nil {
			t.Fatalf("staged ReadKeyed(%d): %v", off, err)
		}
		if string(got) != string(want) {
			t.Fatalf("staged offset %d = %q, want %q", off, got, want)
		}
	}
	// The committed consumer offset moved with the partition.
	committed, ok, err := storage.ReadConsumerOffset(staging)
	if err != nil || !ok || committed != 9 {
		t.Fatalf("staged consumer offset = %d (ok %v, err %v), want 9", committed, ok, err)
	}
}

// A source with a hidden tail (HWM < record count) must copy the
// records but keep the hidden ones invisible — the copy's HWM matches
// the source's, not its record tail.
func TestPartitionMoverPreservesHiddenTail(t *testing.T) {
	src := t.TempDir()
	recordCount, _ := buildSourcePartition(t, src, 10)
	hiddenHWM := recordCount - 3 // source only made the first 7 visible

	fetcher := dirFetcher{dir: src, hwm: hiddenHWM}
	mover := NewPartitionMover(fetcher, 16, nil)
	staging := filepath.Join(t.TempDir(), "staging")

	res, err := mover.Copy(context.Background(), "a", "orders", 0, staging)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if res.HighWatermark != hiddenHWM {
		t.Fatalf("copy HWM = %d, want %d", res.HighWatermark, hiddenHWM)
	}
	log, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer log.Close()
	if log.HighWatermark() != hiddenHWM {
		t.Fatalf("staged HWM = %d, want %d (hidden tail must stay hidden)", log.HighWatermark(), hiddenHWM)
	}
	// But all records are physically present (the bytes were copied).
	if log.NextOffset() != recordCount {
		t.Fatalf("staged NextOffset = %d, want %d (all records copied)", log.NextOffset(), recordCount)
	}
}
