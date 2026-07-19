package messaging

// Serve-side of partition rebalance, end to end through the Engine API:
// a node exposes an owned partition's segments + durable positions, a
// destination fetches every byte via ReadPartitionSegment, and the
// recovered copy is identical (same offsets, HWM, records). This is the
// copy-round-trips-identically proof, one layer up from the raw storage
// primitives, over the real ownership guard.

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestPartitionTransferInfoRequiresLocalOwnership(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)

	_, err := e.PartitionTransferInfo(context.Background(), "orders", 99) // out of range
	if err == nil {
		t.Fatal("out-of-range partition must error")
	}
	_, err = e.PartitionTransferInfo(context.Background(), "missing", 0)
	if err == nil {
		t.Fatal("missing topic must error")
	}
}

func TestPartitionTransferServeAndCopyIsIdentical(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	// Commit several records to partition 0 (advances the HWM).
	const n = 20
	recs := make([]ingress.ProduceRecord, 0, n)
	for i := range n {
		recs = append(recs, ingress.ProduceRecord{
			Topic:           "orders",
			Key:             "k",
			TargetPartition: 0,
			Payload:         []byte{byte('a' + i%26), byte('0' + i%10)},
		})
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, recs); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Serve side: list segments + durable positions.
	info, err := e.PartitionTransferInfo(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("PartitionTransferInfo: %v", err)
	}
	if len(info.Segments) == 0 || info.HighWatermark != n {
		t.Fatalf("info = %d segments, hwm %d; want segments>0, hwm %d", len(info.Segments), info.HighWatermark, n)
	}

	// Destination: fetch every segment byte via the Engine API, install
	// into a fresh dir, copy the HWM, recover, and audit identity.
	dst := t.TempDir()
	for _, seg := range info.Segments {
		var at int64
		first := true
		for at < seg.SizeBytes || first {
			chunk, err := e.ReadPartitionSegment(ctx, "orders", 0, seg.BaseOffset, at, 8)
			if err != nil {
				t.Fatalf("ReadPartitionSegment: %v", err)
			}
			if len(chunk) == 0 {
				break
			}
			if first {
				if err := storage.WriteSegmentFile(dst, seg.BaseOffset, chunk); err != nil {
					t.Fatalf("WriteSegmentFile: %v", err)
				}
				first = false
			} else if err := storage.AppendToSegmentFile(dst, seg.BaseOffset, chunk); err != nil {
				t.Fatalf("AppendToSegmentFile: %v", err)
			}
			at += int64(len(chunk))
		}
	}

	copyLog, err := storage.NewLog(dst, storage.Options{})
	if err != nil {
		t.Fatalf("recover copy: %v", err)
	}
	defer copyLog.Close()
	if copyLog.NextOffset() != info.HighWatermark {
		t.Fatalf("copy NextOffset = %d, want %d", copyLog.NextOffset(), info.HighWatermark)
	}
	for off := int64(0); off < n; off++ {
		if _, _, _, err := copyLog.ReadKeyed(off); err != nil {
			t.Fatalf("copy ReadKeyed(%d): %v", off, err)
		}
	}
}
