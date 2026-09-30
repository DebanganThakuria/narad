package messaging

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestZZWP24ListingRecoversCrashImageBoundary pins the boundary a
// listing ships for a closed partition. An open log empties its hwm
// file before its first advance, so a crash leaves the file empty, and
// a restarted owner serves cluster RPCs before startup opens its
// partitions. Such a listing used to ship boundary 0: every frontier sat
// above it, and a force-promote's gate (staged next offset >= boundary)
// accepted a copy of any length. The owner must recover the real
// boundary, while a partition with no record bytes gets its exact
// boundary without an open.
func TestZZWP24ListingRecoversCrashImageBoundary(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 30000}
	ms.topics["idle"] = topic.Topic{Name: "idle", Partitions: 1}
	ms.topics["aged"] = topic.Topic{Name: "aged", Partitions: 1}
	dataDir := t.TempDir()
	e := newTestEngineWithDir(t, dataDir, ms, nil, nil)
	ctx := context.Background()

	recs := make([]ingress.ProduceRecord, 0, 5)
	for range 5 {
		recs = append(recs, ingress.ProduceRecord{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("v")})
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, recs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for range 3 {
		msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
		if err != nil || !found {
			t.Fatalf("consume: found=%v err=%v", found, err)
		}
		if err := e.Ack(ctx, "orders", decodeHandleForTest(t, msg.ReceiptHandle)); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if err := e.logs.CloseTopic("orders"); err != nil {
		t.Fatalf("close: %v", err)
	}
	info, err := e.PartitionTransferInfo(ctx, "orders", 0)
	if err != nil || info.HighWatermark != 5 || info.CommittedOffset != 2 {
		t.Fatalf("cleanly closed listing: boundary %d committed %d err %v, want 5 2", info.HighWatermark, info.CommittedOffset, err)
	}

	// The crash image: records on disk, an emptied hwm file, no open log.
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := os.WriteFile(filepath.Join(dir, "hwm"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err = e.PartitionTransferInfo(ctx, "orders", 0)
	if err != nil || info.HighWatermark != 5 || info.CommittedOffset != 2 {
		t.Fatalf("crash-image listing: boundary %d committed %d err %v, want 5 2 (the record tail recovery exposes)", info.HighWatermark, info.CommittedOffset, err)
	}
	if _, open := e.logs.Peek("orders", 0); !open {
		t.Fatal("crash-image listing did not leave the recovered log open")
	}

	// Nothing ever written: the boundary is exactly 0.
	if info, err := e.PartitionTransferInfo(ctx, "idle", 0); err != nil || info.HighWatermark != 0 {
		t.Fatalf("idle listing: boundary %d err %v, want 0", info.HighWatermark, err)
	}
	// Every record aged out and the hwm file emptied: the one empty
	// segment's name is the boundary recovery would find.
	const base = int64(5168020)
	agedDir := storage.TopicPartitionDir(dataDir, "aged", 0)
	if err := os.MkdirAll(agedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteSegmentFile(agedDir, base, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(agedDir, "hwm"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if info, err := e.PartitionTransferInfo(ctx, "aged", 0); err != nil || info.HighWatermark != base {
		t.Fatalf("aged-out listing: boundary %d err %v, want %d", info.HighWatermark, err, base)
	}
}

func TestZZWP24FrontierBelowBoundary(t *testing.T) {
	cases := []struct {
		name          string
		hwm           int64
		committed     int64
		ahead         []int64
		wantCommitted int64
		wantAhead     []int64
	}{
		{"below", 10, 4, []int64{6, 8}, 4, []int64{6, 8}},
		{"frontier at the boundary", 10, 10, []int64{12}, 9, []int64{}},
		{"frontier past the boundary", 10, 25, nil, 9, nil},
		{"acked-ahead at and past the boundary", 10, 4, []int64{6, 10, 11}, 4, []int64{6}},
		{"empty partition", 0, 3, []int64{5}, -1, []int64{}},
		{"no frontier yet", 10, -1, []int64{2}, -1, []int64{2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := slices.Clone(tc.ahead)
			committed, ahead := FrontierBelowBoundary(tc.hwm, tc.committed, in)
			if committed != tc.wantCommitted || !slices.Equal(ahead, tc.wantAhead) {
				t.Fatalf("FrontierBelowBoundary(%d, %d, %v) = %d %v, want %d %v", tc.hwm, tc.committed, tc.ahead, committed, ahead, tc.wantCommitted, tc.wantAhead)
			}
			if !slices.Equal(in, tc.ahead) {
				t.Fatalf("input modified: %v, was %v", in, tc.ahead)
			}
		})
	}
}
