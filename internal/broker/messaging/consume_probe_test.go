package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// A commit that lands between ConsumeProbe and ConsumeWait must wake the
// wait: the probe snapshots the channels BEFORE scanning, so the handler
// can ask remote owners in between without losing the wake-up.
func TestConsumeProbeThenWaitSeesCommitInBetween(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	start := 1
	msg, found, waiter, err := engine.ConsumeProbe(ctx, "orders", ConsumeOpts{ScanStart: &start})
	if err != nil || found || waiter == nil {
		t.Fatalf("ConsumeProbe on empty topic = (%v, %v, %v, %v), want no message and a waiter", msg, found, waiter, err)
	}

	// The remote-owner probes happen here in the real ladder; a commit
	// lands on a local partition meanwhile.
	if _, _, err := engine.Produce(ctx, "orders", "", []byte(`{"id":1}`), 0); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	done := make(chan struct{})
	var got topic.Message
	var gotFound bool
	var gotErr error
	go func() {
		got, gotFound, gotErr = engine.ConsumeWait(ctx, waiter, 5*time.Second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeWait did not wake for a commit that landed after the probe")
	}
	if gotErr != nil || !gotFound || got.Partition != 0 || got.Offset != 0 {
		t.Fatalf("ConsumeWait = (%+v, %v, %v), want the committed message", got, gotFound, gotErr)
	}
}

// ScanStart puts the router's partition first in the scan.
func TestConsumeProbeScanStartOrdersScan(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	for p := range 3 {
		if _, _, err := engine.Produce(ctx, "orders", "", []byte(`{}`), p); err != nil {
			t.Fatalf("Produce(%d): %v", p, err)
		}
	}
	start := 2
	msg, found, _, err := engine.ConsumeProbe(ctx, "orders", ConsumeOpts{ScanStart: &start})
	if err != nil || !found || msg.Partition != 2 {
		t.Fatalf("ConsumeProbe(ScanStart=2) = (%+v, %v, %v), want partition 2 first", msg, found, err)
	}
	start = 1
	msg, found, _, err = engine.ConsumeProbe(ctx, "orders", ConsumeOpts{ScanStart: &start})
	if err != nil || !found || msg.Partition != 1 {
		t.Fatalf("ConsumeProbe(ScanStart=1) = (%+v, %v, %v), want partition 1 first", msg, found, err)
	}
	if _, _, _, err := engine.ConsumeProbe(ctx, "orders", ConsumeOpts{Partition: &start}); err == nil {
		t.Fatal("ConsumeProbe must reject pinned-partition options")
	}
	if _, _, err := engine.ConsumeWait(ctx, nil, time.Second); err == nil {
		t.Fatal("ConsumeWait must reject a nil waiter")
	}
}
