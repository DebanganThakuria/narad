package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// consume-local#1. A consume that hits on the first partition it scans
// no longer resolves (opens and stamps) the log of every other
// partition first.
func TestZZWP7aConsumeHitDoesNotOpenTheRestOfTheScan(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3, VisibilityTimeoutMs: 60_000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "x")); err != nil {
		t.Fatal(err)
	}
	start := 0
	msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{ScanStart: &start})
	if err != nil || !found || msg.Partition != 0 {
		t.Fatalf("consume: found=%v partition=%d err=%v", found, msg.Partition, err)
	}
	for p := 1; p < 3; p++ {
		if _, open := e.logs.Peek("orders", p); open {
			t.Fatalf("partition %d's log was opened by a consume that hit on partition 0", p)
		}
	}
}

// The pause counters track the maps through arm, re-arm, expiry and
// resume, and a zero count answers "not paused" without the lock.
func TestZZWP7aPauseCountersTrackPauses(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2}
	e := newTestEngine(t, ms, nil, nil)
	clock := &fakeClock{now: time.Unix(1_000, 0)}
	e.now = clock.Now
	counts := func() (int32, int32) { return e.producePausesActive.Load(), e.consumePausesActive.Load() }

	e.PauseProduceForHandoff("orders", 0, time.Second)
	e.PauseProduceForHandoff("orders", 0, time.Second) // extend, not a second pause
	e.PauseConsumeForHandoff("orders", 0, time.Second)
	e.PauseConsumeForHandoff("orders", 0, time.Second)
	if p, c := counts(); p != 1 || c != 1 {
		t.Fatalf("after pausing partition 0: counts %d/%d, want 1/1", p, c)
	}
	if !e.isProducePaused("orders", 0) || !e.isConsumePaused("orders", 0) || e.isProducePaused("orders", 1) {
		t.Fatal("pause state wrong while partition 0 is paused")
	}
	e.PauseProduceForHandoff("orders", 1, time.Second)
	clock.advance(2 * time.Second) // both lapse
	if e.isProducePaused("orders", 0) || e.isConsumePaused("orders", 0) || e.isProducePaused("orders", 1) {
		t.Fatal("a lapsed pause still reads as paused")
	}
	if p, c := counts(); p != 0 || c != 0 {
		t.Fatalf("after every pause lapsed: counts %d/%d, want 0/0", p, c)
	}
	e.PauseProduceForHandoff("orders", 0, time.Second)
	e.PauseConsumeForHandoff("orders", 0, time.Second)
	e.ResumeProduce("orders", 0)
	e.ResumeProduce("orders", 0)
	if p, c := counts(); p != 0 || c != 0 {
		t.Fatalf("after resume: counts %d/%d, want 0/0", p, c)
	}
	token, err := e.armHandoffFreeze("orders", 0, time.Second, "")
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(2 * time.Second)
	if _, err := e.armHandoffFreeze("orders", 0, time.Second, token); !errors.Is(err, ErrHandoffFreezeLapsed) {
		t.Fatalf("re-arm of a lapsed freeze: %v", err)
	}
	if p, _ := counts(); p != 0 {
		t.Fatalf("a lapsed freeze refused on re-arm is still counted: %d", p)
	}
}
