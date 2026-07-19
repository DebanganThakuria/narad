package messaging

import (
	"testing"
	"time"
)

func TestProducePauseLifecycle(t *testing.T) {
	e := newTestEngine(t, nil, nil, nil)

	if e.isProducePaused("orders", 0) {
		t.Fatal("fresh engine reports a pause")
	}
	e.PauseProduceForHandoff("orders", 0, time.Minute)
	if !e.isProducePaused("orders", 0) {
		t.Fatal("paused partition reports not paused")
	}
	// Isolation: a different partition is unaffected.
	if e.isProducePaused("orders", 1) {
		t.Fatal("pause leaked to a sibling partition")
	}
	e.ResumeProduce("orders", 0)
	if e.isProducePaused("orders", 0) {
		t.Fatal("resumed partition still reports paused")
	}
}

// The TTL is the source's safety valve: if the destination dies mid-
// handoff and never completes the flip, the pause self-clears so produce
// is never wedged forever.
func TestProducePauseAutoResumesOnTTL(t *testing.T) {
	e := newTestEngine(t, nil, nil, nil)
	e.PauseProduceForHandoff("orders", 0, 20*time.Millisecond)
	if !e.isProducePaused("orders", 0) {
		t.Fatal("should be paused immediately")
	}
	time.Sleep(40 * time.Millisecond)
	if e.isProducePaused("orders", 0) {
		t.Fatal("pause did not auto-resume after its TTL")
	}
}

// Re-pausing extends the deadline (idempotent).
func TestProducePauseReExtends(t *testing.T) {
	e := newTestEngine(t, nil, nil, nil)
	e.PauseProduceForHandoff("orders", 0, 20*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	e.PauseProduceForHandoff("orders", 0, time.Minute) // extend
	time.Sleep(20 * time.Millisecond)
	if !e.isProducePaused("orders", 0) {
		t.Fatal("re-pause did not extend the deadline")
	}
}
