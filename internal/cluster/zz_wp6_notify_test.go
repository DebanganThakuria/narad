package cluster

import (
	"slices"
	"testing"
	"time"
)

// produce-accept#5 / produce-dispatch-commit#8: at light load a record
// that became durable while the dispatcher slept its 10 ms idle poll
// waited out the rest of the sleep before dispatch began. The accept
// now wakes the dispatcher.
func TestZZWP6AcceptWakesTheDispatcher(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	// A long idle poll isolates the wakeup from the timer backstop.
	d := NewProduceDispatcher(m, store, "node-self", c, nil, nil, ProduceDispatcherConfig{PollInterval: time.Second})
	zzWP6StartRun(t, d)
	time.Sleep(50 * time.Millisecond)
	lats := zzWP6LocalLatencies(t, m, c, 20, 20*time.Millisecond, 2*time.Second)
	slices.Sort(lats)
	if p50 := lats[len(lats)/2]; p50 > 50*time.Millisecond {
		t.Fatalf("accept-to-commit p50 %v with a 1 s idle poll, want the accept to wake the dispatcher", p50)
	}
}
