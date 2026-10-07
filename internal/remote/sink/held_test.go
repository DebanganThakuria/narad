package sink

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A report delayed past a later change must not leave the gauge at the
// stale value: the last value reported is what the budget holds once
// every reserve and release has returned.
func TestHeldBudgetReportsWhatItHoldsAfterConcurrentChanges(t *testing.T) {
	var last atomic.Int64
	reserved, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	b := NewHeldBudget(1000, func(v int64) {
		if v == 100 {
			// The reserve's report, held up as a preempted goroutine
			// would be.
			once.Do(func() { close(reserved); <-proceed })
		}
		last.Store(v)
	})
	var wg sync.WaitGroup
	wg.Go(func() { b.TryReserve(100) })
	<-reserved
	released := make(chan struct{})
	go func() { b.Release(100); close(released) }()
	// Let the release report first if nothing orders it after the
	// held-up report.
	select {
	case <-released:
	case <-time.After(200 * time.Millisecond):
	}
	close(proceed)
	wg.Wait()
	<-released
	if b.Used() != 0 || last.Load() != 0 {
		t.Fatalf("holds %d, last reported %d", b.Used(), last.Load())
	}
}
