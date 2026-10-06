package sink

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// fakeClock is a settable gate clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func unavailable() Verdict {
	return Verdict{Action: ActRetry, State: topic.RemoteStateUnavailable, Class: topic.RemoteStateUnavailable, Ambiguous: true}
}

func TestGateTripsAfterThreeUnavailable(t *testing.T) {
	g := NewGate()
	for i := range GateTripAfter - 1 {
		if !g.Failed(unavailable(), false) {
			t.Fatalf("failure %d closed the gate; a lone failure backs off only its lane", i+1)
		}
		if g.Closed() {
			t.Fatalf("gate closed after %d failures", i+1)
		}
	}
	if g.Failed(unavailable(), false) || !g.Closed() {
		t.Fatal("the third failure in a row must close the gate")
	}
	if g.Backoff() != GateMinBackoff {
		t.Fatalf("backoff = %s, want %s", g.Backoff(), GateMinBackoff)
	}
	g.Succeeded(false)
	if g.Closed() || g.Backoff() != 0 {
		t.Fatal("a success must open the gate and reset its backoff")
	}
	// The count resets on a success, so two failures do not trip it.
	g.Failed(unavailable(), false)
	g.Failed(unavailable(), false)
	if g.Closed() {
		t.Fatal("the streak did not reset on success")
	}
}

func TestGateAuthGoesStraightToTheCeiling(t *testing.T) {
	g := NewGate()
	g.Failed(Verdict{Action: ActGate, State: topic.RemoteStateAuthFailed}, false)
	if !g.Closed() || g.Backoff() != GateMaxBackoff {
		t.Fatalf("closed=%v backoff=%s, want closed at %s", g.Closed(), g.Backoff(), GateMaxBackoff)
	}
	g.Reset()
	if g.Closed() {
		t.Fatal("a new credential version must reopen the gate at once")
	}
}

func TestGateThrottledHonoursRetryAfter(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000, 0)}
	g := NewGate()
	g.now = clock.Now
	g.Failed(Verdict{Action: ActGate, State: topic.RemoteStateThrottled, RetryAfter: 7 * time.Second}, false)
	if !g.Closed() {
		t.Fatal("a 429 must close the gate")
	}
	g.mu.Lock()
	wait := g.until.Sub(clock.Now())
	g.mu.Unlock()
	if wait != 7*time.Second {
		t.Fatalf("gate reopens after %s, want exactly the Retry-After", wait)
	}
}

func TestGateTLSClosesAtOnce(t *testing.T) {
	g := NewGate()
	g.Failed(Verdict{Action: ActGate, State: topic.RemoteStateTLSFailed}, false)
	if !g.Closed() {
		t.Fatal("tls_failed must close the gate at once")
	}
}

// While the gate backs off, exactly one chunk goes out per interval as
// the probe, and the others wait for its outcome.
func TestGateOneProbePerInterval(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000, 0)}
	g := NewGate()
	g.now = clock.Now
	for range GateTripAfter {
		g.Failed(unavailable(), false)
	}
	epoch := g.Epoch()
	if epoch != 1 {
		t.Fatalf("epoch = %d after one close, want 1", epoch)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var probes, passes atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			probe, err := g.Wait(ctx)
			if err != nil {
				return
			}
			if probe {
				probes.Add(1)
			} else {
				passes.Add(1)
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	if probes.Load()+passes.Load() != 0 {
		t.Fatal("a chunk went out before the backoff elapsed")
	}
	clock.Advance(GateMaxBackoff)
	// Nudge the waiters: the fake clock does not fire their timers.
	g.mu.Lock()
	g.signalLocked()
	g.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for probes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if probes.Load() != 1 || passes.Load() != 0 {
		t.Fatalf("probes=%d passes=%d while backing off, want exactly one probe", probes.Load(), passes.Load())
	}
	// The probe fails: still closed, the others still wait, backoff doubled.
	g.Failed(unavailable(), true)
	if !g.Closed() || g.Backoff() != 2*GateMinBackoff {
		t.Fatalf("after a failed probe: closed=%v backoff=%s", g.Closed(), g.Backoff())
	}
	if g.Epoch() != epoch {
		t.Fatal("a failed probe must not count as a new close")
	}
	clock.Advance(GateMaxBackoff)
	g.mu.Lock()
	g.signalLocked()
	g.mu.Unlock()
	deadline = time.Now().Add(2 * time.Second)
	for probes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	g.Succeeded(true)
	wg.Wait()
	if probes.Load() != 2 || passes.Load() != 6 {
		t.Fatalf("probes=%d passes=%d, want 2 probes and the other 6 released by the success", probes.Load(), passes.Load())
	}
}

func TestGateWaitDueDoesNotTakeTheProbe(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000, 0)}
	g := NewGate()
	g.now = clock.Now
	g.Failed(Verdict{Action: ActGate, State: topic.RemoteStateTLSFailed}, false)
	clock.Advance(GateMaxBackoff)
	if err := g.WaitDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe, _ := g.Wait(context.Background()); !probe {
		t.Fatal("WaitDue took the probe; a reader must leave it to the lanes")
	}
}

func TestGateReleasedHandsTheProbeOn(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000, 0)}
	g := NewGate()
	g.now = clock.Now
	g.Failed(Verdict{Action: ActGate, State: topic.RemoteStateTLSFailed}, false)
	clock.Advance(GateMaxBackoff)
	if probe, _ := g.Wait(context.Background()); !probe {
		t.Fatal("want the probe")
	}
	g.Released()
	if probe, _ := g.Wait(context.Background()); !probe {
		t.Fatal("a released probe must go to the next waiter")
	}
}

func TestHeldBudgetFirstComeFirstServed(t *testing.T) {
	var observed atomic.Int64
	b := NewHeldBudget(100, func(v int64) { observed.Store(v) })
	if !b.TryReserve(60) || b.TryReserve(50) || !b.TryReserve(40) {
		t.Fatal("reservations must take up to the limit, first come first served")
	}
	if b.Used() != 100 || observed.Load() != 100 {
		t.Fatalf("used = %d, observed = %d", b.Used(), observed.Load())
	}
	b.Release(60)
	if b.Used() != 40 || !b.TryReserve(50) {
		t.Fatal("a release must free its bytes")
	}
	if NewHeldBudget(0, nil).TryReserve(1) {
		t.Fatal("a zero budget holds nothing")
	}
}

func TestHoldCopiesPayloads(t *testing.T) {
	buf := []byte("abcdef")
	recs := []topic.KeyedRecord{{Key: "k", Offset: 1, Payload: buf[0:3]}, {Offset: 2, Payload: buf[3:6]}}
	held := Hold(recs)
	buf[0], buf[3] = 'X', 'Y'
	if string(held[0].Payload) != "abc" || string(held[1].Payload) != "def" || held[0].Offset != 1 || held[0].Key != "k" {
		t.Fatalf("held = %+v, want copies unaffected by the log buffer", held)
	}
	if HeldSize(recs) != 7 {
		t.Fatalf("HeldSize = %d, want 7", HeldSize(recs))
	}
}

func TestSemaphoreResizesLive(t *testing.T) {
	s := NewSemaphore(1)
	ctx := context.Background()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	got := make(chan struct{})
	go func() {
		_, _ = s.Acquire(ctx)
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("a second slot was handed out past the limit")
	case <-time.After(50 * time.Millisecond):
	}
	s.Resize(2)
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("growing the limit did not wake the waiter")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Acquire(cctx); err == nil {
		t.Fatal("a cancelled acquire past the limit must fail")
	}
}

// Answers to chunks already in flight when the gate closed report the
// same outage, not failed probes: sixteen of them at once leave the
// backoff at its floor and the probe due at the first interval. Only a
// failed probe escalates.
func TestGateEscalatesOnFailedProbesNotOnAnswersInFlight(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	g := NewGate()
	g.now = clock.Now
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { g.Failed(unavailable(), false) })
	}
	wg.Wait()
	if !g.Closed() || g.Backoff() != GateMinBackoff {
		t.Fatalf("after 16 answers in flight: closed=%v backoff=%s, want closed at %s", g.Closed(), g.Backoff(), GateMinBackoff)
	}
	// Sixteen throttled answers in flight with Retry-After: 1 wait that
	// long, without doubling the backoff.
	throttled := Verdict{Action: ActGate, State: topic.RemoteStateThrottled, Class: topic.RemoteStateThrottled, RetryAfter: time.Second}
	for range 16 {
		g.Failed(throttled, false)
	}
	if g.Backoff() != GateMinBackoff {
		t.Fatalf("after 16 throttled answers in flight: backoff=%s, want %s", g.Backoff(), GateMinBackoff)
	}
	clock.Advance(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	probe, err := g.Wait(ctx)
	if err != nil || !probe {
		t.Fatalf("probe after Retry-After: %v %v", probe, err)
	}
	// A failed probe escalates.
	g.Failed(unavailable(), true)
	if g.Backoff() != 2*GateMinBackoff {
		t.Fatalf("after a failed probe: backoff=%s, want %s", g.Backoff(), 2*GateMinBackoff)
	}
}
