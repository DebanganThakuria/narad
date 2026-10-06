package sink

import (
	"context"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// Gate pacing (ch. 6.6).
const (
	// GateMinBackoff and GateMaxBackoff bound the gate's backoff, which
	// doubles with full jitter between them.
	GateMinBackoff = 250 * time.Millisecond
	GateMaxBackoff = 30 * time.Second
	// GateTripAfter is how many unavailable or edge answers in a row,
	// across a remote's lanes on this node, close the gate. A single one
	// backs off only its lane: a lone reset or timeout is routine on a
	// WAN.
	GateTripAfter = 3
)

// Gate is one remote's shared pacing on this node. While it is closed
// nothing goes to the remote except, once each backoff has elapsed, one
// chunk from any cursor as the probe, so a dead remote sees one request
// per interval per node, not one per cursor. The backoff doubles per
// failed probe, never per answer to a chunk already in flight when the
// gate closed. Any accepted chunk and any new credential version open
// it at once.
type Gate struct {
	mu       sync.Mutex
	closed   bool
	until    time.Time
	backoff  time.Duration
	failures int
	probing  bool
	epoch    uint64
	wake     chan struct{}
	now      func() time.Time
	// observe, when set, is told the backoff each time the gate closes
	// and 0 when it opens (narad_remote_gate_backoff_seconds).
	observe func(time.Duration)
}

// ObserveBackoff makes the gate report its backoff to fn: the backoff
// each time it closes, 0 when it opens. fn runs under the gate's lock
// and must not call back into the gate.
func (g *Gate) ObserveBackoff(fn func(time.Duration)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.observe = fn
	if fn != nil {
		if g.closed {
			fn(g.backoff)
		} else {
			fn(0)
		}
	}
}

// NewGate returns an open gate.
func NewGate() *Gate { return &Gate{wake: make(chan struct{}), now: time.Now} }

// Wait blocks until the gate lets a chunk go, or ctx ends. probe reports
// that the chunk is the gate's probe: the caller must then report its
// outcome (Succeeded, Failed or Released) before any other chunk goes.
func (g *Gate) Wait(ctx context.Context) (probe bool, err error) {
	for {
		g.mu.Lock()
		if !g.closed {
			g.mu.Unlock()
			return false, nil
		}
		now := g.now()
		if !g.probing && !now.Before(g.until) {
			g.probing = true
			g.mu.Unlock()
			return true, nil
		}
		wake, d := g.wake, time.Duration(-1)
		if !g.probing {
			d = g.until.Sub(now)
		}
		g.mu.Unlock()
		if err := sleepOrWake(ctx, wake, d); err != nil {
			return false, err
		}
	}
}

// TryWait is Wait without blocking: ok is false when Wait would block
// (the gate is closed and its probe is out or not yet due). A probe it
// hands out must be reported like one from Wait.
func (g *Gate) TryWait() (probe, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		return false, true
	}
	if !g.probing && !g.now().Before(g.until) {
		g.probing = true
		return true, true
	}
	return false, false
}

// WaitDue blocks until the gate is open or its probe is due, without
// taking the probe: a cursor waits here before reading its next slab so
// a dead remote costs no reads.
func (g *Gate) WaitDue(ctx context.Context) error {
	for {
		g.mu.Lock()
		now := g.now()
		if !g.closed || (!g.probing && !now.Before(g.until)) {
			g.mu.Unlock()
			return nil
		}
		wake, d := g.wake, time.Duration(-1)
		if !g.probing {
			d = g.until.Sub(now)
		}
		g.mu.Unlock()
		if err := sleepOrWake(ctx, wake, d); err != nil {
			return err
		}
	}
}

// sleepOrWake waits for ctx, wake or d (d < 0: no timeout).
func sleepOrWake(ctx context.Context, wake <-chan struct{}, d time.Duration) error {
	var timeout <-chan time.Time
	if d >= 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
	case <-timeout:
	}
	return nil
}

// Succeeded records an answer that proves the remote reachable and
// willing (an accepted chunk, or a refusal of one record or one topic):
// the gate opens and its backoff resets.
func (g *Gate) Succeeded(probe bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if probe {
		g.probing = false
	}
	g.failures = 0
	if g.closed || g.backoff != 0 {
		g.closed, g.backoff = false, 0
		g.signalLocked()
		if g.observe != nil {
			g.observe(0)
		}
	}
}

// Released gives back a probe that was never sent (its context ended,
// or its cursor found something else to wait for).
func (g *Gate) Released() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.probing {
		g.probing = false
		g.signalLocked()
	}
}

// Failed records a remote-wide or transient failure and reports whether
// the lane should back off on its own (a lone unavailable or edge
// answer; the gate stays open).
func (g *Gate) Failed(v Verdict, probe bool) (laneBackoff bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if probe {
		g.probing = false
	}
	if g.closed && !probe {
		// While the gate is closed only the probe is sent, so this
		// answers a chunk already in flight when it closed: the same
		// outage the closing answer reported, not a failed probe. It
		// counts, but never escalates the backoff or re-draws the wait;
		// a 429 only holds the probe until its Retry-After.
		g.failures++
		switch {
		case v.Action == ActGate && v.State == topic.RemoteStateAuthFailed && g.backoff < GateMaxBackoff:
			g.closeLocked(GateMaxBackoff, true)
		case v.Action == ActGate && v.State == topic.RemoteStateThrottled && v.RetryAfter > 0:
			if until := g.now().Add(v.RetryAfter); until.After(g.until) {
				g.until = until
			}
		}
		return false
	}
	switch {
	case v.Action == ActGate && v.State == topic.RemoteStateAuthFailed:
		// A wrong password costs the target one failed login per node
		// per ceiling, well inside its failed-login throttle.
		g.closeLocked(GateMaxBackoff, true)
	case v.Action == ActGate && v.State == topic.RemoteStateThrottled:
		g.closeLocked(max(v.RetryAfter, g.nextBackoffLocked()), v.RetryAfter > 0)
	case v.Action == ActGate:
		g.closeLocked(g.nextBackoffLocked(), false)
	default:
		g.failures++
		if !probe && !g.closed && g.failures < GateTripAfter {
			return true
		}
		g.closeLocked(g.nextBackoffLocked(), false)
	}
	return false
}

// Reset opens the gate for a new credential version: a corrected
// password takes effect without waiting out the backoff.
func (g *Gate) Reset() { g.Succeeded(false) }

// Backoff is the current backoff, 0 while the gate is healthy.
func (g *Gate) Backoff() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		return 0
	}
	return g.backoff
}

// WouldWait reports whether Wait would block now: the gate is closed
// and its probe is either out or not yet due. A lane holds its records
// before such a wait; one that would take the due probe holds nothing,
// so a lane that cannot hold still gets to probe.
func (g *Gate) WouldWait() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed && (g.probing || g.now().Before(g.until))
}

// Closed reports whether the gate is backing off.
func (g *Gate) Closed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

// Epoch counts the gate's closes. A cursor that saw it move re-runs its
// target check before its next chunk: when a remote comes back after a
// failure streak it may be a different cluster behind the same name.
func (g *Gate) Epoch() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch
}

func (g *Gate) nextBackoffLocked() time.Duration {
	if g.backoff < GateMinBackoff {
		return GateMinBackoff
	}
	return min(g.backoff*2, GateMaxBackoff)
}

// closeLocked closes the gate for a wait of backoff (full jitter unless
// exact).
func (g *Gate) closeLocked(backoff time.Duration, exact bool) {
	if !g.closed {
		g.epoch++
	}
	g.closed = true
	g.backoff = backoff
	wait := backoff
	if !exact {
		wait = jitter(backoff)
	}
	g.until = g.now().Add(wait)
	g.signalLocked()
	if g.observe != nil {
		g.observe(backoff)
	}
}

func (g *Gate) signalLocked() {
	close(g.wake)
	g.wake = make(chan struct{})
}
