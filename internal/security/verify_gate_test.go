package security

import (
	"context"
	"errors"
	"testing"
	"time"
)

// waitUntil polls cond for up to 5 seconds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (g *verifyGate) waiting() (clean, suspect int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.clean), len(g.suspect)
}

func (g *verifyGate) idle() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.free
}

func TestVerifyGateServesCleanWaitersBeforeSuspectOnes(t *testing.T) {
	g := newVerifyGate(1)
	if err := g.acquire(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	order := make(chan string, 3)
	go func() {
		_ = g.acquire(context.Background(), true)
		order <- "suspect"
	}()
	waitUntil(t, "the suspect waiter", func() bool { _, s := g.waiting(); return s == 1 })
	go func() {
		_ = g.acquire(context.Background(), false)
		order <- "clean-1"
	}()
	waitUntil(t, "the first clean waiter", func() bool { c, _ := g.waiting(); return c == 1 })
	go func() {
		_ = g.acquire(context.Background(), false)
		order <- "clean-2"
	}()
	waitUntil(t, "the second clean waiter", func() bool { c, _ := g.waiting(); return c == 2 })

	for _, want := range []string{"clean-1", "clean-2", "suspect"} {
		g.release()
		if got := <-order; got != want {
			t.Fatalf("the slot went to %s, want %s (clean waiters first, each lane in arrival order)", got, want)
		}
	}
	g.release()
	if free := g.idle(); free != 1 {
		t.Fatalf("free slots = %d, want 1", free)
	}
}

// A waiter that gives up leaves its lane, and one handed the slot just
// as it gives up passes the slot on: slots never leak.
func TestVerifyGateCancelledWaiterPassesItsSlotOn(t *testing.T) {
	g := newVerifyGate(1)
	if err := g.acquire(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.acquire(ctx, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire on a full gate: %v, want deadline exceeded", err)
	}
	if c, s := g.waiting(); c != 0 || s != 0 {
		t.Fatalf("a waiter that gave up is still queued: clean %d, suspect %d", c, s)
	}
	g.release()
	if free := g.idle(); free != 1 {
		t.Fatalf("free slots = %d after the cancelled waiter, want 1", free)
	}

	// Race the hand-off against the cancellation many times: whichever
	// wins, exactly one slot exists afterwards.
	for range 200 {
		if err := g.acquire(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		go func() { got <- g.acquire(ctx, false) }()
		waitUntil(t, "the waiter", func() bool { c, _ := g.waiting(); return c == 1 })
		go cancel()
		g.release()
		if err := <-got; err == nil {
			g.release() // the waiter won the slot; give it back
		}
		cancel()
		if free := g.idle(); free != 1 {
			t.Fatalf("free slots = %d, want 1", free)
		}
	}
}
