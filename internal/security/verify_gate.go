package security

import (
	"context"
	"slices"
	"sync"
)

// verifyGate bounds concurrent bcrypt verifications (the only slow step
// of authentication) and decides who runs next when a slot frees. It
// has two waiting lanes: an attempt for a username with no failures
// outstanding ("clean", which every honest cold login is) always goes
// ahead of an attempt for a username that has some ("suspect", which a
// password-guessing flood becomes after its first try per name). Each
// lane is served in arrival order.
//
// A flood of wrong passwords therefore delays an honest login by at
// most the clean attempts already queued ahead of it (one per username
// the flood has not tried yet) and the attempts running, instead of by
// everything the flood ever queued; the node-wide failure budget (see
// Authenticator.admit) bounds how many suspect attempts are queued at
// all.
type verifyGate struct {
	mu      sync.Mutex
	free    int
	clean   []chan struct{}
	suspect []chan struct{}
}

func newVerifyGate(slots int) *verifyGate {
	return &verifyGate{free: slots}
}

// acquire takes a slot, waiting in the lane suspect selects, and
// returns ctx's error if ctx ends first. A nil error means the caller
// holds a slot and must release it.
func (g *verifyGate) acquire(ctx context.Context, suspect bool) error {
	g.mu.Lock()
	if g.free > 0 {
		g.free--
		g.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	if suspect {
		g.suspect = append(g.suspect, ch)
	} else {
		g.clean = append(g.clean, ch)
	}
	g.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
	}
	g.mu.Lock()
	stillWaiting := g.dequeueLocked(ch, suspect)
	g.mu.Unlock()
	if !stillWaiting {
		// release handed us the slot as we gave up: pass it on.
		g.release()
	}
	return ctx.Err()
}

// dequeueLocked removes ch from its lane, reporting whether it was
// still waiting there. Caller holds g.mu.
func (g *verifyGate) dequeueLocked(ch chan struct{}, suspect bool) bool {
	lane := &g.clean
	if suspect {
		lane = &g.suspect
	}
	i := slices.Index(*lane, ch)
	if i < 0 {
		return false
	}
	*lane = slices.Delete(*lane, i, i+1)
	return true
}

// release frees a slot, handing it straight to the first clean waiter,
// else to the first suspect waiter.
func (g *verifyGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case len(g.clean) > 0:
		ch := g.clean[0]
		g.clean = slices.Delete(g.clean, 0, 1)
		close(ch)
	case len(g.suspect) > 0:
		ch := g.suspect[0]
		g.suspect = slices.Delete(g.suspect, 0, 1)
		close(ch)
	default:
		g.free++
	}
}
