package handlers

import "sync/atomic"

// DrainGate admits client produce on a node that may be decommissioned.
// It holds the node's drain flag and counts the client produce requests
// it admitted and has not seen answered, so a node can report when no
// produce can reach its ingress WAL any more: every request admitted
// before the flag was set has been answered, and every later one is
// refused. The zero value admits everything; a nil gate too.
type DrainGate struct {
	draining atomic.Bool
	inFlight atomic.Int64
}

// SetDraining sets the drain flag. serve keeps it equal to this node's
// own member record.
func (g *DrainGate) SetDraining(v bool) { g.draining.Store(v) }

// Draining reports the drain flag; false for a nil gate.
func (g *DrainGate) Draining() bool { return g != nil && g.draining.Load() }

// InFlight reports how many admitted client produce requests have not
// been answered yet; 0 for a nil gate.
func (g *DrainGate) InFlight() int64 {
	if g == nil {
		return 0
	}
	return g.inFlight.Load()
}

// Admit admits one client produce unless the drain flag is set. A
// request it admits calls Done once it is answered, after anything it
// accepted is durable in the ingress WAL.
//
// The count is raised before the flag is read, and a reader takes the
// flag before the count (Draining, then InFlight, then the WAL backlog).
// A reader that sees the flag set and then a count of 0 therefore knows
// that every request admitted before the flag was set has already been
// answered, so whatever it accepted is in the backlog it reads next, and
// that none is admitted while the flag stays set.
func (g *DrainGate) Admit() bool {
	if g == nil {
		return true
	}
	g.inFlight.Add(1)
	if g.draining.Load() {
		g.inFlight.Add(-1)
		return false
	}
	return true
}

// Done ends a request Admit admitted.
func (g *DrainGate) Done() {
	if g != nil {
		g.inFlight.Add(-1)
	}
}
