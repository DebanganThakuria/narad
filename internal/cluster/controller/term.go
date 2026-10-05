package controller

import (
	"context"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// leaderTerm is the controller's in-memory state for one stretch of
// leadership. It is built when the leader loop starts and dropped when
// leadership ends, so the next leader re-derives everything in it from
// Raft. What it remembers only ever makes a new leader wait longer, never
// act sooner: a new leader starts its own dead-marking clock at its
// election and Barriers before it judges anyone.
type leaderTerm struct {
	mu sync.Mutex
	// since is when this node's leader loop started, on the controller's
	// clock; zero for passes run outside a leader loop (no grace).
	since time.Time
	// barrierOK is set once a Barrier succeeded in this term: the FSM
	// then reflects everything earlier leaders committed.
	barrierOK bool
	// statuses is the member status last seen by the heartbeat pass, to
	// log a member turning dead or alive once.
	statuses map[string]metastore.MemberStatus
	// refusing is set while the dead-marking breaker refuses verdicts,
	// to log the refusal once per streak.
	refusing bool
	// blocked maps a draining node to the reasons its decommission is
	// blocked, as last logged and exported, each to whether it needed an
	// operator (logged at error) then.
	blocked map[string]map[string]bool
	// movesLogged holds the blocked moves already logged in this term.
	movesLogged map[string]bool
}

func newLeaderTerm(since time.Time) *leaderTerm {
	return &leaderTerm{since: since}
}

type termKey struct{}

// withTerm returns ctx carrying t, so every pass of one leader loop
// shares one term, and a pass still running from a loop that lost
// leadership keeps its own.
func withTerm(ctx context.Context, t *leaderTerm) context.Context {
	return context.WithValue(ctx, termKey{}, t)
}

// term returns the leader state of the loop ctx belongs to. A pass run
// outside a leader loop (tests call the passes directly) shares one
// default term whose since is zero: no election grace.
func (c *Controller) term(ctx context.Context) *leaderTerm {
	if t, ok := ctx.Value(termKey{}).(*leaderTerm); ok && t != nil {
		return t
	}
	c.termMu.Lock()
	defer c.termMu.Unlock()
	if c.defaultTerm == nil {
		c.defaultTerm = newLeaderTerm(time.Time{})
	}
	return c.defaultTerm
}

// termBarrier makes sure a Barrier has succeeded in this term before
// the caller acts on the local FSM. A freshly elected leader's log is
// complete but its FSM may still be applying what earlier leaders
// committed (fresher heartbeats among them); one successful Barrier per
// term closes that window.
func (c *Controller) termBarrier(t *leaderTerm) error {
	t.mu.Lock()
	ok := t.barrierOK
	t.mu.Unlock()
	if ok {
		return nil
	}
	if err := c.store.Barrier(); err != nil {
		return err
	}
	c.noteBarrier(t)
	return nil
}

// noteBarrier records a successful Barrier in this term.
func (c *Controller) noteBarrier(t *leaderTerm) {
	t.mu.Lock()
	t.barrierOK = true
	t.mu.Unlock()
}

// deadFor reports whether a member whose last heartbeat is stamped
// lastHeartbeat (Unix seconds) has been silent for at least d, as far as
// this leader can tell. Stamps are written by whichever leader applied
// the heartbeat, and none are written while the cluster has no leader,
// so an old stamp read by a new leader says nothing about the stretch
// before its election. A member is therefore judged only once this
// leader's loop has itself run for d, measured on the monotonic clock,
// and its stamp is older than d.
func (c *Controller) deadFor(t *leaderTerm, lastHeartbeat int64, d time.Duration, now time.Time) bool {
	t.mu.Lock()
	since := t.since
	t.mu.Unlock()
	if !since.IsZero() && now.Sub(since) < d {
		return false
	}
	return lastHeartbeat < now.Unix()-int64(d.Seconds())
}

// beginTerm makes t the term whose leader-only metrics this node
// exports, starting them from zero.
func (c *Controller) beginTerm(t *leaderTerm) {
	c.termMu.Lock()
	defer c.termMu.Unlock()
	c.activeTerm = t
	c.m.reset()
	c.blockedMoves.Store(nil)
}

// endTerm withdraws the leader-only series t exported, so a node that
// lost leadership does not keep reporting a stale verdict. A term that a
// newer one already replaced leaves the newer one's series alone.
func (c *Controller) endTerm(t *leaderTerm) {
	c.termMu.Lock()
	defer c.termMu.Unlock()
	if c.activeTerm != t {
		return
	}
	c.activeTerm = nil
	c.m.reset()
	c.blockedMoves.Store(nil)
}

// publish runs fn, which writes leader-only metrics, unless t is a term
// a newer one has replaced (a pass still finishing after leadership
// moved on). Passes run outside a leader loop always publish.
func (c *Controller) publish(t *leaderTerm, fn func()) {
	c.termMu.Lock()
	defer c.termMu.Unlock()
	if c.activeTerm != nil && c.activeTerm != t {
		return
	}
	fn()
}
