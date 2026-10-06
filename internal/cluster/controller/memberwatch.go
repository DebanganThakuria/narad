package controller

import (
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// memberWatchInterval is how often the leader loop looks for a member
// that has turned alive since the last look. Each look is one atomic
// load of the routing-members version; the member list is read only
// when that version has moved.
const memberWatchInterval = 250 * time.Millisecond

// memberWatch lets the leader place partitions soon after a member turns
// alive instead of on the next ReconcileInterval tick.
//
// On a fresh cluster the member table is empty for the first moments
// after /readyz, so a topic created then gets no owners from the create
// path; it used to wait up to a full reconcile interval (10 s) for the
// sweep, with produces parked in the ingress WAL and consumers getting
// empty answers. The same happens after a full-cluster restart, while
// members are still re-registering with the new leader.
//
// The out-of-cycle pass is debounced. Members register a few hundred
// milliseconds apart, and a pass run on the first arrival would hand
// that member every unassigned partition, which rebalance would then
// have to move. The watch waits until every Raft voter is alive, or
// until MemberSettleDelay has passed since the latest member turned
// alive, whichever comes first; a voter that never comes back only
// costs that delay.
type memberWatch struct {
	// version is the routing-members version alive was read at.
	version uint64
	// alive is the set of alive member IDs at version.
	alive map[string]bool
	// pendingSince is when the latest member turned alive while a pass
	// was still owed; zero when none is owed.
	pendingSince time.Time
}

// newMemberWatch snapshots the current alive set. Taken before the
// leader's first pass, so a member that turns alive while that pass runs
// still triggers a later one.
func (c *Controller) newMemberWatch() *memberWatch {
	w := &memberWatch{version: c.store.RoutingMembersVersion()}
	if members, err := c.store.ListMembers(); err == nil {
		w.alive = aliveMemberSet(members)
	}
	return w
}

// memberPassDue records any member that turned alive since the last call
// and reports whether the debounced out-of-cycle pass should run now.
// A true answer clears the pending pass.
func (c *Controller) memberPassDue(w *memberWatch, now time.Time) bool {
	if v := c.store.RoutingMembersVersion(); v != w.version {
		members, err := c.store.ListMembers()
		if err != nil {
			return false // keep the old version; the next look retries
		}
		alive := aliveMemberSet(members)
		for id := range alive {
			if !w.alive[id] {
				w.pendingSince = now
				break
			}
		}
		w.version, w.alive = v, alive
	}
	if w.pendingSince.IsZero() {
		return false
	}
	if now.Sub(w.pendingSince) < c.cfg.MemberSettleDelay && !c.everyVoterAlive(w.alive) {
		return false
	}
	w.pendingSince = time.Time{}
	return true
}

// everyVoterAlive reports whether every Raft voter has an alive member
// record, i.e. no member the cluster expects is still to arrive.
func (c *Controller) everyVoterAlive(alive map[string]bool) bool {
	voters, err := c.store.Voters()
	if err != nil || len(voters) == 0 {
		return false
	}
	for _, id := range voters {
		if !alive[id] {
			return false
		}
	}
	return true
}

func aliveMemberSet(members []metastore.Member) map[string]bool {
	alive := make(map[string]bool, len(members))
	for _, m := range members {
		if m.Status == metastore.MemberAlive {
			alive[m.ID] = true
		}
	}
	return alive
}
