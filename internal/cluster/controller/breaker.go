package controller

// The dead-marking breaker: a dead verdict that would leave most Raft
// voters marked dead is evidence that the judge is broken, not the
// voters.
//
// Member liveness is judged from heartbeats that reach the leader over
// the node RPC plane, while the leader's authority comes from Raft.
// hashicorp/raft steps a leader down once it has not heard from a quorum
// of voters for its lease, so a node still running the leader loop is in
// touch with a quorum of voters over Raft. A heartbeat pass that would
// leave fewer alive voters than a quorum therefore points at the
// heartbeat path into this leader (a blocked cluster port, a mismatched
// secret, a wedged listener), and marking those voters dead would make
// every node route around live owners.
//
// The breaker refuses such a verdict for every voter in it, marks
// members that are not voters as usual, logs the refusal at error once
// per refusing streak and sets narad_dead_marking_refused to 1. It can
// never hide a real failure: if most voters were really down, this node
// could not be leading. Five voters with two crashed leaves three alive,
// a quorum, and both are marked. It does nothing on its own beyond
// refusing: no leadership transfer, no switch.

import (
	"slices"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// applyBreaker returns the members of verdict this pass may mark dead.
func (c *Controller) applyBreaker(t *leaderTerm, members, verdict []metastore.Member) []metastore.Member {
	if len(verdict) == 0 {
		c.breakerClear(t)
		return nil
	}
	voters, err := c.store.Voters()
	if err != nil {
		// Without the configuration there is no quorum to check
		// against. A dead verdict can wait a pass.
		c.logger().Debug("controller: heartbeat verdict deferred: Raft configuration unavailable", "err", err)
		return nil
	}
	refused, allowed, aliveAfter, quorum := splitDeadVerdict(voters, members, verdict, c.store.LeaderID())
	if len(refused) == 0 {
		c.breakerClear(t)
		return verdict
	}
	c.publish(t, func() { c.m.setDeadMarkingRefused(true) })
	t.mu.Lock()
	first := !t.refusing
	t.refusing = true
	t.mu.Unlock()
	if first {
		c.logger().Error("controller: refusing to mark voters dead: the verdict would leave fewer alive voters than a Raft quorum, which a leader holding its lease rules out; check the cluster RPC plane (port, secret, certificates) into this leader",
			"refused", memberIDs(refused), "alive_voters_after", aliveAfter, "quorum", quorum, "voters", voters)
	}
	return allowed
}

// breakerClear ends a refusing streak after a pass the breaker let
// through.
func (c *Controller) breakerClear(t *leaderTerm) {
	t.mu.Lock()
	was := t.refusing
	t.refusing = false
	t.mu.Unlock()
	c.publish(t, func() { c.m.setDeadMarkingRefused(false) })
	if was {
		c.logger().Info("controller: dead-marking breaker cleared; heartbeat verdicts apply again")
	}
}

// splitDeadVerdict splits verdict into the members the breaker refuses
// and the ones it allows. Nothing is refused while the voters left alive
// after the verdict still form a quorum; otherwise every voter in the
// verdict is refused and members that are not voters are allowed. The
// leader counts as alive (it is running this pass); a voter with no
// member record, or one already dead, does not.
func splitDeadVerdict(voters []string, members, verdict []metastore.Member, leaderID string) (refused, allowed []metastore.Member, aliveAfter, quorum int) {
	if len(voters) == 0 {
		return nil, verdict, 0, 0
	}
	quorum = len(voters)/2 + 1
	inVerdict := make(map[string]bool, len(verdict))
	for _, m := range verdict {
		inVerdict[m.ID] = true
	}
	alive := make(map[string]bool, len(members))
	for _, m := range members {
		alive[m.ID] = m.Status == metastore.MemberAlive
	}
	for _, v := range voters {
		if v == leaderID || (alive[v] && !inVerdict[v]) {
			aliveAfter++
		}
	}
	if aliveAfter >= quorum {
		return nil, verdict, aliveAfter, quorum
	}
	for _, m := range verdict {
		if slices.Contains(voters, m.ID) {
			refused = append(refused, m)
		} else {
			allowed = append(allowed, m)
		}
	}
	return refused, allowed, aliveAfter, quorum
}

func memberIDs(ms []metastore.Member) []string {
	ids := make([]string, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	return ids
}
