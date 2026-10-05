package metastore

// Scale-out admission: a joiner is staged as a Raft non-voter and
// promoted to voter only when it asks again, caught up, and the leader
// sees it healthy.
//
// Admitting a joiner straight into the voter set applied the larger
// configuration's quorum the moment the entry was appended. A joiner the
// voters could not reach (a wrong advertise address, a certificate from
// another CA, a crash right after its join request), admitted while one
// of three voters was down, left two of four voters reachable: the
// leader lost its lease, no one could win an election, every node went
// not ready, and no configuration change could commit to undo it.
//
// A non-voter replicates but carries no quorum weight, so an unreachable
// joiner now costs nothing. Promotion needs no check of the voters'
// health: if the configuration can commit at all, the healthy voters h
// reach the quorum q(v) of the v voters, and h >= q(v) implies
// h+1 >= q(v+1), so adding a voter the leader is reaching keeps a
// healthy quorum. The only thing to prove is that the joiner is
// reachable, which its own request (it asks only once its replica has
// caught up with the leader) and the leader's heartbeat view show.
//
// Nothing here removes a non-voter or promotes one on its own. A
// non-voter that never asks (a 3.0.x binary) or keeps being deferred
// stays a replicating non-voter, visible in narad_raft_nonvoters and in
// the leader's log, until it asks again or is decommissioned.

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// Join outcomes, the "status" of a 200 answer to a join request.
const (
	// JoinStaged: the joiner was not in the configuration and was added
	// as a non-voter.
	JoinStaged = "staged"
	// JoinDeferred: the joiner is a non-voter and was not promoted;
	// JoinAdmission.Reason says why. It asks again later.
	JoinDeferred = "deferred"
	// JoinPromoted: the non-voter was promoted to voter by this request.
	JoinPromoted = "promoted"
	// JoinVoter: the joiner already is a voter (a retried join, or a
	// voter's leaderless join loop); at most its address changed.
	JoinVoter = "voter"
)

// JoinAdmission is the leader's answer to one join request.
type JoinAdmission struct {
	Status string
	Reason string
}

// promotionSettle is how long a node must have led before it promotes
// anyone, or forgets a voter (forget.go). A leader learns that a peer is
// unreachable from a failed heartbeat, and the first one to a peer that
// drops packets fails only after the transport's 10 s dial timeout; a
// leader younger than that cannot tell an unreachable peer from a
// healthy one. A variable so tests can shorten it.
var promotionSettle = 12 * time.Second

// AdmitJoiner is the leader's side of a join request. A node not in
// the Raft configuration is added as a non-voter (staged). A non-voter
// at the address it is known by is promoted to voter when this node has
// led for promotionSettle, its heartbeats to the joiner are not failing,
// and the joiner's member record exists, is alive and is not draining;
// otherwise the answer is deferred with the reason. A non-voter that
// asks from a new address is re-addressed and deferred. A voter stays a
// voter (AddNonvoter never demotes; a changed address is updated).
//
// Leader-only: a follower gets an errs.ErrUnavailable wrapping
// raft.ErrNotLeader. Calls are serialised, so two requests for one node
// cannot both act on the same configuration. The configuration changes
// pass no prevIndex: raft v1.8.0's GetConfiguration future reports index
// 0, so there is no index to guard on.
func (s *Store) AdmitJoiner(id, clusterAddr string) (JoinAdmission, error) {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	if s.r.State() != raft.Leader {
		return JoinAdmission{}, classifyRaftError(raft.ErrNotLeader)
	}
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return JoinAdmission{}, classifyRaftError(err)
	}
	sid, addr := raft.ServerID(id), raft.ServerAddress(clusterAddr)
	var current *raft.Server
	for _, srv := range future.Configuration().Servers {
		if srv.ID == sid {
			current = &srv
			break
		}
	}
	switch {
	case current == nil:
		if err := s.r.AddNonvoter(sid, addr, 0, barrierTimeout).Error(); err != nil {
			return JoinAdmission{}, classifyRaftError(err)
		}
		return JoinAdmission{Status: JoinStaged}, nil
	case current.Suffrage == raft.Voter:
		if current.Address != addr {
			if err := s.r.AddVoter(sid, addr, 0, barrierTimeout).Error(); err != nil {
				return JoinAdmission{}, classifyRaftError(err)
			}
		}
		return JoinAdmission{Status: JoinVoter}, nil
	case current.Address != addr:
		// Restarted under a new address: replicate there first, and judge
		// the promotion on a later request against the new address.
		if err := s.r.AddNonvoter(sid, addr, 0, barrierTimeout).Error(); err != nil {
			return JoinAdmission{}, classifyRaftError(err)
		}
		return JoinAdmission{Status: JoinDeferred, Reason: "raft address updated; ask again"}, nil
	}
	if reason := s.promotionDeferral(sid); reason != "" {
		return JoinAdmission{Status: JoinDeferred, Reason: reason}, nil
	}
	if err := s.r.AddVoter(sid, addr, 0, barrierTimeout).Error(); err != nil {
		return JoinAdmission{}, classifyRaftError(err)
	}
	return JoinAdmission{Status: JoinPromoted}, nil
}

// promotionDeferral returns why the non-voter id may not be promoted
// now, or "" when it may. The reasons carry no clock readings, so a
// joiner that logs each new reason logs a persisting one once.
func (s *Store) promotionDeferral(id raft.ServerID) string {
	if s.health.leaderFor() < promotionSettle {
		return fmt.Sprintf("the leader has led for less than %s, too short to have seen a failed heartbeat to an unreachable node", promotionSettle)
	}
	if _, failing := s.health.failingSince(id); failing {
		return "the leader's raft heartbeats to it are failing"
	}
	m, err := s.GetMember(string(id))
	switch {
	case errors.Is(err, ErrNotFound):
		return "it has no member record yet"
	case err != nil:
		return "its member record could not be read: " + err.Error()
	case m.Status != MemberAlive:
		return "its member record is marked " + string(m.Status)
	case m.Draining:
		return "it is draining"
	}
	return ""
}

// LocalVoter reports whether this node is a voter in the latest Raft
// configuration it knows. A staged joiner asks for promotion until it
// is.
func (s *Store) LocalVoter() (bool, error) {
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return false, err
	}
	for _, srv := range future.Configuration().Servers {
		if srv.ID == s.id {
			return srv.Suffrage == raft.Voter, nil
		}
	}
	return false, nil
}

// Nonvoters returns the IDs of the servers in the latest Raft
// configuration that do not vote: staged joiners not yet promoted.
// Decommission uses it to remove a drained non-voter from Raft.
func (s *Store) Nonvoters() ([]string, error) {
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var out []string
	for _, srv := range future.Configuration().Servers {
		if srv.Suffrage != raft.Voter {
			out = append(out, string(srv.ID))
		}
	}
	return out, nil
}
