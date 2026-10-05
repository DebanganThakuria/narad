package metastore

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// settlePromotionsAfter sets how long a leader must have led before it
// promotes a non-voter, for the length of the test.
func settlePromotionsAfter(t *testing.T, d time.Duration) {
	t.Helper()
	old := promotionSettle
	promotionSettle = d
	t.Cleanup(func() { promotionSettle = old })
}

// singleVoter opens a bootstrapped one-node store and waits until it
// leads with its ownership view ready.
func singleVoter(t *testing.T, id string) (*Store, string) {
	t.Helper()
	addr := freeAddr(t)
	s, err := New(Config{NodeID: id, DataDir: t.TempDir(), BindAddr: addr, AdvertiseAddr: addr})
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	waitUntil(t, 10*time.Second, "the store to lead", func() bool {
		if !s.IsLeader() {
			return false
		}
		_, err := s.ListAssignments("__probe__")
		return err == nil
	})
	return s, addr
}

// joinOnlyStore opens a store that waits to be admitted.
func joinOnlyStore(t *testing.T, id string) (*Store, string) {
	t.Helper()
	addr := freeAddr(t)
	s, err := New(Config{NodeID: id, DataDir: t.TempDir(), BindAddr: addr, AdvertiseAddr: addr, JoinOnly: true})
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, addr
}

// stageCaughtUp admits joiner through leader as a non-voter and waits
// until its replica has caught up with the leader.
func stageCaughtUp(t *testing.T, leader, joiner *Store, id, addr string) {
	t.Helper()
	adm, err := leader.AdmitJoiner(id, addr)
	if err != nil || adm.Status != JoinStaged {
		t.Fatalf("AdmitJoiner(%s) = %+v, %v; want staged", id, adm, err)
	}
	waitUntil(t, 10*time.Second, "the staged joiner to catch up", joiner.AppliedCaughtUp)
}

func registerAlive(t *testing.T, leader *Store, id, addr string) {
	t.Helper()
	if err := leader.RegisterMember(context.Background(), Member{ID: id, Addr: addr, ClusterAddr: addr, Status: MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("RegisterMember(%s): %v", id, err)
	}
}

// suffrageOf reports id's suffrage in s's latest configuration.
func suffrageOf(s *Store, id string) (raft.ServerSuffrage, bool) {
	for _, srv := range s.r.GetConfiguration().Configuration().Servers {
		if string(srv.ID) == id {
			return srv.Suffrage, true
		}
	}
	return 0, false
}

func requireDeferred(t *testing.T, adm JoinAdmission, err error, reasonContains string) {
	t.Helper()
	if err != nil || adm.Status != JoinDeferred || !strings.Contains(adm.Reason, reasonContains) {
		t.Fatalf("AdmitJoiner = %+v, %v; want deferred with a reason containing %q", adm, err, reasonContains)
	}
}

// A staged joiner that has caught up and asks again is promoted to
// voter, and then answers as a voter.
func TestCaughtUpJoinerIsPromotedOnRequest(t *testing.T) {
	settlePromotionsAfter(t, 100*time.Millisecond)
	leader, leaderAddr := singleVoter(t, "ad-0")
	joiner, joinerAddr := joinOnlyStore(t, "ad-1")
	stageCaughtUp(t, leader, joiner, "ad-1", joinerAddr)
	if suf, ok := suffrageOf(leader, "ad-1"); !ok || suf != raft.Nonvoter {
		t.Fatalf("staged joiner suffrage = %v (present %v), want Nonvoter", suf, ok)
	}
	if voter, err := joiner.LocalVoter(); err != nil || voter {
		t.Fatalf("staged joiner LocalVoter() = %v, %v; want false", voter, err)
	}
	registerAlive(t, leader, "ad-0", leaderAddr)
	registerAlive(t, leader, "ad-1", joinerAddr)

	adm, err := leader.AdmitJoiner("ad-1", joinerAddr)
	if err != nil || adm.Status != JoinPromoted {
		t.Fatalf("AdmitJoiner from a caught-up, healthy non-voter = %+v, %v; want promoted", adm, err)
	}
	if voters, err := leader.Voters(); err != nil || len(voters) != 2 || !slices.Contains(voters, "ad-1") {
		t.Fatalf("voters = %v, %v; want ad-0 and ad-1", voters, err)
	}
	if nonvoters, err := leader.Nonvoters(); err != nil || len(nonvoters) != 0 {
		t.Fatalf("non-voters = %v, %v; want none", nonvoters, err)
	}
	waitUntil(t, 10*time.Second, "the joiner to see itself as a voter", func() bool {
		voter, err := joiner.LocalVoter()
		return err == nil && voter
	})
	if adm, err := leader.AdmitJoiner("ad-1", joinerAddr); err != nil || adm.Status != JoinVoter {
		t.Fatalf("AdmitJoiner from the promoted voter = %+v, %v; want voter", adm, err)
	}
}

// A leader promotes no one until it has led for promotionSettle: a
// younger leader may not have seen the first failed heartbeat to an
// unreachable node yet.
func TestPromotionWaitsForTheLeaderToSettle(t *testing.T) {
	settlePromotionsAfter(t, time.Hour)
	leader, leaderAddr := singleVoter(t, "ad-0")
	joiner, joinerAddr := joinOnlyStore(t, "ad-1")
	stageCaughtUp(t, leader, joiner, "ad-1", joinerAddr)
	registerAlive(t, leader, "ad-0", leaderAddr)
	registerAlive(t, leader, "ad-1", joinerAddr)

	adm, err := leader.AdmitJoiner("ad-1", joinerAddr)
	requireDeferred(t, adm, err, "has led for less than")
	if suf, _ := suffrageOf(leader, "ad-1"); suf != raft.Nonvoter {
		t.Fatalf("joiner suffrage after a deferral = %v, want Nonvoter", suf)
	}

	promotionSettle = 0
	if adm, err := leader.AdmitJoiner("ad-1", joinerAddr); err != nil || adm.Status != JoinPromoted {
		t.Fatalf("AdmitJoiner once settled = %+v, %v; want promoted", adm, err)
	}
}

// A non-voter the leader fails to heartbeat is not promoted, whatever
// its member record says: a voter nobody reaches costs quorum.
func TestPromotionIsDeferredForAJoinerTheLeaderCannotReach(t *testing.T) {
	settlePromotionsAfter(t, 100*time.Millisecond)
	leader, leaderAddr := singleVoter(t, "ad-0")
	joiner, joinerAddr := joinOnlyStore(t, "ad-1")
	stageCaughtUp(t, leader, joiner, "ad-1", joinerAddr)
	registerAlive(t, leader, "ad-0", leaderAddr)
	registerAlive(t, leader, "ad-1", joinerAddr)
	if err := joiner.Close(); err != nil {
		t.Fatalf("Close(joiner): %v", err)
	}
	waitUntil(t, 10*time.Second, "the leader to see the joiner's heartbeats fail", func() bool {
		_, failing := leader.health.failingSince("ad-1")
		return failing
	})

	adm, err := leader.AdmitJoiner("ad-1", joinerAddr)
	requireDeferred(t, adm, err, "heartbeats to it are failing")
	if voters, err := leader.Voters(); err != nil || !slices.Equal(voters, []string{"ad-0"}) {
		t.Fatalf("voters = %v, %v; want only ad-0", voters, err)
	}
}

// A non-voter without a member record, or whose record is draining or
// marked dead, is not promoted; once its record is alive and not
// draining it is.
func TestPromotionIsDeferredForADrainingOrUnregisteredJoiner(t *testing.T) {
	ctx := context.Background()
	settlePromotionsAfter(t, 100*time.Millisecond)
	leader, leaderAddr := singleVoter(t, "ad-0")
	joiner, joinerAddr := joinOnlyStore(t, "ad-1")
	stageCaughtUp(t, leader, joiner, "ad-1", joinerAddr)
	registerAlive(t, leader, "ad-0", leaderAddr)
	waitUntil(t, 5*time.Second, "the leader to have led for the settle time", func() bool {
		return leader.health.leaderFor() >= promotionSettle
	})

	adm, err := leader.AdmitJoiner("ad-1", joinerAddr)
	requireDeferred(t, adm, err, "no member record")

	registerAlive(t, leader, "ad-1", joinerAddr)
	if err := leader.SetMemberDraining(ctx, "ad-1", true); err != nil {
		t.Fatalf("SetMemberDraining: %v", err)
	}
	adm, err = leader.AdmitJoiner("ad-1", joinerAddr)
	requireDeferred(t, adm, err, "draining")

	if err := leader.SetMemberDraining(ctx, "ad-1", false); err != nil {
		t.Fatalf("SetMemberDraining(false): %v", err)
	}
	if err := leader.MarkMemberDead(ctx, "ad-1"); err != nil {
		t.Fatalf("MarkMemberDead: %v", err)
	}
	adm, err = leader.AdmitJoiner("ad-1", joinerAddr)
	requireDeferred(t, adm, err, "marked dead")
	if suf, _ := suffrageOf(leader, "ad-1"); suf != raft.Nonvoter {
		t.Fatalf("joiner suffrage after deferrals = %v, want Nonvoter", suf)
	}

	registerAlive(t, leader, "ad-1", joinerAddr)
	if adm, err := leader.AdmitJoiner("ad-1", joinerAddr); err != nil || adm.Status != JoinPromoted {
		t.Fatalf("AdmitJoiner with an alive record = %+v, %v; want promoted", adm, err)
	}
}

// A join from a node that already votes (a retried join, or a voter's
// leaderless join loop) answers voter and leaves the configuration as
// it was: it is never demoted to a non-voter.
func TestJoinOfAnExistingVoterChangesNothing(t *testing.T) {
	stores := threeNodeCluster(t)
	var leader *Store
	waitUntil(t, 10*time.Second, "a leader", func() bool {
		for _, s := range stores {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	before := leader.r.GetConfiguration().Configuration().Servers
	for _, srv := range before {
		adm, err := leader.AdmitJoiner(string(srv.ID), string(srv.Address))
		if err != nil || adm.Status != JoinVoter {
			t.Fatalf("AdmitJoiner(%s) = %+v, %v; want voter", srv.ID, adm, err)
		}
	}
	if after := leader.r.GetConfiguration().Configuration().Servers; !slices.Equal(after, before) {
		t.Fatalf("configuration after joins from voters = %v, want %v", after, before)
	}
	for _, s := range stores {
		if voter, err := s.LocalVoter(); err != nil || !voter {
			t.Fatalf("LocalVoter() on %s = %v, %v; want true", s.id, voter, err)
		}
	}
}

// Nothing removes a staged non-voter on its own, however long it stays
// unreachable and unregistered: the leader neither prunes nor promotes
// on a timer. It stays until it asks again or is decommissioned.
func TestNothingRemovesAStagedNonvoterOnItsOwn(t *testing.T) {
	settlePromotionsAfter(t, 100*time.Millisecond)
	leader, _ := singleVoter(t, "ad-0")
	ghostAddr := freeAddr(t) // nothing listens there
	if adm, err := leader.AdmitJoiner("ghost", ghostAddr); err != nil || adm.Status != JoinStaged {
		t.Fatalf("AdmitJoiner(ghost) = %+v, %v; want staged", adm, err)
	}
	waitUntil(t, 10*time.Second, "the leader to see the ghost's heartbeats fail", func() bool {
		_, failing := leader.health.failingSince("ghost")
		return failing
	})
	deadline := time.Now().Add(10 * time.Second) // 100 settle periods
	for time.Now().Before(deadline) {
		if suf, ok := suffrageOf(leader, "ghost"); !ok || suf != raft.Nonvoter {
			t.Fatalf("ghost suffrage = %v (present %v), want it still a non-voter", suf, ok)
		}
		time.Sleep(250 * time.Millisecond)
	}
	adm, err := leader.AdmitJoiner("ghost", ghostAddr)
	requireDeferred(t, adm, err, "heartbeats to it are failing")
	if nonvoters, err := leader.Nonvoters(); err != nil || !slices.Equal(nonvoters, []string{"ghost"}) {
		t.Fatalf("non-voters = %v, %v; want [ghost]", nonvoters, err)
	}
	if voters, err := leader.Voters(); err != nil || !slices.Equal(voters, []string{"ad-0"}) {
		t.Fatalf("voters = %v, %v; want only ad-0", voters, err)
	}
}

// A failed heartbeat counts against a peer only while it is recent, so
// a resumed-heartbeat observation dropped by a full observer channel
// does not hold a healthy peer back for good.
func TestHeartbeatFailureCountsOnlyWhileRecent(t *testing.T) {
	leader, _ := singleVoter(t, "ad-0")
	h := leader.health
	h.handle(raft.FailedHeartbeatObservation{PeerID: "peer"}, time.Now())
	if _, failing := h.failingSince("peer"); !failing {
		t.Fatal("a peer that just failed a heartbeat does not count as failing")
	}
	h.handle(raft.FailedHeartbeatObservation{PeerID: "peer"}, time.Now().Add(-heartbeatFailureWindow))
	if _, failing := h.failingSince("peer"); failing {
		t.Fatalf("a peer whose last failure is %s old still counts as failing", heartbeatFailureWindow)
	}
	h.handle(raft.FailedHeartbeatObservation{PeerID: "peer"}, time.Now())
	h.handle(raft.ResumedHeartbeatObservation{PeerID: "peer"}, time.Now())
	if _, failing := h.failingSince("peer"); failing {
		t.Fatal("a resumed heartbeat did not clear the peer")
	}
}
