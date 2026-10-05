package metastore

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// stageGhost adds id to leader's Raft configuration as a non-voter at an
// address nothing listens on, the way a joiner that never registered
// is left behind.
func stageGhost(t *testing.T, leader *Store, id string) {
	t.Helper()
	if adm, err := leader.AdmitJoiner(id, freeAddr(t)); err != nil || adm.Status != JoinStaged {
		t.Fatalf("AdmitJoiner(%s) = %+v, %v; want staged", id, adm, err)
	}
}

func requireRaftServer(t *testing.T, s *Store, id string, want bool) {
	t.Helper()
	if got, err := s.RaftServer(id); err != nil || got != want {
		t.Fatalf("RaftServer(%s) = %v, %v; want %v", id, got, err, want)
	}
}

// A staged joiner that never registered is removed from the Raft
// configuration, which also stops it holding back new entry types.
func TestForgetServerRemovesANonvoterWithNoMemberRecord(t *testing.T) {
	leader, _ := singleVoter(t, "fg-0")
	stageGhost(t, leader, "ghost")
	if ok, _ := leader.EveryMemberKnows(MaxEntryType); ok && MaxEntryType > legacyMaxEntryType {
		t.Fatal("an unregistered server did not hold the newest entry type back")
	}

	voter, err := leader.ForgetServer(context.Background(), "ghost")
	if err != nil || voter {
		t.Fatalf("ForgetServer(ghost) = %v, %v; want a non-voter forgotten", voter, err)
	}
	requireRaftServer(t, leader, "ghost", false)
	if ok, reason := leader.EveryMemberKnows(MaxEntryType); !ok {
		t.Fatalf("EveryMemberKnows(%d) after forget = false (%s), want true", MaxEntryType, reason)
	}
}

// A voter with no member record (admitted straight into the voter set by
// a 3.0.x leader, never registered) counts against quorum for good, and
// decommission cannot reach it. Forget removes it on the leader; a
// follower refuses as not the leader.
func TestForgetServerRemovesAVoterWithNoMemberRecord(t *testing.T) {
	stores := threeNodeCluster(t)
	var leader *Store
	waitUntil(t, 15*time.Second, "a leader", func() bool {
		for _, s := range stores {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	// The ghost: a voter at an address nothing listens on. Three live
	// voters of four still commit.
	if err := leader.r.AddVoter("ghost", raft.ServerAddress(freeAddr(t)), 0, barrierTimeout).Error(); err != nil {
		t.Fatalf("AddVoter(ghost): %v", err)
	}
	if voters, err := leader.Voters(); err != nil || len(voters) != 4 {
		t.Fatalf("voters = %v, %v; want the ghost added", voters, err)
	}
	for _, s := range stores {
		if s == leader {
			continue
		}
		if _, err := s.ForgetServer(context.Background(), "ghost"); !errors.Is(err, errs.ErrUnavailable) {
			t.Fatalf("ForgetServer on a follower = %v; want ErrUnavailable (not the leader)", err)
		}
		break
	}

	voter, err := leader.ForgetServer(context.Background(), "ghost")
	if err != nil || !voter {
		t.Fatalf("ForgetServer(ghost) = %v, %v; want a voter forgotten", voter, err)
	}
	voters, err := leader.Voters()
	if err != nil || len(voters) != 3 || slices.Contains(voters, "ghost") {
		t.Fatalf("voters after forget = %v, %v; want the three real ones", voters, err)
	}
}

// A server with a member record, alive or dead, is decommissioned, not
// forgotten: decommission moves its partitions away first.
func TestForgetServerRefusesAServerWithAMemberRecord(t *testing.T) {
	leader, _ := singleVoter(t, "fg-0")
	ctx := context.Background()
	stageGhost(t, leader, "fg-1")
	registerAlive(t, leader, "fg-1", "10.0.0.9:7942")
	if _, err := leader.ForgetServer(ctx, "fg-1"); !errors.Is(err, ErrMemberRecordExists) {
		t.Fatalf("ForgetServer(alive member) = %v, want ErrMemberRecordExists", err)
	}
	if err := leader.MarkMemberDead(ctx, "fg-1"); err != nil {
		t.Fatalf("MarkMemberDead: %v", err)
	}
	if _, err := leader.ForgetServer(ctx, "fg-1"); !errors.Is(err, ErrMemberRecordExists) {
		t.Fatalf("ForgetServer(dead member) = %v, want ErrMemberRecordExists", err)
	}
	requireRaftServer(t, leader, "fg-1", true)
}

// The leader cannot forget itself.
func TestForgetServerRefusesThisNode(t *testing.T) {
	leader, _ := singleVoter(t, "fg-0")
	if _, err := leader.ForgetServer(context.Background(), "fg-0"); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("ForgetServer(self) = %v, want ErrInvalidArgument", err)
	}
	requireRaftServer(t, leader, "fg-0", true)
}

// A server an assignment names as owner or move target is refused: the
// partition would point at a node that is gone.
func TestForgetServerRefusesAServerNamedByAnAssignment(t *testing.T) {
	leader, _ := singleVoter(t, "fg-0")
	ctx := context.Background()
	stageGhost(t, leader, "ghost")
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := leader.AssignPartition(ctx, "orders", 0, "ghost"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if _, err := leader.ForgetServer(ctx, "ghost"); !errors.Is(err, ErrServerNamedByAssignment) {
		t.Fatalf("ForgetServer(owner) = %v, want ErrServerNamedByAssignment", err)
	}
	if err := leader.AssignPartition(ctx, "orders", 0, "fg-0"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := leader.AssignPartition(ctx, "orders", 1, "fg-0"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := leader.SetAssignmentTarget(ctx, "orders", 1, "ghost"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	if _, err := leader.ForgetServer(ctx, "ghost"); !errors.Is(err, ErrServerNamedByAssignment) {
		t.Fatalf("ForgetServer(move target) = %v, want ErrServerNamedByAssignment", err)
	}
	requireRaftServer(t, leader, "ghost", true)
}

// An ID that is not in the Raft configuration is not found.
func TestForgetServerUnknownIDIsNotFound(t *testing.T) {
	leader, _ := singleVoter(t, "fg-0")
	if _, err := leader.ForgetServer(context.Background(), "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ForgetServer(nobody) = %v, want ErrNotFound", err)
	}
}
