package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// AssignPartition records ownerID as the single owner of the partition
// through Raft, replacing any previous owner.
func (s *Store) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error {
	return s.apply(ctx, opAssignPartition, Assignment{Topic: topicName, Partition: partition, OwnerID: ownerID})
}

// GetAssignment reads the partition's assignment from the local replica.
// It returns ErrNotFound if the partition is unassigned.
func (s *Store) GetAssignment(topicName string, partition int) (Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var a Assignment
	err := s.fsm.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketAssignments).Get(assignmentKey(topicName, partition))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &a)
	})
	return a, err
}

// ListAssignments reads all of the topic's partition assignments from
// the local replica.
func (s *Store) ListAssignments(topicName string) ([]Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Assignment
	err := s.fsm.view(func(tx *bolt.Tx) error {
		prefix := []byte(topicName + ":")
		c := tx.Bucket(bucketAssignments).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var a Assignment
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			out = append(out, a)
		}
		return nil
	})
	return out, err
}

// AssignNewPartitions assigns each unassigned partition in
// [fromPartition, toPartition) to an active member. Narad has no
// follower replication: each partition has a single owner. Assignments
// are sticky — existing assignments are never reassigned here.
//
// A fan-out child gets anti-affine placement: partition p avoids the
// owner of the parent's partition p, so a keyed record's parent copy
// and child copy live on different nodes (the replica pattern). A child
// partition whose parent counterpart is still unassigned is deferred —
// the controller's reconcile sweep retries once the parent is placed.
func (s *Store) AssignNewPartitions(ctx context.Context, topicName string, fromPartition, toPartition int) error {
	members, err := s.ListMembers()
	if err != nil {
		return err
	}
	active := AliveMembers(members)
	if len(active) == 0 {
		return nil
	}
	active = RoundRobinMembers(active)

	parentOwners, parentPartitions, err := s.parentOwnersFor(ctx, topicName)
	if err != nil {
		return err
	}

	existing, err := s.ListAssignments(topicName)
	if err != nil {
		return err
	}
	assigned := make(map[int]bool, len(existing))
	for _, assignment := range existing {
		assigned[assignment.Partition] = true
	}

	for partition := fromPartition; partition < toPartition; partition++ {
		if assigned[partition] {
			continue
		}
		owner, ok, deferred := ChildAwareOwner(active, partition, parentOwners, parentPartitions)
		if deferred {
			continue
		}
		if !ok {
			return nil
		}
		if err := s.AssignPartition(ctx, topicName, partition, owner); err != nil {
			return err
		}
	}
	return nil
}

// parentOwnersFor resolves the anti-affinity constraint for a topic's
// assignment: if the topic is a fan-out child, it returns the parent's
// per-partition owners and partition count. A standalone topic — or a
// child whose parent vanished in a detach/delete race — returns (nil, 0):
// no constraint. Reads are local-replica only.
func (s *Store) parentOwnersFor(ctx context.Context, topicName string) (map[int]string, int, error) {
	t, err := s.GetTopic(ctx, topicName)
	if err != nil || t.Parent == "" {
		return nil, 0, nil
	}
	parent, err := s.GetTopic(ctx, t.Parent)
	if err != nil {
		return nil, 0, nil
	}
	assignments, err := s.ListAssignments(t.Parent)
	if err != nil {
		return nil, 0, err
	}
	owners := make(map[int]string, len(assignments))
	for _, a := range assignments {
		owners[a.Partition] = a.OwnerID
	}
	return owners, parent.Partitions, nil
}

// RoundRobinMembers returns a copy of active sorted by member ID, the
// canonical order for round-robin assignment: every node computes the
// same owner for the same partition.
func RoundRobinMembers(active []Member) []Member {
	out := append([]Member(nil), active...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

// RoundRobinOwner picks the owning member for a partition by round-robin
// over the (ID-sorted) active member list.
func RoundRobinOwner(active []Member, partition int) (string, bool) {
	if len(active) == 0 || partition < 0 {
		return "", false
	}
	return active[partition%len(active)].ID, true
}

// AntiAffineOwner picks the owner for a fan-out child's partition,
// walking the round-robin ring from the partition's canonical position
// until it finds a member other than avoidOwner — the owner of the
// parent's same-index partition, whose disk already holds the original
// copy. With a single live member there is nowhere else to go: it falls
// back to the canonical pick, because a colocated second copy still
// beats an unassigned partition.
func AntiAffineOwner(active []Member, partition int, avoidOwner string) (string, bool) {
	if len(active) == 0 || partition < 0 {
		return "", false
	}
	for i := range active {
		candidate := active[(partition+i)%len(active)]
		if candidate.ID != avoidOwner {
			return candidate.ID, true
		}
	}
	return active[partition%len(active)].ID, true
}

// ChildAwareOwner is the single owner-picking decision both assignment
// paths (topic create/alter and the controller reconcile sweep) share,
// so their placement can never diverge. parentPartitions == 0 means "no
// constraint" (standalone topic). A partition index beyond the parent's
// range has no same-index counterpart to avoid — plain round-robin. A
// partition whose parent counterpart exists but is unassigned reports
// deferred=true: assigning it now would be a blind guess that could
// colocate the copies, and the reconcile sweep retries within seconds.
func ChildAwareOwner(active []Member, partition int, parentOwners map[int]string, parentPartitions int) (owner string, ok bool, deferred bool) {
	if parentPartitions == 0 || partition >= parentPartitions {
		owner, ok = RoundRobinOwner(active, partition)
		return owner, ok, false
	}
	avoid := parentOwners[partition]
	if avoid == "" {
		return "", false, true
	}
	owner, ok = AntiAffineOwner(active, partition, avoid)
	return owner, ok, false
}

// AliveMembers filters members down to those with MemberAlive status.
func AliveMembers(members []Member) []Member {
	active := make([]Member, 0, len(members))
	for _, member := range members {
		if member.Status == MemberAlive {
			active = append(active, member)
		}
	}
	return active
}
