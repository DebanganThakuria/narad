package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error {
	return s.apply(ctx, opAssignPartition, Assignment{Topic: topicName, Partition: partition, OwnerID: ownerID})
}

func (s *Store) GetAssignment(topicName string, partition int) (Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var a Assignment
	err := s.fsm.view("get_assignment", func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketAssignments).Get(assignmentKey(topicName, partition))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &a)
	})
	return a, err
}

func (s *Store) ListAssignments(topicName string) ([]Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Assignment
	err := s.fsm.view("list_assignments", func(tx *bolt.Tx) error {
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
		owner, ok := RoundRobinOwner(active, partition)
		if !ok {
			return nil
		}
		if err := s.AssignPartition(ctx, topicName, partition, owner); err != nil {
			return err
		}
	}
	return nil
}

// RoundRobinMembers returns a copy of active sorted by member ID. It does not
// itself assign partitions; the deterministic ID order it produces is what
// makes the subsequent RoundRobinOwner picks stable and identical across nodes.
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

func AliveMembers(members []Member) []Member {
	active := make([]Member, 0, len(members))
	for _, member := range members {
		if member.Status == MemberAlive {
			active = append(active, member)
		}
	}
	return active
}
