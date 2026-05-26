package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string, followerID string) error {
	return s.apply(ctx, opAssignPartition, Assignment{Topic: topicName, Partition: partition, OwnerID: ownerID, FollowerID: followerID})
}

func (s *Store) GetAssignment(topicName string, partition int) (Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var a Assignment
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
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
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
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

func (s *Store) AssignNewPartitions(ctx context.Context, topicName string, fromPartition, toPartition int, replicationFactor int) error {
	members, err := s.ListMembers()
	if err != nil {
		return err
	}
	active := AliveMembers(members)
	if replicationFactor < 2 || len(active) < replicationFactor {
		return nil
	}

	counts := make(map[string]int, len(active))
	for _, member := range active {
		counts[member.ID] = 0
	}

	existing, err := s.ListAssignments(topicName)
	if err != nil {
		return err
	}
	assigned := make(map[int]bool, len(existing))
	for _, assignment := range existing {
		if _, ok := counts[assignment.OwnerID]; ok {
			counts[assignment.OwnerID]++
		}
		if assignment.FollowerID != "" {
			if _, ok := counts[assignment.FollowerID]; ok {
				counts[assignment.FollowerID]++
			}
		}
		assigned[assignment.Partition] = true
	}

	for partition := fromPartition; partition < toPartition; partition++ {
		if assigned[partition] {
			continue
		}
		sort.Slice(active, func(i, j int) bool {
			if counts[active[i].ID] == counts[active[j].ID] {
				return active[i].ID < active[j].ID
			}
			return counts[active[i].ID] < counts[active[j].ID]
		})
		owner := active[0].ID
		follower := active[1].ID
		if err := s.AssignPartition(ctx, topicName, partition, owner, follower); err != nil {
			return err
		}
		counts[owner]++
		counts[follower]++
	}
	return nil
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
