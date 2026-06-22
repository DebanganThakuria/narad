package controller

import (
	"context"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// reconcileAssignments assigns any partitions that have no owner. It never
// moves existing assignments — without replication, data lives only on the
// current owner's disk.
func (c *Controller) reconcileAssignments(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}

	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	active := metastore.AliveMembers(members)
	if len(active) == 0 {
		return
	}

	topics, _, err := c.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return
	}

	active = metastore.RoundRobinMembers(active)

	for _, t := range topics {
		c.assignTopic(ctx, t.Name, t.Partitions, t.ReplicationFactor, active)
	}
}

// assignTopic assigns partitions of topicName that are missing an alive owner.
func (c *Controller) assignTopic(ctx context.Context, topicName string, numPartitions int, replicationFactor int, active []metastore.Member) {
	if replicationFactor < 2 || len(active) < replicationFactor {
		return
	}

	existing, _ := c.store.ListAssignments(topicName)
	assigned := make(map[int]bool, len(existing))
	for _, a := range existing {
		owner, err := c.store.GetMember(a.OwnerID)
		if err == nil && owner.Status == metastore.MemberAlive {
			assigned[a.Partition] = true
			continue
		}
		if a.FollowerID == "" {
			continue
		}
		follower, err := c.store.GetMember(a.FollowerID)
		if err != nil || follower.Status != metastore.MemberAlive {
			continue
		}
		if err := c.store.AssignPartition(ctx, topicName, a.Partition, a.FollowerID, a.OwnerID); err != nil {
			continue
		}
		assigned[a.Partition] = true
	}

	for p := range numPartitions {
		if assigned[p] {
			continue
		}
		owner, follower, ok := metastore.RoundRobinReplicaPair(active, p)
		if !ok {
			return
		}
		if err := c.store.AssignPartition(ctx, topicName, p, owner, follower); err != nil {
			continue
		}
	}
}
