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
		c.assignTopic(ctx, t.Name, t.Partitions, active)
	}
}

// assignTopic assigns partitions of topicName that have never been
// assigned. Assignments are sticky: a partition whose owner is currently
// dead is NOT reassigned, because Narad has no follower replication and
// the partition's data lives only on that owner's disk — it must wait for
// the owner to restart.
func (c *Controller) assignTopic(ctx context.Context, topicName string, numPartitions int, active []metastore.Member) {
	if len(active) == 0 {
		return
	}

	existing, _ := c.store.ListAssignments(topicName)
	assigned := make(map[int]bool, len(existing))
	for _, a := range existing {
		assigned[a.Partition] = true
	}

	for p := range numPartitions {
		if assigned[p] {
			continue
		}
		owner, ok := metastore.RoundRobinOwner(active, p)
		if !ok {
			return
		}
		if err := c.store.AssignPartition(ctx, topicName, p, owner); err != nil {
			continue
		}
	}
}
