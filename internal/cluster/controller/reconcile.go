package controller

import (
	"context"
	"sort"

	"github.com/debanganthakuria/narad/internal/domain/topic"

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

	// Parents (and standalone topics) before children: a child's
	// anti-affine placement needs its parent's same-index owner on
	// record, and ListTopics is name-ordered, which can put "a-child"
	// ahead of "z-parent".
	sort.SliceStable(topics, func(i, j int) bool {
		return topics[i].Parent == "" && topics[j].Parent != ""
	})

	partitionCounts := make(map[string]int, len(topics))
	for _, t := range topics {
		partitionCounts[t.Name] = t.Partitions
	}

	for _, t := range topics {
		c.assignTopic(ctx, t, active, partitionCounts)
	}
}

// assignTopic assigns partitions of t that have never been assigned.
// Assignments are sticky: a partition whose owner is currently dead is
// NOT reassigned, because Narad has no follower replication and the
// partition's data lives only on that owner's disk — it must wait for
// the owner to restart. A fan-out child gets anti-affine placement
// against its parent's same-index owners (the replica pattern); child
// partitions whose parent counterpart is still unassigned are deferred
// to the next tick.
func (c *Controller) assignTopic(ctx context.Context, t topic.Topic, active []metastore.Member, partitionCounts map[string]int) {
	if len(active) == 0 {
		return
	}

	var parentOwners map[int]string
	var parentPartitions int
	if t.Parent != "" {
		parentAssignments, err := c.store.ListAssignments(t.Parent)
		if err != nil {
			// Can't see the parent's owners: assigning the child now
			// could colocate the copies. Next tick retries.
			return
		}
		parentOwners = make(map[int]string, len(parentAssignments))
		for _, a := range parentAssignments {
			parentOwners[a.Partition] = a.OwnerID
		}
		// The true partition count comes from this tick's topic list. A
		// parent absent from it (detach/delete race) yields 0 — no
		// constraint — matching the create-path behavior.
		parentPartitions = partitionCounts[t.Parent]
	}

	existing, err := c.store.ListAssignments(t.Name)
	if err != nil {
		// A transient read failure must not make every partition look
		// unassigned: round-robin could then hand a partition whose data
		// lives on its current owner's disk to a different member. Skip
		// this topic; the next reconcile tick retries.
		return
	}
	assigned := make(map[int]bool, len(existing))
	for _, a := range existing {
		assigned[a.Partition] = true
	}

	for p := range t.Partitions {
		if assigned[p] {
			continue
		}
		owner, ok, deferred := metastore.ChildAwareOwner(active, p, parentOwners, parentPartitions)
		if deferred {
			continue
		}
		if !ok {
			return
		}
		if err := c.store.AssignPartition(ctx, t.Name, p, owner); err != nil {
			continue
		}
	}
}
