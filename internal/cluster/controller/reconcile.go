package controller

import (
	"context"
	"errors"
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
	// A just-elected leader's FSM may not have applied the placements
	// the previous leader committed; a partition that looks unassigned
	// here could already have an owner holding records. Barrier first
	// (once per term); on failure skip the pass, the next tick retries.
	if err := c.store.LeaderBarrier(ctx); err != nil {
		return
	}

	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	// New partitions go to live members that are not being
	// decommissioned (all live members only if every one is draining).
	active := metastore.PlacementMembers(members)
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

	for _, t := range topics {
		c.assignTopic(ctx, t, active)
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
//
// The store's assignment lock is held from reading the assignments to
// the last write, so a topic create placing the same partitions through
// AssignNewPartitions cannot interleave with this pass and have one
// placement overwrite the other, and a topic delete (which takes the
// same lock) cannot land in between.
//
// listed is the topic as this tick's list showed it. It is read again
// under the lock: a topic deleted or recreated since the list is skipped
// (rows written for a deleted topic would be inherited by a later
// same-named one), and the partition counts placed by are the ones on
// record now, the topic's own and, for a child, its parent's.
func (c *Controller) assignTopic(ctx context.Context, listed topic.Topic, active []metastore.Member) {
	if len(active) == 0 {
		return
	}
	unlock := c.store.LockAssignments()
	defer unlock()

	t, err := c.store.GetTopic(ctx, listed.Name)
	if err != nil || t.ID != listed.ID {
		// Gone, recreated, or unreadable: the next tick lists it afresh.
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
		// The parent's partition count as it stands under the lock. A
		// parent that is gone (detach/delete race) yields 0, no
		// constraint, matching the create-path behavior.
		parent, err := c.store.GetTopic(ctx, t.Parent)
		switch {
		case err == nil:
			parentPartitions = parent.Partitions
		case !errors.Is(err, metastore.ErrNotFound):
			// Unreadable: placing blind could colocate the copies.
			return
		}
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
