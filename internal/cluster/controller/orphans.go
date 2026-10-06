package controller

// Orphan assignment rows. A release before 3.1.0 could leave assignment
// rows behind a topic delete (a placement pass that listed the topic
// before the delete wrote them after it), or for partitions beyond a
// topic's count. They belong to no partition, but a topic recreated
// under the name used to inherit them, owners and all. The leader prunes
// them once every member applies the prune entry type; until then it
// logs each at error, once. It never moves or reassigns data: a row is
// deleted only when the state machine confirms, as the prune applies,
// that it belongs to no partition.

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// orphanPruneEvery is how many reconcile ticks pass between two orphan
// passes: about a minute at the default interval. The leader also runs
// one when it takes over.
const orphanPruneEvery = 6

// pruneOrphanAssignments deletes the assignment rows that belong to no
// partition. Leader-only, after the term's barrier, so the rows listed
// are not an artefact of a lagging replica (and the state machine
// re-checks each one anyway).
func (c *Controller) pruneOrphanAssignments(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}
	if err := c.store.LeaderBarrier(ctx); err != nil {
		return
	}
	orphans, err := c.store.OrphanAssignments()
	if err != nil {
		c.logger().Warn("controller: could not list orphan assignment rows; the next pass retries", "err", err)
		return
	}
	c.orphanMu.Lock()
	defer c.orphanMu.Unlock()
	logged := make(map[string]bool)
	pruned, usable := 0, true
	for _, a := range orphans {
		if ctx.Err() != nil {
			return
		}
		key := fmt.Sprintf("%s/%d", a.Topic, a.Partition)
		if usable {
			err = c.store.PruneAssignment(ctx, a.Topic, a.Partition)
			usable = !errors.Is(err, metastore.ErrEntryTypeNotYetUsable)
		}
		switch {
		case !usable:
			if !c.orphansLogged[key] {
				c.logger().Error(fmt.Sprintf("orphan assignment row for %s; it is pruned once every member runs 3.1.0", key),
					"topic", a.Topic, "partition", a.Partition, "owner", a.OwnerID)
			}
			logged[key] = true
		case err == nil:
			pruned++
		case errors.Is(err, metastore.ErrAssignmentLive), errors.Is(err, metastore.ErrNotFound):
			// The partition exists again (the topic was recreated or
			// grew), or the row went, since the list.
		default:
			c.logger().Warn("controller: could not prune an orphan assignment row; the next pass retries",
				"topic", a.Topic, "partition", a.Partition, "err", err)
		}
	}
	c.orphansLogged = logged
	if pruned > 0 {
		c.logger().Info("controller: pruned assignment rows that belonged to no partition", "rows", pruned)
	}
}
