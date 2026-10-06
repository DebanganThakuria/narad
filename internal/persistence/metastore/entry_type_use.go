package metastore

// Proposing an entry type newer than 3.0.x. Every proposer of one asks
// EveryMemberKnows first (entry_types.go); while some member does not
// apply the type it proposes nothing and returns
// ErrEntryTypeNotYetUsable, and its caller proposes today's entries
// instead. The first proposal of each type is logged, so an operator
// can tell when the cluster crossed the point a 3.0.x node can no
// longer follow.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrEntryTypeNotYetUsable is returned, with nothing proposed, by a
// proposer of an entry type that some member of the cluster does not
// apply yet. The caller falls back to the entries every member applies.
var ErrEntryTypeNotYetUsable = errors.New("metastore: not every member applies this Raft entry type yet")

// entryTypeNames names the entry types newer than 3.0.x in log lines.
var entryTypeNames = map[opCode]string{
	opCreateTopicWith:         "create topic with schema and link",
	opUpdateTopicIf:           "compare-and-set topic update",
	opDeleteTopicIf:           "compare-and-set topic delete",
	opPutSchemaIf:             "compare-and-set schema version",
	opAttachChildIf:           "compare-and-set fan-out attach",
	opDetachChildIf:           "compare-and-set fan-out detach",
	opAssignPartitionIfAbsent: "insert-only partition placement",
	opPruneAssignment:         "orphan assignment prune",
	opMarkMemberDeadIf:        "dead mark from an observed heartbeat",
}

// heldBackLogInterval spaces the info lines that say why an entry type
// is not used yet: one per type per interval.
const heldBackLogInterval = time.Minute

// entryTypeUse remembers which entry types this node has proposed and
// when it last logged why one was held back. The zero value is ready.
type entryTypeUse struct {
	mu       sync.Mutex
	used     map[opCode]bool
	heldBack map[opCode]time.Time
}

// usable reports, from the local replica, whether op may be proposed:
// nil when every member applies it, else an error wrapping
// ErrEntryTypeNotYetUsable that names the member holding it back. It
// logs, at info, the first time op is usable and, at most once a
// minute, why it is not.
func (s *Store) usable(op opCode) error {
	ok, reason := s.EveryMemberKnows(uint32(op))
	u := &s.entryUse
	u.mu.Lock()
	defer u.mu.Unlock()
	if !ok {
		if now := time.Now(); now.Sub(u.heldBack[op]) >= heldBackLogInterval {
			if u.heldBack == nil {
				u.heldBack = map[opCode]time.Time{}
			}
			u.heldBack[op] = now
			s.log.Info("metastore: not using a new raft entry type yet; proposing the entries every member applies",
				"entry_type", uint32(op), "name", entryTypeNames[op], "reason", reason)
		}
		return fmt.Errorf("%w: raft entry type %d (%s): %s", ErrEntryTypeNotYetUsable, op, entryTypeNames[op], reason)
	}
	if !u.used[op] {
		if u.used == nil {
			u.used = map[opCode]bool{}
		}
		u.used[op] = true
		s.log.Info(fmt.Sprintf("metastore: every member applies raft entry type %d; using %s", op, entryTypeNames[op]),
			"entry_type", uint32(op), "name", entryTypeNames[op])
	}
	return nil
}

// applyIfUsable proposes op when every member applies it, and otherwise
// proposes nothing and returns ErrEntryTypeNotYetUsable.
func (s *Store) applyIfUsable(ctx context.Context, op opCode, payload any) error {
	if err := s.usable(op); err != nil {
		return err
	}
	return s.apply(ctx, op, payload)
}
