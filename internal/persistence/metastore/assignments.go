package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrNoAliveMembers is returned by AssignNewPartitions when no member is
// alive to own the new partitions, as on a fresh cluster before any node
// has registered. The partitions stay unassigned and the controller
// places them once members register; the error only makes that visible
// to the create and alter paths, which log it.
var ErrNoAliveMembers = errors.New("metastore: no alive member to own new partitions")

// ErrAllMembersDraining is returned when members are alive but every one
// of them is being decommissioned, so no member may take new partitions:
// placing them on a draining member would override the operator's drain,
// and the decommission would have nowhere to move them. It wraps
// errs.ErrUnavailable (503): a create or partition increase is refused,
// and succeeds once a member that is not draining is alive again.
var ErrAllMembersDraining = fmt.Errorf("%w: every live member is being decommissioned, so no member can take new partitions; wait for restarting members to come back, abort a decommission or add a node",
	errs.ErrUnavailable)

// assignMu orders the read-then-assign sequences that place unassigned
// partitions: AssignNewPartitions (topic create and alter) and the
// controller's reconcile sweep, which takes it through LockAssignments.
// Each reads a topic's assignments and then writes an owner for every
// partition it found unassigned, and AssignPartition replaces whatever
// owner is on record. Unordered, both could place the same partition: a
// create that read the member table before the other members
// registered and a sweep that read it after picked different owners,
// and the later write moved the partition away from an owner that
// might already have committed records to it. One lock per process is
// enough: a process runs one metastore, and only the leader assigns.
var assignMu sync.Mutex

// LockAssignments takes the lock that orders every read-then-assign
// sequence for unassigned partitions (see assignMu) and returns the
// matching unlock. Hold it from reading a topic's assignments until the
// partitions found unassigned have been assigned.
func (s *Store) LockAssignments() (unlock func()) {
	assignMu.Lock()
	return assignMu.Unlock
}

// joinWait bounds how long AssignNewPartitions waits for Raft voters that
// have not registered as members yet (see awaitVotersRegistered).
const joinWait = 2 * time.Second

// joinPollInterval is how often that wait re-reads the member table.
const joinPollInterval = 50 * time.Millisecond

// joinSeen records when AssignNewPartitions first found the cluster still
// forming; the first call that finds it formed clears it. The wait is
// bounded from that moment, not per call, so a voter that never
// registers (a node that never started) costs one bounded wait rather
// than one per create.
var joinSeen struct {
	mu    sync.Mutex
	since time.Time
}

// AssignPartition records ownerID as the single owner of the partition
// through Raft, replacing any previous owner.
func (s *Store) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error {
	return s.apply(ctx, opAssignPartition, Assignment{Topic: topicName, Partition: partition, OwnerID: ownerID})
}

// GetAssignment reads the partition's assignment from the local replica.
// It returns ErrNotFound if the partition is unassigned.
//
// It is gated exactly like ListAssignments: until the replica has
// caught up with the leader once since this process started it answers
// ErrUnavailable, never a possibly stale owner. Every single-partition
// ownership decision (which logs a stats query opens, whether a fan-out
// cursor is this node's to run, where a child commit is routed) reads
// through here, and each of those callers treats the error as
// transient and retries on a later pass.
func (s *Store) GetAssignment(topicName string, partition int) (Assignment, error) {
	if !s.ownershipViewReady() {
		return Assignment{}, fmt.Errorf("%w: metastore replica has not caught up with the leader since start; partition ownership unknown", errs.ErrUnavailable)
	}
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
//
// It refuses (ErrUnavailable) until the replica has caught up with the
// leader once since this process started. Assignments are the only
// input to "do I own this partition", and a freshly restarted node's
// replica lags the cluster until it has heard from the leader and
// applied what the leader had committed: acting on that view, a node
// took ownership of partitions that had been reassigned while it was
// down (a same-named topic recreated in between), committed dispatched
// records into a fresh directory at offset 0, handed them to consumers,
// and later lost them when the correctly gated stale-copy sweep
// reclaimed the directory (seen under SIGKILL chaos). Every caller
// treats the error as transient: the dispatcher leaves the records in
// its WAL, forwarded commits are retried by their sender, and HTTP
// answers 503 until the view is trustworthy, normally within seconds.
func (s *Store) ListAssignments(topicName string) ([]Assignment, error) {
	if !s.ownershipViewReady() {
		return nil, fmt.Errorf("%w: metastore replica has not caught up with the leader since start; partition ownership unknown", errs.ErrUnavailable)
	}
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
//
// With no alive member it assigns nothing and returns ErrNoAliveMembers
// rather than nil: a create that returned success with every partition
// unowned used to leave produces waiting in the ingress WAL and
// consumers getting empty answers, with nothing in the log to say why.
// With every live member draining it assigns nothing and returns
// ErrAllMembersDraining.
//
// While the cluster is still forming it first waits, briefly, for the
// voters that have not registered yet (awaitVotersRegistered), so a
// topic created right after /readyz is spread over every node instead
// of landing whole on the first one to register and then being moved.
//
// It holds the assignment lock from reading the members to the last
// write, so a concurrent controller sweep either finishes first (and
// this call finds those partitions assigned) or waits until this call
// is done.
func (s *Store) AssignNewPartitions(ctx context.Context, topicName string, fromPartition, toPartition int) error {
	if fromPartition >= toPartition {
		return nil
	}
	s.awaitVotersRegistered(ctx)

	unlock := s.LockAssignments()
	defer unlock()

	members, err := s.ListMembers()
	if err != nil {
		return err
	}
	active := PlacementMembers(members)
	if len(active) == 0 {
		return PlacementRefusal(members)
	}
	active = RoundRobinMembers(active)

	parentOwners, parentPartitions, err := s.parentOwnersFor(ctx, topicName)
	if err != nil {
		return err
	}
	// The incarnation the placement is computed for: an insert-only
	// placement is refused for any other.
	var topicID string
	if t, err := s.GetTopic(ctx, topicName); err == nil {
		topicID = t.ID
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
		if err := s.placePartition(ctx, topicName, topicID, partition, owner); err != nil {
			return err
		}
	}
	return nil
}

// placePartition records owner for a partition the caller found without
// one. Once every member applies insert-only placement it never replaces
// an owner placed in between (the partition is placed: nil); until then
// it is a plain AssignPartition.
func (s *Store) placePartition(ctx context.Context, topicName, topicID string, partition int, owner string) error {
	err := s.AssignPartitionIfAbsent(ctx, topicName, partition, owner, topicID)
	switch {
	case errors.Is(err, ErrEntryTypeNotYetUsable):
		return s.AssignPartition(ctx, topicName, partition, owner)
	case errors.Is(err, ErrPartitionAssigned):
		return nil
	}
	return err
}

// AssignPartitionIfAbsent records ownerID as the owner of a partition
// that has none on record, through Raft. It is refused when the
// partition already has an owner (ErrPartitionAssigned: placement never
// replaces one; moves change owners through CompleteMove), when the
// topic is gone (ErrNotFound) or is not incarnation expectID
// (errs.ErrTopicChanged), when the partition is out of range or the
// owner is empty or was removed from the cluster (errs.ErrInvalidArgument).
// While some member does not apply it, it proposes nothing and returns
// ErrEntryTypeNotYetUsable.
func (s *Store) AssignPartitionIfAbsent(ctx context.Context, topicName string, partition int, ownerID, expectID string) error {
	return s.applyIfUsable(ctx, opAssignPartitionIfAbsent, assignIfAbsentPayload{Topic: topicName, Partition: partition, OwnerID: ownerID, ExpectID: expectID})
}

// PruneAssignment deletes the assignment row of a partition that does
// not exist: its topic is gone, or the index is at or beyond the
// topic's partition count. A row that belongs to a partition is refused
// with ErrAssignmentLive, so a prune computed from a lagging read never
// removes an owner; a row already gone answers ErrNotFound. While some
// member does not apply it, it proposes nothing and returns
// ErrEntryTypeNotYetUsable.
func (s *Store) PruneAssignment(ctx context.Context, topicName string, partition int) error {
	return s.applyIfUsable(ctx, opPruneAssignment, pruneAssignmentPayload{Topic: topicName, Partition: partition})
}

// OrphanAssignments lists, from the local replica, the assignment rows
// that belong to no partition: their topic is gone, or the partition
// index is at or beyond the topic's count. Releases before 3.1.0 could
// leave such rows behind a topic delete; the leader prunes them
// (PruneAssignment), which re-checks each row as it applies.
func (s *Store) OrphanAssignments() ([]Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Assignment
	err := s.fsm.view(func(tx *bolt.Tx) error {
		partitions := map[string]int{}
		if err := tx.Bucket(bucketTopics).ForEach(func(k, v []byte) error {
			var t topic.Topic
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			partitions[string(k)] = t.Partitions
			return nil
		}); err != nil {
			return err
		}
		// The key names the row ("<topic>:<partition>"), so only the
		// orphans' values are decoded.
		return tx.Bucket(bucketAssignments).ForEach(func(k, v []byte) error {
			name, partition, ok := splitAssignmentKey(k)
			if !ok {
				return nil
			}
			if n, exists := partitions[name]; exists && partition < n {
				return nil
			}
			a := Assignment{Topic: name, Partition: partition}
			var stored Assignment
			if json.Unmarshal(v, &stored) == nil {
				a.OwnerID = stored.OwnerID
			}
			out = append(out, a)
			return nil
		})
	})
	return out, err
}

// splitAssignmentKey parses an assignment key, "<topic>:<partition>".
// Topic names cannot contain ':', so the last one separates the two.
func splitAssignmentKey(key []byte) (topicName string, partition int, ok bool) {
	i := bytes.LastIndexByte(key, ':')
	if i <= 0 {
		return "", 0, false
	}
	p, err := strconv.Atoi(string(key[i+1:]))
	if err != nil || p < 0 {
		return "", 0, false
	}
	return string(key[:i]), p, true
}

// awaitVotersRegistered waits until every Raft voter has a member record,
// the cluster has been seen forming for joinWait, or ctx ends, whichever
// comes first. It returns at once when the cluster is not forming.
//
// On a fresh cluster the nodes register a few hundred milliseconds
// apart, and /readyz can go green in between. A create placed then put
// every partition on the members registered so far, usually the leader
// alone, and the controller's rebalance then moved most of them to the
// others while producers and consumers were already using them. Waiting
// here is cheaper than those moves: the create returns a little later
// with its partitions already spread.
func (s *Store) awaitVotersRegistered(ctx context.Context) {
	for {
		members, err := s.ListMembers()
		if err != nil {
			return
		}
		joinSeen.mu.Lock()
		if !s.votersJoining(members) {
			joinSeen.since = time.Time{}
			joinSeen.mu.Unlock()
			return
		}
		if joinSeen.since.IsZero() {
			joinSeen.since = time.Now()
		}
		expired := time.Since(joinSeen.since) >= joinWait
		joinSeen.mu.Unlock()
		if expired {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(joinPollInterval):
		}
	}
}

// votersJoining reports whether the cluster is still forming: some Raft
// voter has a member record and some voter has none yet. A voter whose
// record says dead has registered before; it may be gone for good, and
// nothing waits for it. When no voter has registered at all there is
// nothing to wait against either: the members registered so far (if
// any) are used as they are, and with none the caller reports
// ErrNoAliveMembers and the controller places the partitions later.
func (s *Store) votersJoining(members []Member) bool {
	voters, err := s.Voters()
	if err != nil {
		return false
	}
	registered := make(map[string]bool, len(members))
	for _, m := range members {
		registered[m.ID] = true
	}
	anyRegistered, anyMissing := false, false
	for _, id := range voters {
		if registered[id] {
			anyRegistered = true
		} else {
			anyMissing = true
		}
	}
	return anyRegistered && anyMissing
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

// PlacementMembers returns the members that may receive NEW partitions
// (a create, a partition increase, the controller's sweep): the live
// members that are not being decommissioned. A draining member keeps
// serving what it owns, but a partition placed on it would only have to
// be moved off again, and a drain that keeps receiving new partitions
// may never finish.
//
// It never falls back to draining members. When every live member is
// draining it returns none and logs, once per episode and at error
// level, that new partitions have no owner: placing them on a draining
// member would override the operator's drain, and while every live
// member drains the decommission has nowhere to move them either. The
// create and partition-increase paths refuse with ErrAllMembersDraining
// (see PlacementRefusal), and the controller's sweep leaves unassigned
// partitions unassigned until a member that is not draining is alive.
func PlacementMembers(members []Member) []Member {
	alive := AliveMembers(members)
	out := make([]Member, 0, len(alive))
	for _, m := range alive {
		if !m.Draining {
			out = append(out, m)
		}
	}
	if len(out) > 0 || len(alive) == 0 {
		allDraining.Store(false)
		return out
	}
	if !allDraining.Swap(true) {
		ids := make([]string, 0, len(alive))
		for _, m := range alive {
			ids = append(ids, m.ID)
		}
		placementLog().Error("every live member is being decommissioned, so new partitions have no owner: topic creates and partition increases are refused and unassigned partitions stay unassigned until a member that is not draining is alive; wait for restarting members to come back, abort a decommission or add a node",
			"members", ids)
	}
	return nil
}

// PlacementRefusal says why PlacementMembers(members) is empty:
// ErrNoAliveMembers when no member is alive (a cluster still forming,
// whose controller places the partitions once members register), and
// ErrAllMembersDraining when every live member is being decommissioned.
// It returns nil when some member may take new partitions.
func PlacementRefusal(members []Member) error {
	alive := AliveMembers(members)
	if len(alive) == 0 {
		return ErrNoAliveMembers
	}
	for _, m := range alive {
		if !m.Draining {
			return nil
		}
	}
	return ErrAllMembersDraining
}

// CheckPlacement refuses, with ErrAllMembersDraining, new partitions
// that could have no owner because every live member is being
// decommissioned. Leader-only, read from the local replica. The create
// and partition-increase paths call it before they commit anything, so
// the request is refused instead of leaving partitions unowned. No
// alive member at all is not refused here: on a cluster still forming,
// the controller places the partitions once members register.
func (s *Store) CheckPlacement() error {
	members, err := s.ListMembers()
	if err != nil {
		return fmt.Errorf("metastore: list members: %w", err)
	}
	if err := PlacementRefusal(members); errors.Is(err, ErrAllMembersDraining) {
		return err
	}
	return nil
}

// allDraining is set while every live member is draining, so
// PlacementMembers logs that once per episode.
var allDraining atomic.Bool

// placementLogger is the logger PlacementMembers logs on: the logger of
// the process's metastore (set by New), or a discarding one.
var placementLogger atomic.Pointer[slog.Logger]

func placementLog() *slog.Logger {
	if l := placementLogger.Load(); l != nil {
		return l
	}
	return slog.New(slog.DiscardHandler)
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

// SetAssignmentTarget records the node a partition should move to (empty
// clears it). Leader-only; the desired-state write the controller's
// rebalance policy makes. The current owner keeps serving until the flip.
func (s *Store) SetAssignmentTarget(ctx context.Context, topicName string, partition int, targetID string) error {
	return s.apply(ctx, opSetAssignmentTarget, assignmentTargetPayload{Topic: topicName, Partition: partition, TargetID: targetID})
}

// CompleteMove atomically flips ownership to the target, guarded as a
// compare-and-swap: it succeeds only if the current owner still equals
// expectedOwner and the target still equals targetID, or if that flip
// already committed (the retry is answered as done). A caught-up
// destination proposes this. An error does not mean the flip did not
// happen: a refusal the state machine applied (errs.ErrInvalidArgument,
// errs.ErrNotFound) is final, but errs.ErrUnavailable (leadership lost
// mid-commit) or a context error may still commit under the next
// leader, so the caller resolves the outcome with the leader before it
// undoes anything.
func (s *Store) CompleteMove(ctx context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	return s.apply(ctx, opCompleteMove, completeMovePayload{Topic: topicName, Partition: partition, ExpectedOwner: expectedOwner, TargetID: targetID})
}

// AbortMove clears a move target, but only if it still matches
// expectedTarget — a stale abort never clobbers a re-planned target.
func (s *Store) AbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) error {
	return s.apply(ctx, opAbortMove, abortMovePayload{Topic: topicName, Partition: partition, ExpectedTarget: expectedTarget})
}
