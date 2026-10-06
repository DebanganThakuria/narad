package metastore

// This file defines the Raft log command encoding. The wire format is
// frozen: changing the opCode values, JSON tags, or payload shapes would
// break replay of existing Raft logs.

import (
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
)

type opCode byte

const (
	opCreateTopic opCode = iota + 1
	opUpdateTopic
	opDeleteTopic
	opPutSchema
	opAssignPartition
	opMemberJoin
	opMemberHeartbeat
	opMemberDead
	opCreateUser
	opUpdateUser
	opDeleteUser
	opSeedRootUser
	opAttachChild
	opDetachChild
	opSetAssignmentTarget
	opCompleteMove
	opAbortMove
	opSetMemberDraining
	opRemoveMember
	opReadmitMember
	opSetUserPassword
	opSetUserGrants

	// The entry types below are newer than every 3.0.x release. The
	// leader proposes one only once every member reports a release that
	// applies it (EveryMemberKnows), and proposes the entries above
	// until then. Their numbers are part of the log format: never reorder
	// them. See fsm_apply_create.go, fsm_apply_cas.go and
	// fsm_apply_assign.go.

	// opCreateTopicWith creates a topic, its first schema version and its
	// fan-out parent link in one transaction.
	opCreateTopicWith
	// opUpdateTopicIf is a topic config update that applies only to the
	// incarnation it was read from and never shrinks the partitions.
	opUpdateTopicIf
	// opDeleteTopicIf deletes a topic only if it is the incarnation the
	// delete was checked against.
	opDeleteTopicIf
	// opPutSchemaIf appends a schema version only to the incarnation it
	// was checked against, within the schema byte budgets.
	opPutSchemaIf
	// opAttachChildIf links a fan-out child only if both topics are the
	// incarnations it was checked against, comparing schema histories by
	// JSON value.
	opAttachChildIf
	// opDetachChildIf unlinks a fan-out child only if both topics are
	// the incarnations it was checked against.
	opDetachChildIf
	// opAssignPartitionIfAbsent places a partition that has no owner on
	// record; it never replaces one.
	opAssignPartitionIfAbsent
	// opPruneAssignment deletes an assignment row that belongs to no
	// partition: its topic is gone or the partition is out of range.
	opPruneAssignment
	// opMarkMemberDeadIf marks a member dead unless a heartbeat newer
	// than the one the decision was made from is on record.
	opMarkMemberDeadIf
	// opDeleteUserReleaseTopics deletes a user and clears the owner of
	// every topic it owned, in one transaction (fsm_apply_users.go).
	// Proposed only once every member applies it (Store.DeleteUser).
	opDeleteUserReleaseTopics

	// opEnd is not an entry type: it marks the end of the list. New
	// entry types go above this line, and a leader proposes one only
	// once every member reports a release that knows it, so an older
	// node never meets an entry it cannot apply. One that does stops
	// applying (see Apply).
	opEnd
)

// MaxEntryType is the newest Raft entry type this release applies. An
// entry above it was proposed by a newer release; Apply stops on it
// instead of skipping it, and a database or snapshot that records one
// is refused.
const MaxEntryType = uint32(opEnd - 1)

// legacyMaxEntryType is the newest entry type every v3.0.x release
// applies, and so the set a member that reports nothing is assumed to
// know. Frozen: it never moves when a type is added.
const legacyMaxEntryType = uint32(opSetUserGrants)

// memberRemovalPayload is the body of opRemoveMember and opReadmitMember.
// At is a Unix timestamp (seconds) supplied by the proposer so Apply
// stays deterministic; it is recorded on the tombstone.
type memberRemovalPayload struct {
	ID string `json:"id"`
	At int64  `json:"at"`
}

// userPasswordPayload is the body of opSetUserPassword: replace only the
// stored password hash of an existing user. Field-scoped so a password
// change proposed from a lagging replica carries nothing else (see
// fsm_apply_users.go).
type userPasswordPayload struct {
	Username     string `json:"u"`
	PasswordHash []byte `json:"h"`
	UpdatedAtMs  int64  `json:"t"`
}

// userDeletePayload is the body of opDeleteUserReleaseTopics.
type userDeletePayload struct {
	Username string `json:"u"`
}

// userGrantsPayload is the body of opSetUserGrants: replace only the
// grants of an existing user.
type userGrantsPayload struct {
	Username    string       `json:"u"`
	Grants      []user.Grant `json:"g"`
	UpdatedAtMs int64        `json:"t"`
}

// memberDrainingPayload marks a member as draining (or clears it). A
// draining member keeps serving but is excluded from receiving new
// partitions, so the rebalance planner sheds everything it owns onto the
// other live nodes ahead of decommission.
type memberDrainingPayload struct {
	ID       string `json:"id"`
	Draining bool   `json:"draining"`
}

// assignmentTargetPayload sets (or clears, when TargetID=="") the move
// target on an existing partition assignment.
type assignmentTargetPayload struct {
	Topic     string `json:"t"`
	Partition int    `json:"p"`
	TargetID  string `json:"g"`
}

// completeMovePayload is the atomic ownership flip a caught-up
// destination proposes: set OwnerID=TargetID and clear the target, but
// ONLY if the current owner still matches ExpectedOwner and the target
// still matches TargetID (compare-and-swap through Raft).
type completeMovePayload struct {
	Topic         string `json:"t"`
	Partition     int    `json:"p"`
	ExpectedOwner string `json:"e"`
	TargetID      string `json:"g"`
}

// abortMovePayload clears a move target, but only if it still matches
// ExpectedTarget — so aborting a stale move never clobbers a target a
// re-plan just set.
type abortMovePayload struct {
	Topic          string `json:"t"`
	Partition      int    `json:"p"`
	ExpectedTarget string `json:"g"`
}

// cmd is the envelope written to the Raft log.
type cmd struct {
	Op   opCode `json:"o"`
	Data []byte `json:"d"`
}

// schemaPayload is the body of an opPutSchema command.
type schemaPayload struct {
	Topic   string `json:"t"`
	Version int    `json:"v"`
	Schema  []byte `json:"s"`
}

// childLinkPayload is the body of opAttachChild and opDetachChild.
// Epoch (attach only) is generated by the proposer so the FSM stays
// deterministic; it becomes the child's AttachEpoch. DelayMs (attach
// only) makes the child a delay child. Offsets (attach only) is the
// parent's per-partition committed high watermark the proposer
// observed while processing the attach; it becomes the child's
// AttachOffsets, the exact point fan-out starts from. Nil when the
// proposer had no way to observe it (older nodes, or no fan-out runner
// registered), which leaves cursors on the tail-anchor behaviour.
type childLinkPayload struct {
	Parent  string  `json:"p"`
	Child   string  `json:"c"`
	Epoch   string  `json:"e,omitempty"`
	DelayMs int64   `json:"d,omitempty"`
	Offsets []int64 `json:"o,omitempty"`
}

// heartbeatPayload is the body of an opMemberHeartbeat command.
// At is a Unix timestamp (seconds) — passed in by the caller so Apply
// stays deterministic.
type heartbeatPayload struct {
	ID string `json:"id"`
	At int64  `json:"at"`
}

// createTopicWithPayload is the body of opCreateTopicWith: the topic
// record, its first schema version (optional, already validated and
// compacted by the proposer) and its fan-out parent link (optional).
type createTopicWithPayload struct {
	Topic  topic.Topic        `json:"t"`
	Schema []byte             `json:"s,omitempty"`
	Link   *createLinkPayload `json:"l,omitempty"`
}

// createLinkPayload is the fan-out link of a create-as-child: what an
// attach carries, plus the parent incarnation the create was checked
// against.
type createLinkPayload struct {
	Parent   string  `json:"p"`
	ParentID string  `json:"pi"`
	Epoch    string  `json:"e,omitempty"`
	DelayMs  int64   `json:"d,omitempty"`
	Offsets  []int64 `json:"o,omitempty"`
}

// updateTopicIfPayload is the body of opUpdateTopicIf: the proposed
// record and the incarnation it was read from.
type updateTopicIfPayload struct {
	Topic    topic.Topic `json:"t"`
	ExpectID string      `json:"x"`
}

// deleteTopicIfPayload is the body of opDeleteTopicIf.
type deleteTopicIfPayload struct {
	Name     string `json:"n"`
	ExpectID string `json:"x"`
}

// putSchemaIfPayload is the body of opPutSchemaIf.
type putSchemaIfPayload struct {
	Topic    string `json:"t"`
	Version  int    `json:"v"`
	Schema   []byte `json:"s"`
	ExpectID string `json:"x"`
}

// attachChildIfPayload is the body of opAttachChildIf: the link, and
// the parent and child incarnations it was checked against.
type attachChildIfPayload struct {
	Link     childLinkPayload `json:"l"`
	ParentID string           `json:"pi"`
	ChildID  string           `json:"ci"`
}

// detachChildIfPayload is the body of opDetachChildIf.
type detachChildIfPayload struct {
	Parent   string `json:"p"`
	Child    string `json:"c"`
	ParentID string `json:"pi"`
	ChildID  string `json:"ci"`
}

// assignIfAbsentPayload is the body of opAssignPartitionIfAbsent:
// ExpectID is the topic incarnation the placement was computed for.
type assignIfAbsentPayload struct {
	Topic     string `json:"t"`
	Partition int    `json:"p"`
	OwnerID   string `json:"o"`
	ExpectID  string `json:"x"`
}

// pruneAssignmentPayload is the body of opPruneAssignment.
type pruneAssignmentPayload struct {
	Topic     string `json:"t"`
	Partition int    `json:"p"`
}

// markMemberDeadIfPayload is the body of opMarkMemberDeadIf: Observed
// is the member's LastHeartbeat (Unix seconds) the decision was made
// from.
type markMemberDeadIfPayload struct {
	ID       string `json:"id"`
	Observed int64  `json:"h"`
}
