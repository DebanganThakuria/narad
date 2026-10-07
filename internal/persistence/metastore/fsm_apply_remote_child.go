package metastore

// Remote child handlers. A remote child is a stub topic record (zero
// partitions, role child, a Remote link) whose fan-out cursors send the
// parent's records to a topic on another cluster. Attach creates the
// stub and links it in one transaction, so every invariant (remote
// exists, name free, caps, retention floor, one link per remote topic)
// is checked against the same state it commits to. The state op changes
// only the fields it names, and only on the attachment it was proposed
// against. Like every apply* handler these run on every node and must
// stay deterministic.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// AttachRemoteChildOp is the body of opAttachRemoteChild. The proposer
// mints StubID and Epoch and stamps CreatedAt (Unix seconds), so the
// apply stays deterministic. Offsets is the start offset per parent
// partition, resolved by the proposer in the link's From mode.
//
// ParentID is the parent's incarnation as the leader read and checked it
// (the leader always sets it): the attach applies only to that
// incarnation, like opAttachChildIf, so a parent deleted and recreated
// in between is refused with errs.ErrTopicChanged.
type AttachRemoteChildOp struct {
	Parent    string           `json:"parent"`
	ParentID  string           `json:"parent_id,omitempty"`
	Stub      string           `json:"stub"`
	StubID    string           `json:"stub_id"`
	Epoch     string           `json:"epoch"`
	DelayMs   int64            `json:"delay_ms,omitempty"`
	Offsets   []int64          `json:"offsets,omitempty"`
	Remote    topic.RemoteLink `json:"remote"`
	CreatedAt int64            `json:"created_at"`
	Actor     string           `json:"actor,omitempty"`
	RequestID string           `json:"request_id,omitempty"`
}

// RemoteChildStateOp is the body of opSetRemoteChildState: a
// field-scoped change of one stub's link, applied only while the stub's
// attach epoch is still Epoch. A nil field is left alone.
type RemoteChildStateOp struct {
	Parent    string            `json:"parent"`
	Stub      string            `json:"stub"`
	Epoch     string            `json:"epoch"`
	Pause     *RemotePauseState `json:"pause,omitempty"`
	TargetID  *string           `json:"target_id,omitempty"`
	Lanes     *int              `json:"lanes,omitempty"`
	Skip      *RemoteSkip       `json:"skip,omitempty"`
	Actor     string            `json:"actor,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

// RemotePauseState pauses (Paused) or resumes a link. Reason, By and
// AtMs are recorded on a pause and cleared on a resume.
type RemotePauseState struct {
	Paused bool   `json:"paused"`
	Reason string `json:"reason,omitempty"`
	By     string `json:"by,omitempty"`
	AtMs   int64  `json:"at_ms,omitempty"`
}

// RemoteSkip records the one offset of a parent partition an admin
// accepts to lose.
type RemoteSkip struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (f *fsmState) applyAttachRemoteChild(data []byte) error {
	var op AttachRemoteChildOp
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		if err := checkAttachRemoteChildOp(op); err != nil {
			return err
		}
		parent, err := getTopicRecord(tx, op.Parent)
		if err != nil {
			return err
		}
		// An empty ParentID is a parent created before topic IDs
		// existed, and matches only a record that still has none.
		if err := expectIncarnation(parent, op.ParentID); err != nil {
			return err
		}
		if parent.IsChild() {
			return fmt.Errorf("%w: %q is a child of %q and cannot become a parent",
				errs.ErrFanoutRoleConflict, op.Parent, parent.Parent)
		}
		if ok, err := remoteExists(tx, op.Remote.Name); err != nil {
			return err
		} else if !ok {
			return errs.RemoteChildError(errs.ErrRemoteChildConflict,
				"remote "+strconv.Quote(op.Remote.Name)+" does not exist (deleted meanwhile?)")
		}
		if tx.Bucket(bucketTopics).Get([]byte(op.Stub)) != nil {
			return fmt.Errorf("%w: topic %q", ErrAlreadyExists, op.Stub)
		}
		if len(parent.Children) >= topic.MaxChildrenPerParent {
			return fmt.Errorf("%w: %q already has %d children",
				errs.ErrFanoutChildLimit, op.Parent, len(parent.Children))
		}
		remotes, err := countRemoteChildren(tx, parent)
		if err != nil {
			return err
		}
		if remotes >= topic.MaxRemoteChildrenPerParent {
			return errs.RemoteChildError(errs.ErrRemoteChildLimit, fmt.Sprintf(
				"parent %q already has %d remote children (max %d)", op.Parent, remotes, topic.MaxRemoteChildrenPerParent))
		}
		if err := checkRemoteSourceRetention(parent.RetentionMs, op.Parent); err != nil {
			return err
		}
		if err := checkDelayAgainstRetention(op.DelayMs, parent.RetentionMs, op.Parent); err != nil {
			return err
		}
		if linked, err := stubLinkingTarget(tx, op.Remote.Name, op.Remote.Topic); err != nil {
			return err
		} else if linked != "" {
			return errs.RemoteChildError(errs.ErrRemoteTargetLinked, fmt.Sprintf(
				"this cluster already links to %s/%s through %q", op.Remote.Name, op.Remote.Topic, linked))
		}

		link := op.Remote
		link.Lanes = link.EffectiveLanes()
		if link.From == "" {
			link.From = topic.RemoteFromAttach
		}
		link.Paused, link.PauseReason, link.PausedBy, link.PausedAtMs, link.Skip = false, "", "", 0, nil
		stub := topic.Topic{
			Name:          op.Stub,
			ID:            op.StubID,
			Partitions:    0,
			CreatedAt:     op.CreatedAt,
			Role:          topic.RoleChild,
			Parent:        op.Parent,
			AttachEpoch:   op.Epoch,
			FanoutDelayMs: op.DelayMs,
			AttachOffsets: op.Offsets,
			Remote:        &link,
		}
		parent.Role = topic.RoleParent
		parent.Children = append(parent.Children, op.Stub)
		if err := putTopicRecord(tx, parent); err != nil {
			return err
		}
		return putTopicRecord(tx, stub)
	})
	if err == nil {
		f.versions.bumpTopic(op.Parent)
		f.versions.bumpTopic(op.Stub)
	}
	return err
}

// checkAttachRemoteChildOp checks the op's own fields. The ingress and
// the leader validated them already; the FSM re-checks what a malformed
// proposal could otherwise store.
func checkAttachRemoteChildOp(op AttachRemoteChildOp) error {
	switch {
	case op.Parent == "" || op.Stub == "" || op.Remote.Name == "" || op.Remote.Topic == "":
		return fmt.Errorf("%w: parent, stub, remote and remote topic are required", errs.ErrInvalidArgument)
	case op.Parent == op.Stub:
		return fmt.Errorf("%w: a topic cannot be its own child", errs.ErrFanoutRoleConflict)
	case op.Epoch == "":
		return fmt.Errorf("%w: attach epoch required", errs.ErrInvalidArgument)
	case op.DelayMs < 0:
		return fmt.Errorf("%w: delay_ms must be >= 0", errs.ErrInvalidArgument)
	case op.DelayMs > topic.MaxFanoutDelayMs:
		return fmt.Errorf("%w: delay_ms (%d) exceeds the maximum of %d (1 year)",
			errs.ErrFanoutDelayTooLong, op.DelayMs, topic.MaxFanoutDelayMs)
	case op.Remote.Lanes < 0 || op.Remote.Lanes > topic.MaxRemoteLanes:
		return fmt.Errorf("%w: lanes must be between %d and %d", errs.ErrInvalidArgument, topic.MinRemoteLanes, topic.MaxRemoteLanes)
	case !topic.ValidRemoteFrom(op.Remote.From):
		return fmt.Errorf("%w: from must be attach, unconsumed or earliest", errs.ErrInvalidArgument)
	}
	return nil
}

// checkRemoteSourceRetention enforces the retention floor of a parent
// with remote children: 0 (keep forever) or at least
// topic.MinRemoteSourceRetentionMs.
func checkRemoteSourceRetention(retentionMs int64, parentName string) error {
	if retentionMs == 0 || retentionMs >= topic.MinRemoteSourceRetentionMs {
		return nil
	}
	return errs.RemoteChildError(errs.ErrRemoteRetentionFloor, fmt.Sprintf(
		"parent %q retention (%dms) must be at least %dms (24h) while it has remote children",
		parentName, retentionMs, topic.MinRemoteSourceRetentionMs))
}

// countRemoteChildren counts parent's children that are stubs.
func countRemoteChildren(tx *bolt.Tx, parent topic.Topic) (int, error) {
	n := 0
	for _, name := range parent.Children {
		child, err := getTopicRecord(tx, name)
		if err != nil {
			continue // a broken link is not a remote child
		}
		if child.IsRemoteChild() {
			n++
		}
	}
	return n, nil
}

// stubLinkingTarget returns the stub, if any, that already links this
// cluster to (remoteName, remoteTopic).
func stubLinkingTarget(tx *bolt.Tx, remoteName, remoteTopic string) (string, error) {
	var found string
	err := forEachStub(tx, func(stub topic.Topic) bool {
		if stub.Remote.Name == remoteName && stub.Remote.Topic == remoteTopic {
			found = stub.Name
			return false
		}
		return true
	})
	return found, err
}

// remoteChildrenNaming returns every stub that names remoteName, as
// "parent/child" pairs in name order: a remote delete refuses while any
// exists.
func remoteChildrenNaming(tx *bolt.Tx, remoteName string) ([]string, error) {
	var out []string
	err := forEachStub(tx, func(stub topic.Topic) bool {
		if stub.Remote.Name == remoteName {
			out = append(out, stub.Parent+"/"+stub.Name)
		}
		return true
	})
	return out, err
}

// forEachStub calls fn for every remote child's stub until fn returns
// false. The scan decodes only records that carry a remote link.
func forEachStub(tx *bolt.Tx, fn func(topic.Topic) bool) error {
	c := tx.Bucket(bucketTopics).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if !mayBeStub(v) {
			continue
		}
		var t topic.Topic
		if err := json.Unmarshal(v, &t); err != nil {
			return err
		}
		if !t.IsRemoteChild() {
			continue
		}
		if !fn(t) {
			return nil
		}
	}
	return nil
}

// stubMarker is the JSON key only a stub's record carries.
var stubMarker = []byte(`"remote":{`)

// mayBeStub is a cheap pre-filter: a record without the remote key is
// not a stub, so the scan need not decode it.
func mayBeStub(raw []byte) bool { return bytes.Contains(raw, stubMarker) }

func (f *fsmState) applySetRemoteChildState(data []byte) error {
	var op RemoteChildStateOp
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		stub, err := getTopicRecord(tx, op.Stub)
		if err != nil {
			return err
		}
		if !stub.IsRemoteChild() || stub.Parent != op.Parent {
			return fmt.Errorf("%w: %q is not a remote child of %q", ErrNotFound, op.Stub, op.Parent)
		}
		if stub.AttachEpoch != op.Epoch {
			return errs.RemoteChildError(errs.ErrRemoteChildStale, fmt.Sprintf(
				"remote child %q was attached again since this change was proposed; retry", op.Stub))
		}
		link := *stub.Remote
		if op.Pause != nil {
			if op.Pause.Paused {
				if err := topic.ValidateRemotePauseReason(op.Pause.Reason); err != nil {
					return fmt.Errorf("%w: %v", errs.ErrInvalidArgument, err)
				}
				link.Paused, link.PauseReason, link.PausedBy, link.PausedAtMs = true, op.Pause.Reason, op.Pause.By, op.Pause.AtMs
			} else {
				link.Paused, link.PauseReason, link.PausedBy, link.PausedAtMs = false, "", "", 0
			}
		}
		if op.TargetID != nil {
			link.TargetID = *op.TargetID
		}
		if op.Lanes != nil {
			if *op.Lanes < topic.MinRemoteLanes || *op.Lanes > topic.MaxRemoteLanes {
				return fmt.Errorf("%w: lanes must be between %d and %d", errs.ErrInvalidArgument, topic.MinRemoteLanes, topic.MaxRemoteLanes)
			}
			link.Lanes = *op.Lanes
		}
		if op.Skip != nil {
			if op.Skip.Partition < 0 || op.Skip.Offset < 0 {
				return fmt.Errorf("%w: skip needs a partition and an offset >= 0", errs.ErrInvalidArgument)
			}
			link.Skip = topic.WithSkip(link.Skip, op.Skip.Partition, op.Skip.Offset)
		}
		stub.Remote = &link
		return putTopicRecord(tx, stub)
	})
	if err == nil {
		f.versions.bumpTopic(op.Stub)
	}
	return err
}
