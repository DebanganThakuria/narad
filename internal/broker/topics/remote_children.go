package topics

// Remote children through the topic manager. A remote child's attach
// takes the same name locks, leader barrier and authority re-check as a
// local attach before its one Raft entry; its detach and its stub's
// delete are the ordinary DetachChild and DeleteTopicID, which know a
// stub has no owner and is managed through its parent.

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/security"
)

// remoteChildStore is the metastore capability behind a remote child's
// attach (implemented by *metastore.Store). It proposes nothing and
// returns metastore.ErrEntryTypeNotYetUsable until every member applies
// the remote entry types; there is no older entry to fall back to.
type remoteChildStore interface {
	AttachRemoteChild(ctx context.Context, op metastore.AttachRemoteChildOp) error
}

// AttachRemoteChild creates op.Stub as a remote child of op.Parent. The
// leader ran the remote checks against the parent as incarnation
// op.ParentID; under both names' locks and after the leader barrier the
// parent must still be that incarnation (errs.ErrTopicChanged
// otherwise), and the request identity must be an admin, since the
// link lends the remote's credential. The FSM checks every fan-out and
// remote invariant in the entry's own transaction.
func (m *Manager) AttachRemoteChild(ctx context.Context, op metastore.AttachRemoteChildOp) error {
	if err := validateTopicName(op.Parent); err != nil {
		return err
	}
	if err := validateTopicName(op.Stub); err != nil {
		return err
	}
	if op.Parent == op.Stub {
		return fmt.Errorf("%w: a topic cannot be attached to itself", ErrInvalid)
	}
	if op.ParentID == "" {
		return fmt.Errorf("%w: the parent incarnation the checks ran against is required", ErrInvalid)
	}
	s, ok := m.metastore.(remoteChildStore)
	if !ok {
		return fmt.Errorf("%w: this metastore does not hold remote children", ErrInvalid)
	}
	unlock := m.lockTopicNames(op.Parent, op.Stub)
	defer unlock()
	if err := m.leaderBarrier(ctx); err != nil {
		return err
	}
	p, err := m.getExistingTopic(ctx, op.Parent)
	if err != nil {
		return err
	}
	if p.ID != op.ParentID {
		return fmt.Errorf("%w: parent %q was replaced since the remote checks ran; retry", errs.ErrTopicChanged, op.Parent)
	}
	if err := authorizeAdmin(ctx, "attach a remote child"); err != nil {
		return err
	}
	err = s.AttachRemoteChild(ctx, op)
	if errors.Is(err, errs.ErrNotFound) {
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	if err != nil {
		return err
	}
	m.logger.Info("remote child attached", "parent", op.Parent, "child", op.Stub,
		"remote", op.Remote.Name, "remote_topic", op.Remote.Topic)
	return nil
}

// authorizeAdmin refuses a request identity that is not an admin. No
// identity (security off, or an internal call) passes, as in
// authorizeManage.
func authorizeAdmin(ctx context.Context, what string) error {
	id, ok := security.IdentityFrom(ctx)
	if !ok || id.IsAdmin() {
		return nil
	}
	return &ForbiddenError{Username: id.Username, Msg: "only an admin may " + what}
}

// authorizeManageTopic is authorizeManage for a topic that may be a
// remote child's stub: a stub has no owner and is managed through its
// parent, by the parent's owner or an admin. A stub whose parent cannot
// be read is judged on its own record, which only an admin passes.
func (m *Manager) authorizeManageTopic(ctx context.Context, t topic.Topic) error {
	if !t.IsRemoteChild() {
		return authorizeManage(ctx, t)
	}
	if p, err := m.GetTopic(ctx, t.Parent); err == nil {
		return authorizeManage(ctx, p)
	}
	return authorizeManage(ctx, t)
}
