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
	"maps"
	"strconv"

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

// remoteLinks is t's remote-linked set as it stands, each stub's name
// to its incarnation: t itself when it is a stub, otherwise t's
// children that are stubs. A child that cannot be read for any reason
// but its absence fails the read: a set read short could let a delete
// pass that must be refused.
func (m *Manager) remoteLinks(ctx context.Context, t topic.Topic) (map[string]string, error) {
	if t.IsRemoteChild() {
		return map[string]string{t.Name: t.ID}, nil
	}
	links := map[string]string{}
	for _, name := range t.Children {
		c, err := m.GetTopic(ctx, name)
		if errors.Is(err, ErrNotFound) || errors.Is(err, errs.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if c.IsRemoteChild() {
			links[name] = c.ID
		}
	}
	return links, nil
}

// refuseRemoteLinked is the raw delete's and detach's guard: a stub,
// or a parent with remote children, goes only through the remote-aware
// delete, which runs the unshipped check first. It runs under the name
// lock after the leader barrier, so an attach that committed before the
// delete took the lock is seen.
func refuseRemoteLinked(name string, links map[string]string) error {
	if len(links) == 0 {
		return nil
	}
	return errs.RemoteChildError(errs.ErrRemoteAwareDeleteRequired,
		"use the remote-aware delete: "+strconv.Quote(name)+" is, or has, a remote child whose records may not be shipped yet")
}

// expectRemoteLinks returns a check that passes only when a topic's
// remote-linked set is exactly the one the caller's unshipped check
// covered: an attach, a delete or a re-attach that applied between the
// check and the delete's lock would otherwise let the delete abandon a
// stub nobody checked.
func expectRemoteLinks(expect map[string]string) func(string, map[string]string) error {
	return func(name string, links map[string]string) error {
		if maps.Equal(links, expect) {
			return nil
		}
		return fmt.Errorf("%w: the remote children of %q changed since the unshipped check; retry", errs.ErrTopicChanged, name)
	}
}

// DeleteRemoteLinkedTopicID is DeleteTopicID for the remote-aware
// delete. expect is the remote-linked set (stub name to incarnation)
// the caller's unshipped check covered, empty for a topic that had
// none; under the name lock, after the leader barrier, the topic's set
// must still be exactly that (errs.ErrTopicChanged otherwise). Holding
// the parent's name lock keeps any attach from landing until the
// delete's entry is proposed.
func (m *Manager) DeleteRemoteLinkedTopicID(ctx context.Context, name string, expect map[string]string) (string, error) {
	return m.deleteTopicID(ctx, name, expectRemoteLinks(expect))
}

// DetachRemoteChild is DetachChild for the remote-aware delete of a
// remote child: under both names' locks, after the leader barrier,
// child must still be a stub of parent with incarnation stubID, the one
// the caller's unshipped check covered (errs.ErrTopicChanged
// otherwise).
func (m *Manager) DetachRemoteChild(ctx context.Context, parent, child, stubID string) error {
	return m.detachChild(ctx, parent, child, func(p, c topic.Topic) error {
		if !c.IsRemoteChild() || c.Parent != parent || c.ID != stubID {
			return fmt.Errorf("%w: remote child %q changed since the unshipped check; retry", errs.ErrTopicChanged, child)
		}
		return nil
	})
}
