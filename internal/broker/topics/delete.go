package topics

import (
	"context"
	"fmt"
	"path/filepath"
)

// PurgeError reports a DeleteTopic that removed the topic's metadata
// but failed to purge its local state. The metadata delete stands —
// callers should treat the topic as gone and the leftover files as
// reclaimable by the startup orphan sweep.
type PurgeError struct {
	Topic string
	Err   error
}

func (e PurgeError) Error() string {
	return fmt.Sprintf("topics: purge %q after metadata delete: %v", e.Topic, e.Err)
}

func (e PurgeError) Unwrap() error {
	return e.Err
}

// DeleteTopic removes a topic and all of its data: closes cached
// partition logs (each does a final flush), drops in-flight
// reservations, removes the on-disk directory, and wipes the
// metastore record + offsets + schemas. Irreversible.
//
// The request identity must manage the topic as it stands under the
// name lock (audit H1): a delete authorized at the ingress against one
// topic is refused if the name now holds someone else's.
func (m *Manager) DeleteTopic(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	unlock := m.lockTopicName(name)
	defer unlock()
	if err := m.leaderBarrier(ctx); err != nil {
		return err
	}

	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return err
	}
	if err := authorizeManage(ctx, t); err != nil {
		return err
	}

	if err := m.deleteTopicMetadata(ctx, name); err != nil {
		return err
	}
	if err := m.purgeTopicLocked(ctx, name, t.ID); err != nil {
		return PurgeError{Topic: name, Err: err}
	}
	m.logger.Info("topic deleted", "topic", name, "incarnation", t.ID)
	return nil
}

// assignmentLocker is the metastore capability behind the assignment
// lock (implemented by *metastore.Store).
type assignmentLocker interface {
	LockAssignments() (unlock func())
}

// deleteTopicMetadata deletes the topic's record (and with it its
// schemas and assignment rows) under the metastore's assignment lock.
// The controller's placement pass re-reads each topic under that lock
// before writing owners, so holding it here means the pass either
// finishes first (and the delete removes its rows) or sees the topic
// gone; without it a pass could write rows for the deleted topic that a
// later same-named topic inherited (audit M2, L6). The caller holds the
// topic's name lock: the lock order is name lock, then assignment lock,
// everywhere.
func (m *Manager) deleteTopicMetadata(ctx context.Context, name string) error {
	if l, ok := m.metastore.(assignmentLocker); ok {
		unlock := l.LockAssignments()
		defer unlock()
	}
	return m.metastore.DeleteTopic(ctx, name)
}

// PurgeTopic drops all local state of one incarnation of a topic
// (cached logs, in-flight reservations, in-memory schemas, on-disk
// files). Also invoked directly via the cluster purge broadcast on
// non-coordinating nodes.
//
// id names the incarnation being purged (topic.Topic.ID of the deleted
// record). The on-disk directory is removed only if it belongs to that
// incarnation: a directory that a same-named RECREATED topic already
// owns is left alone, and a quarantined copy of the purged incarnation
// is reclaimed. An empty id (a sender that predates incarnation IDs)
// purges by name, as it always did.
func (m *Manager) PurgeTopic(ctx context.Context, name, id string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	unlock := m.lockTopicName(name)
	defer unlock()
	return m.purgeTopicLocked(ctx, name, id)
}

// purgeTopicLocked is PurgeTopic's body; callers must hold the topic's
// name lock. The directory removal itself runs inside runtime.Logs
// under the topic's open guard, so a lazy open of the same topic
// cannot land a log in the directory while it is being unlinked; the
// in-memory state is dropped through the retired hook (see
// NewManager) when, and only when, the directory was this
// incarnation's. Caches that reload on use are dropped either way.
func (m *Manager) purgeTopicLocked(_ context.Context, name, id string) error {
	if _, err := m.topicDir(name); err != nil {
		return err
	}
	// Wake the consumers parked on the topic on THIS node before the
	// purge, and regardless of whether this node holds any of its files:
	// a node that only ever served the topic's long polls has no
	// directory to purge, so the retired hook below would never fire for
	// it and its consumers would sleep out their wait.
	if m.waiters != nil {
		m.waiters.ReleaseTopicWaiters(name)
	}
	_, err := m.logs.PurgeTopic(name, id)
	// Also regardless of the directory: a node that only accepted
	// produces for the topic compiled its schema and cached its metadata
	// all the same.
	m.dropTopicCaches(name)
	return err
}

// dropTopicState drops the in-memory state a topic incarnation left
// behind: in-flight reservations and committed frontiers, loaded
// schemas and the engine's cached metadata. Registered with
// runtime.Logs as the retired hook so it also runs when an open
// quarantines a deleted incarnation's directory, where that state
// would otherwise be resumed by the recreated topic.
func (m *Manager) dropTopicState(name string) {
	if m.waiters != nil {
		// First, so a consumer woken here cannot be handed a record from
		// state that is being torn down under it.
		m.waiters.ReleaseTopicWaiters(name)
	}
	if m.offsets != nil {
		m.offsets.DropTopic(name)
	}
	m.dropTopicCaches(name)
}

// topicCacheForgetter is the messaging engine's ForgetTopic, reached
// through the waiter releaser it is wired as.
type topicCacheForgetter interface {
	ForgetTopic(topicName string)
}

// dropTopicCaches drops what this node only caches about a topic: its
// compiled schemas and the messaging engine's cached record,
// assignments, schema load marker and consume cursor. Without this a
// deleted topic's entries stayed on every node it was used on, one set
// per name ever deleted. All of it reloads from the metastore on next
// use, so dropping it under a live same-named successor only costs a
// reload. The engine forgets after the registry drop: a schema reload
// racing the drop is then redone rather than recorded as loaded.
func (m *Manager) dropTopicCaches(name string) {
	if m.schemas != nil {
		if err := m.schemas.DropTopic(context.Background(), name); err != nil {
			m.logger.Warn("drop topic schemas after retiring or purging it", "topic", name, "err", err)
		}
	}
	if f, ok := m.waiters.(topicCacheForgetter); ok {
		f.ForgetTopic(name)
	}
}

// topicDir resolves the on-disk directory for a topic and verifies —
// defense in depth behind validateTopicName — that it is strictly a
// direct child of dataDir/topics. A crafted name like ".." would
// otherwise resolve to the data dir itself and RemoveAll would wipe it;
// "." would resolve to the topics root and purge every topic.
func (m *Manager) topicDir(name string) (string, error) {
	root := filepath.Join(m.dataDir, "topics")
	dir := filepath.Join(root, name)
	if dir == root || filepath.Dir(dir) != root {
		return "", fmt.Errorf("%w: topic name %q escapes the topics directory", ErrInvalid, name)
	}
	return dir, nil
}
