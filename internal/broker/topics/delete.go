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
func (m *Manager) DeleteTopic(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	unlock := m.lockTopicName(name)
	defer unlock()

	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return err
	}

	if err := m.metastore.DeleteTopic(ctx, name); err != nil {
		return err
	}
	if err := m.purgeTopicLocked(ctx, name, t.ID); err != nil {
		return PurgeError{Topic: name, Err: err}
	}
	m.logger.Info("topic deleted", "topic", name, "incarnation", t.ID)
	return nil
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
// incarnation's.
func (m *Manager) purgeTopicLocked(_ context.Context, name, id string) error {
	if _, err := m.topicDir(name); err != nil {
		return err
	}
	_, err := m.logs.PurgeTopic(name, id)
	return err
}

// dropTopicState drops the in-memory state a topic incarnation left
// behind: in-flight reservations and committed frontiers, and loaded
// schemas. Registered with runtime.Logs as the retired hook so it also
// runs when an open quarantines a deleted incarnation's directory,
// where that state would otherwise be resumed by the recreated topic.
func (m *Manager) dropTopicState(name string) {
	if m.offsets != nil {
		m.offsets.DropTopic(name)
	}
	if m.schemas != nil {
		if err := m.schemas.DropTopic(context.Background(), name); err != nil {
			m.logger.Warn("drop topic schemas after retiring incarnation", "topic", name, "err", err)
		}
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
