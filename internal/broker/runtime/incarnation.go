package runtime

// Topic incarnations on disk. Partition directories are keyed by topic
// NAME, and a name outlives the topic: delete "orders", recreate
// "orders", and the new topic's partition logs open exactly where the
// old one's segments, high-watermark and consumer offset still sit on
// any node that missed the purge (it was down, or the purge lost the
// race with the recreate and was skipped because the name existed
// again). Served as-is, the recreated topic hands consumers the deleted
// topic's messages and appends new produce after them.
//
// Every topic incarnation has an ID (topic.Topic.ID) and every topic
// directory a marker naming the incarnation it belongs to
// (storage.IncarnationMarkerFileName). The rules, all applied here:
//
//   - opening a partition log stamps an unmarked directory with the
//     current ID (adoption, the upgrade path) and refuses a directory
//     whose marker names another ID: that directory is a deleted
//     incarnation's leftover and is quarantined (renamed to
//     topics/<name>.stale-<oldid>), never served, then a fresh directory
//     is opened for the current ID;
//   - a purge acts on the incarnation it was told to purge: it removes
//     the directory whose marker carries that ID (or its quarantine)
//     and leaves a directory that belongs to a different, live
//     incarnation alone;
//   - a purge holds the topic's guard from closing the open logs
//     through unlinking the directory, so an open of the same topic
//     waits and then sees the directory gone.
//
// A record without an ID (created before IDs existed) keeps the old
// name-based behaviour: nothing is stamped, nothing is quarantined.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ErrStaleTopicIncarnation reports that a topic directory belongs to an
// incarnation other than the one the metastore records for the name.
var ErrStaleTopicIncarnation = errors.New("topic directory belongs to a deleted incarnation of the topic")

// topicGuard is a refcounted per-topic mutex (see Logs.guards).
type topicGuard struct {
	mu   sync.Mutex
	refs int
}

// lockTopic acquires the guard for topicName; the returned func
// releases it. Idle guards are deleted so topic churn does not grow the
// map.
func (g *Logs) lockTopic(topicName string) (unlock func()) {
	g.guardMu.Lock()
	tg := g.guards[topicName]
	if tg == nil {
		tg = &topicGuard{}
		g.guards[topicName] = tg
	}
	tg.refs++
	g.guardMu.Unlock()

	tg.mu.Lock()
	return func() {
		tg.mu.Unlock()
		g.guardMu.Lock()
		tg.refs--
		if tg.refs == 0 {
			delete(g.guards, topicName)
		}
		g.guardMu.Unlock()
	}
}

// SetTopicRetiredHook registers fn to run whenever a topic
// incarnation's local state is retired: its directory was purged, or
// quarantined because a newer incarnation of the name is live. The
// owner of the per-topic in-memory state (consumer reservations and
// committed frontiers, loaded schemas) drops it there so nothing of the
// retired incarnation carries over into a same-named successor. fn runs
// outside the log map lock but under the topic's guard, so it may Peek
// but must not Get.
func (g *Logs) SetTopicRetiredHook(fn func(topicName string)) {
	g.retired = fn
}

// ensureIncarnationLocked makes topics/<name> the directory of the
// incarnation id before a partition log is opened in it, and reports
// whether a directory of another incarnation was quarantined doing so
// (the caller runs the retired hook once it has released mu). Caller
// holds the topic's guard and mu (write). An empty id is a record
// without an incarnation: the directory is used as-is.
func (g *Logs) ensureIncarnationLocked(topicName, id string) (quarantined bool, err error) {
	if id == "" {
		return false, nil
	}
	topicDir := storage.TopicDir(g.dataDir, topicName)
	marker, marked, err := storage.ReadTopicIncarnation(topicDir)
	if err != nil {
		return false, err
	}
	if marked && marker == id {
		return false, nil
	}
	if marked {
		// The directory belongs to another incarnation of the name: a
		// deleted topic whose purge never reached this node. Close
		// nothing that is not open (the map holds no entry for this
		// topic under the current incarnation), set the directory
		// aside for the sweep, and start the current incarnation from
		// an empty directory.
		if err := g.closeTopicLocked(topicName); err != nil {
			return false, fmt.Errorf("broker/runtime: close stale incarnation of %s: %w", topicName, err)
		}
		setAside, err := storage.QuarantineTopicDir(g.dataDir, topicName, marker)
		if err != nil {
			return false, fmt.Errorf("broker/runtime: quarantine stale incarnation of %s: %w", topicName, err)
		}
		g.logger.Error("topic directory belongs to a deleted incarnation of the topic; quarantined instead of served",
			"topic", topicName, "directory_incarnation", marker, "current_incarnation", id, "quarantine_dir", setAside)
		quarantined = true
	}
	// Unmarked: a fresh directory, or one written before markers
	// existed (adopted by the current incarnation, the upgrade path).
	if err := storage.WriteTopicIncarnation(topicDir, id); err != nil {
		return quarantined, fmt.Errorf("broker/runtime: stamp incarnation of %s: %w", topicName, err)
	}
	return quarantined, nil
}

// notifyRetired runs the retired hook. Callers hold the topic's guard
// and have released mu.
func (g *Logs) notifyRetired(topicName string) {
	if g.retired != nil {
		g.retired(topicName)
	}
}

// EnsureTopicIncarnation prepares topics/<name> for the incarnation id
// exactly as an open does (adopt an unmarked directory, quarantine a
// directory of another incarnation, stamp the marker) without opening
// a log. A move calls it before installing a copied partition so the
// copy never lands inside a deleted incarnation's directory.
func (g *Logs) EnsureTopicIncarnation(topicName, id string) error {
	unlock := g.lockTopic(topicName)
	defer unlock()
	g.mu.Lock()
	quarantined, err := g.ensureIncarnationLocked(topicName, id)
	g.mu.Unlock()
	if quarantined {
		g.notifyRetired(topicName)
	}
	return err
}

// TopicIncarnationMatches reports whether topics/<name> may be served
// as the incarnation id: the record has no ID, the directory has no
// marker, or the marker equals the ID. Used by paths that read a
// partition directory directly, without opening a log (the transfer
// info a move source serves), so they never hand out a deleted
// incarnation's segments.
func (g *Logs) TopicIncarnationMatches(topicName, id string) (bool, error) {
	if id == "" {
		return true, nil
	}
	marker, marked, err := storage.ReadTopicIncarnation(storage.TopicDir(g.dataDir, topicName))
	if err != nil {
		return false, err
	}
	return !marked || marker == id, nil
}

// PurgeTopic removes the local on-disk state of one incarnation of a
// topic and reports whether topics/<name> was removed. It runs under
// the topic's guard from closing the open logs through unlinking, so
// no Get opens a log in the directory while it is being removed.
//
// id names the incarnation being purged. When set:
//
//   - a quarantined directory for it (topics/<name>.stale-<id>) is
//     removed: the purge is the leader's word that the incarnation is
//     gone;
//   - topics/<name> is removed when its marker carries the id, or when
//     it has no marker (written before markers existed: the purged
//     incarnation's, or older);
//   - topics/<name> is left alone when its marker names ANOTHER
//     incarnation: the name was recreated and that directory is the
//     live topic's.
//
// An empty id is a purge from a sender that predates incarnation IDs
// and removes topics/<name> whatever it holds, as it always did.
func (g *Logs) PurgeTopic(topicName, id string) (purged bool, err error) {
	unlock := g.lockTopic(topicName)
	purged, err = g.purgeTopicGuarded(topicName, id)
	unlock()
	if purged {
		// Retire the topic's produce-serialization mutexes too;
		// otherwise topic churn leaks one entry per (topic, partition)
		// forever. Each entry is deleted only while holding its mutex
		// (retireProduceMutex), so a produce commit mid-critical-section
		// finishes first. That commit may be inside Get waiting for the
		// guard, which is why this runs only after the guard is
		// released.
		g.retireProduceEntries(func(k string) bool { return strings.HasPrefix(k, topicName+"/") })
	}
	return purged, err
}

// purgeTopicGuarded is PurgeTopic's body; caller holds the guard.
func (g *Logs) purgeTopicGuarded(topicName, id string) (purged bool, err error) {
	if id != "" {
		if err := removeQuarantines(g.dataDir, topicName, id); err != nil {
			return false, err
		}
	}
	dir := storage.TopicDir(g.dataDir, topicName)
	if id != "" {
		marker, marked, err := storage.ReadTopicIncarnation(dir)
		if err != nil {
			return false, err
		}
		if marked && marker != id {
			g.logger.Info("purge: topic directory belongs to another incarnation; left in place",
				"topic", topicName, "purged_incarnation", id, "directory_incarnation", marker)
			return false, nil
		}
	}

	g.mu.Lock()
	closeErr := g.closeTopicLocked(topicName)
	rmErr := os.RemoveAll(dir)
	g.mu.Unlock()
	if closeErr != nil {
		err = closeErr
	}
	if rmErr != nil && err == nil {
		err = fmt.Errorf("broker/runtime: remove topic dir: %w", rmErr)
	}
	g.notifyRetired(topicName)
	return true, err
}

// removeQuarantines deletes every quarantine directory of topicName's
// incarnation id: the exact name and the numbered variants a repeated
// quarantine can produce.
func removeQuarantines(dataDir, topicName, id string) error {
	entries, err := os.ReadDir(storage.TopicDir(dataDir, ""))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	exact := topicName + storage.StaleTopicDirSuffix + id
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name != exact && !strings.HasPrefix(name, exact+".") {
			continue
		}
		if err := os.RemoveAll(storage.TopicDir(dataDir, name)); err != nil {
			return fmt.Errorf("broker/runtime: remove quarantined topic dir: %w", err)
		}
	}
	return nil
}
