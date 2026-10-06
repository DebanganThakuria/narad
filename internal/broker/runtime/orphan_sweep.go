package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// OrphanCandidate is one directory under dataDir/topics as the sweep
// found it: the topic name it is for, the incarnation its marker names
// (empty for a directory written before markers existed), and whether
// it is a quarantined copy (topics/<name>.stale-<id>, set aside by an
// open that found it belonged to a deleted incarnation).
type OrphanCandidate struct {
	Dir         string
	Topic       string
	Incarnation string
	Quarantined bool
}

// SweepOrphanTopicDirs removes topic directories under dataDir/topics
// that keep reports false for: directories whose topic (incarnation) no
// longer exists.
//
// It is crash-safety, not the normal delete path: a node that dies
// between a topic's metastore delete and the purge of its files leaves an
// orphan directory that no live delete will ever revisit (and, with the
// Get guard, no reaper either); a node that was down while a same-named
// topic was deleted and recreated keeps the OLD incarnation's directory,
// which the name alone would call live. The sweep reconciles disk
// against metadata by INCARNATION: keep receives the marker's ID, and a
// directory whose ID is not the live topic's is an orphan whether or not
// the name exists. A quarantined directory is reported as such; it is
// reclaimed once keep confirms its incarnation is gone.
//
// SAFETY: the caller MUST ensure the local metastore replica is caught up
// before calling, and MUST confirm with the leader before answering
// false; if the local replica were stale, a still-existing topic could be
// misjudged as absent and its data deleted. Run this only after
// AppliedCaughtUp, and (for non-quarantined directories) before the node
// begins accepting topic creates, so the topic set is authoritative and
// not racing concurrent creation.
func SweepOrphanTopicDirs(dataDir string, keep func(c OrphanCandidate) bool, logger *slog.Logger) (removed []string, err error) {
	topicsRoot := filepath.Join(dataDir, "topics")
	entries, readErr := os.ReadDir(topicsRoot)
	if os.IsNotExist(readErr) {
		return nil, nil
	}
	if readErr != nil {
		return nil, fmt.Errorf("runtime: read topics dir: %w", readErr)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		c, cerr := classifyTopicDir(topicsRoot, entry.Name())
		if cerr != nil {
			if logger != nil {
				logger.Warn("orphan sweep: cannot read incarnation marker; keeping dir", "dir", entry.Name(), "err", cerr)
			}
			if err == nil {
				err = cerr
			}
			continue
		}
		if keep(c) {
			continue
		}
		if rmErr := os.RemoveAll(c.Dir); rmErr != nil {
			if logger != nil {
				logger.Warn("orphan sweep: failed to remove orphaned topic dir", "dir", entry.Name(), "topic", c.Topic, "incarnation", c.Incarnation, "err", rmErr)
			}
			if err == nil {
				err = rmErr
			}
			continue
		}
		removed = append(removed, entry.Name())
		if logger != nil {
			logger.Info("orphan sweep: removed orphaned topic dir", "dir", entry.Name(), "topic", c.Topic, "incarnation", c.Incarnation, "quarantined", c.Quarantined)
		}
	}
	return removed, err
}

// classifyTopicDir reads a directory's marker and decides what it is.
// A quarantined directory is one whose name parses as
// <topic>.stale-<id> AND whose marker equals that id, or one of the
// numbered variants <topic>.stale-<id>.<n> a repeated quarantine of the
// same incarnation makes; the suffix alone is not proof (a topic may
// legitimately be named that way), so any other combination is treated
// as a plain topic directory named by the full entry name.
func classifyTopicDir(topicsRoot, base string) (OrphanCandidate, error) {
	dir := filepath.Join(topicsRoot, base)
	marker, marked, err := storage.ReadTopicIncarnation(dir)
	if err != nil {
		return OrphanCandidate{}, err
	}
	if name, id, ok := storage.ParseStaleTopicDirName(base); ok && marked && quarantineOf(id, marker) {
		return OrphanCandidate{Dir: dir, Topic: name, Incarnation: marker, Quarantined: true}, nil
	}
	return OrphanCandidate{Dir: dir, Topic: base, Incarnation: marker}, nil
}

// quarantineOf reports whether suffix, the part of a quarantined
// directory's name after ".stale-", names the incarnation marker: the
// marker itself, or the marker followed by "." and the number
// storage.QuarantineTopicDir adds when the plain name is taken.
func quarantineOf(suffix, marker string) bool {
	if suffix == marker {
		return true
	}
	n, ok := strings.CutPrefix(suffix, marker+".")
	if !ok || n == "" {
		return false
	}
	for _, c := range n {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ListTopicDirs classifies every directory under dataDir/topics as the
// orphan sweeps see it (see OrphanCandidate). A directory whose marker
// cannot be read is left out and the first such error returned with the
// rest; a missing topics directory lists nothing.
func ListTopicDirs(dataDir string) ([]OrphanCandidate, error) {
	topicsRoot := filepath.Join(dataDir, "topics")
	entries, err := os.ReadDir(topicsRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("runtime: read topics dir: %w", err)
	}
	var (
		out      []OrphanCandidate
		firstErr error
	)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		c, cerr := classifyTopicDir(topicsRoot, e.Name())
		if cerr != nil {
			if firstErr == nil {
				firstErr = cerr
			}
			continue
		}
		out = append(out, c)
	}
	return out, firstErr
}

// ErrNotAnOrphan reports that ReclaimOrphanTopicDir found, under the
// topic's guard, that the directory is not (or is no longer) a leftover
// of the incarnation it was asked to reclaim.
var ErrNotAnOrphan = errors.New("topic directory is not an orphan of that incarnation")

// ReclaimOrphanTopicDir purges topics/<name> as the leftover of the
// deleted incarnation id: a topic whose purge never reached this node
// (it was down, marked dead or lagging when the delete committed). The
// caller must have had the LEADER confirm that incarnation gone (the
// name is absent, or live as another incarnation). The periodic sweeps
// run without the startup create gate, so the directory is never removed
// by path: under the topic's guard, where no open of the name runs, the
// call refuses with ErrNotAnOrphan unless
//
//   - the local metastore record of the name is absent (a record that
//     is present, or cannot be read, leaves the directory to the open
//     path and to the stale-incarnation sweep), and
//   - the directory's marker still names id. A recreate this node served
//     in the meantime set the old directory aside and stamped its own
//     marker; an unmarked directory is never purged here (id must be
//     set), since this path cannot tell it from a directory being made.
//
// It then purges exactly as PurgeTopic does (the open logs of the name
// are closed, the directory is set aside before it is removed, and the
// retired hook drops the name's in-memory state) and reports whether
// topics/<name> was removed. A Logs without a metastore refuses: it
// cannot tell a live topic from an orphan.
func (g *Logs) ReclaimOrphanTopicDir(topicName, id string) (purged bool, err error) {
	if id == "" {
		return false, fmt.Errorf("%w: %s: an unmarked directory is removed only by the startup sweep", ErrNotAnOrphan, topicName)
	}
	if g.metastore == nil {
		return false, fmt.Errorf("%w: %s: no metastore to check the topic against", ErrNotAnOrphan, topicName)
	}
	unlock := g.lockTopic(topicName)
	purged, err = g.reclaimOrphanGuarded(topicName, id)
	unlock()
	if purged {
		// As PurgeTopic: after the guard, since a produce commit waiting
		// for the guard may hold one of these mutexes.
		g.retireProduceEntries(func(k logKey) bool { return k.topic == topicName })
	}
	return purged, err
}

// reclaimOrphanGuarded is ReclaimOrphanTopicDir's body; caller holds the
// topic's guard.
func (g *Logs) reclaimOrphanGuarded(topicName, id string) (bool, error) {
	switch t, err := g.metastore.GetTopic(context.Background(), topicName); {
	case err == nil:
		return false, fmt.Errorf("%w: %s: the local record names incarnation %q", ErrNotAnOrphan, topicName, t.ID)
	case !errors.Is(err, errs.ErrNotFound):
		return false, fmt.Errorf("%w: %s: read the local record: %w", ErrNotAnOrphan, topicName, err)
	}
	marker, marked, err := storage.ReadTopicIncarnationOf(g.dataDir, topicName)
	if err != nil {
		return false, err
	}
	if !marked || marker != id {
		return false, fmt.Errorf("%w: %s: the directory's marker is %q (marked %v), not %s", ErrNotAnOrphan, topicName, marker, marked, id)
	}
	return g.purgeTopicGuarded(topicName, id)
}
