package runtime

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

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
// <topic>.stale-<id> AND whose marker equals that id; the suffix alone
// is not proof (a topic may legitimately be named that way), so any
// other combination is treated as a plain topic directory named by the
// full entry name.
func classifyTopicDir(topicsRoot, base string) (OrphanCandidate, error) {
	dir := filepath.Join(topicsRoot, base)
	marker, marked, err := storage.ReadTopicIncarnation(dir)
	if err != nil {
		return OrphanCandidate{}, err
	}
	if name, id, ok := storage.ParseStaleTopicDirName(base); ok && marked && marker == id {
		return OrphanCandidate{Dir: dir, Topic: name, Incarnation: id, Quarantined: true}, nil
	}
	return OrphanCandidate{Dir: dir, Topic: base, Incarnation: marker}, nil
}
