package runtime

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// SweepOrphanTopicDirs removes partition-log directories under
// dataDir/topics whose topic no longer exists according to topicExists.
//
// It is crash-safety, not the normal delete path: a node that dies
// between a topic's metastore delete and the purge of its files leaves an
// orphan directory that no live delete will ever revisit (and, with the
// Get guard, no reaper either). This reconciles disk against metadata on
// startup.
//
// SAFETY: the caller MUST ensure the local metastore replica is caught up
// before calling. topicExists is consulted against the local replica; if
// that replica were stale, a still-existing topic could be misjudged as
// absent and its data deleted. Run this only after AppliedCaughtUp, and
// before the node begins accepting topic creates, so the topic set is
// authoritative and not racing concurrent creation.
func SweepOrphanTopicDirs(dataDir string, topicExists func(name string) bool, logger *slog.Logger) (removed []string, err error) {
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
		name := entry.Name()
		if topicExists(name) {
			continue
		}
		dir := filepath.Join(topicsRoot, name)
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			if logger != nil {
				logger.Warn("orphan sweep: failed to remove orphaned topic dir", "topic", name, "err", rmErr)
			}
			if err == nil {
				err = rmErr
			}
			continue
		}
		removed = append(removed, name)
		if logger != nil {
			logger.Info("orphan sweep: removed orphaned topic dir", "topic", name)
		}
	}
	return removed, err
}
