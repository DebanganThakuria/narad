package runtime

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func mkTopicDir(t *testing.T, dataDir, topicName string) {
	t.Helper()
	dir := filepath.Join(dataDir, "topics", topicName, "p00000")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.log"), []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func topicDirExists(t *testing.T, dataDir, topicName string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dataDir, "topics", topicName))
	return err == nil
}

func TestSweepOrphanTopicDirsRemovesAbsentKeepsPresent(t *testing.T) {
	dataDir := t.TempDir()
	mkTopicDir(t, dataDir, "live-1")
	mkTopicDir(t, dataDir, "live-2")
	mkTopicDir(t, dataDir, "orphan-1")
	mkTopicDir(t, dataDir, "orphan-2")

	exists := map[string]bool{"live-1": true, "live-2": true}
	removed, err := SweepOrphanTopicDirs(dataDir, func(name string) bool { return exists[name] }, nil)
	if err != nil {
		t.Fatalf("SweepOrphanTopicDirs() error = %v", err)
	}

	slices.Sort(removed)
	if want := []string{"orphan-1", "orphan-2"}; !slices.Equal(removed, want) {
		t.Fatalf("removed = %v, want %v", removed, want)
	}
	if !topicDirExists(t, dataDir, "live-1") || !topicDirExists(t, dataDir, "live-2") {
		t.Fatal("live topic dirs were removed; want kept")
	}
	if topicDirExists(t, dataDir, "orphan-1") || topicDirExists(t, dataDir, "orphan-2") {
		t.Fatal("orphan topic dirs survived; want removed")
	}
}

func TestSweepOrphanTopicDirsNoTopicsDir(t *testing.T) {
	// A data dir with no topics/ subdir must be a no-op, not an error.
	removed, err := SweepOrphanTopicDirs(t.TempDir(), func(string) bool { return true }, nil)
	if err != nil {
		t.Fatalf("SweepOrphanTopicDirs() error = %v, want nil", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
	}
}

func TestSweepOrphanTopicDirsKeepsEverythingWhenAllExist(t *testing.T) {
	// Guards the data-loss case: if the metastore says every topic exists
	// (e.g. nothing was deleted), the sweep must remove nothing.
	dataDir := t.TempDir()
	mkTopicDir(t, dataDir, "a")
	mkTopicDir(t, dataDir, "b")

	removed, err := SweepOrphanTopicDirs(dataDir, func(string) bool { return true }, nil)
	if err != nil {
		t.Fatalf("SweepOrphanTopicDirs() error = %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none (all topics exist)", removed)
	}
	if !topicDirExists(t, dataDir, "a") || !topicDirExists(t, dataDir, "b") {
		t.Fatal("a live topic dir was removed; want all kept")
	}
}

func TestSweepOrphanTopicDirsIgnoresNonDirEntries(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "topics"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// A stray file under topics/ must be ignored, not treated as a topic.
	if err := os.WriteFile(filepath.Join(dataDir, "topics", "README"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	removed, err := SweepOrphanTopicDirs(dataDir, func(string) bool { return false }, nil)
	if err != nil {
		t.Fatalf("SweepOrphanTopicDirs() error = %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none (file entry must be skipped)", removed)
	}
}
