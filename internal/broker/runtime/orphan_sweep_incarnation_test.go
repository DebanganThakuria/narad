package runtime

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// The sweep reports each directory's incarnation and whether it is a
// quarantined copy, and removes exactly what keep refuses. A topic
// legitimately named "x.stale-y" is a plain directory (its marker does
// not equal the parsed suffix), never mistaken for a quarantine.
func TestSweepOrphanTopicDirsClassifiesIncarnations(t *testing.T) {
	dataDir := t.TempDir()
	mkTopicDir(t, dataDir, "unmarked")
	mkTopicDir(t, dataDir, "marked")
	if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "marked"), "1111111111111111"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	mkTopicDir(t, dataDir, "marked.stale-1111111111111111")
	if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "marked.stale-1111111111111111"), "1111111111111111"); err != nil {
		t.Fatalf("WriteTopicIncarnation(stale): %v", err)
	}
	mkTopicDir(t, dataDir, "odd.stale-name")
	if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "odd.stale-name"), "3333333333333333"); err != nil {
		t.Fatalf("WriteTopicIncarnation(odd): %v", err)
	}

	var seen []OrphanCandidate
	removed, err := SweepOrphanTopicDirs(dataDir, func(c OrphanCandidate) bool {
		seen = append(seen, c)
		return !c.Quarantined
	}, nil)
	if err != nil {
		t.Fatalf("SweepOrphanTopicDirs: %v", err)
	}
	if !slices.Equal(removed, []string{"marked.stale-1111111111111111"}) {
		t.Fatalf("removed = %v, want only the quarantined dir", removed)
	}
	want := map[string]OrphanCandidate{
		"unmarked":                      {Topic: "unmarked"},
		"marked":                        {Topic: "marked", Incarnation: "1111111111111111"},
		"marked.stale-1111111111111111": {Topic: "marked", Incarnation: "1111111111111111", Quarantined: true},
		"odd.stale-name":                {Topic: "odd.stale-name", Incarnation: "3333333333333333"},
	}
	if len(seen) != len(want) {
		t.Fatalf("candidates = %+v, want %d", seen, len(want))
	}
	for _, c := range seen {
		w, ok := want[filepath.Base(c.Dir)]
		if !ok {
			t.Fatalf("unexpected candidate %+v", c)
		}
		if c.Topic != w.Topic || c.Incarnation != w.Incarnation || c.Quarantined != w.Quarantined {
			t.Fatalf("candidate %s = %+v, want %+v", filepath.Base(c.Dir), c, w)
		}
	}
	for _, kept := range []string{"unmarked", "marked", "odd.stale-name"} {
		if _, err := os.Stat(storage.TopicDir(dataDir, kept)); err != nil {
			t.Fatalf("%s was removed: %v", kept, err)
		}
	}
}
