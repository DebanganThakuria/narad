package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
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
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "marked"), "1111111111111111"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	mkTopicDir(t, dataDir, "marked.stale-1111111111111111")
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "marked.stale-1111111111111111"), "1111111111111111"); err != nil {
		t.Fatalf("WriteTopicIncarnation(stale): %v", err)
	}
	mkTopicDir(t, dataDir, "odd.stale-name")
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "odd.stale-name"), "3333333333333333"); err != nil {
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
		if _, err := os.Stat(topicDirT(t, dataDir, kept)); err != nil {
			t.Fatalf("%s was removed: %v", kept, err)
		}
	}
}

// A directory quarantined twice for the same incarnation (something
// recreated the plain name in between) gets a numbered name,
// <topic>.stale-<id>.<n>. It is that incarnation's copy like the first
// one and is classified as such, so the sweeps reclaim it once the
// leader confirms the incarnation gone; a suffix that is not a number
// stays a plain directory of that full name.
func TestSweepOrphanTopicDirsClassifiesANumberedQuarantine(t *testing.T) {
	dataDir := t.TempDir()
	const id = "1111111111111111"
	for range 2 {
		mkTopicDir(t, dataDir, "orders")
		if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "orders"), id); err != nil {
			t.Fatalf("WriteTopicIncarnation: %v", err)
		}
		if _, err := storage.QuarantineTopicDir(dataDir, "orders", id); err != nil {
			t.Fatalf("QuarantineTopicDir: %v", err)
		}
	}
	odd := "orders.stale-" + id + ".x"
	mkTopicDir(t, dataDir, odd)
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, odd), id); err != nil {
		t.Fatalf("WriteTopicIncarnation(odd): %v", err)
	}

	seen := map[string]OrphanCandidate{}
	if _, err := SweepOrphanTopicDirs(dataDir, func(c OrphanCandidate) bool {
		seen[filepath.Base(c.Dir)] = c
		return true
	}, nil); err != nil {
		t.Fatalf("SweepOrphanTopicDirs: %v", err)
	}
	want := map[string]OrphanCandidate{
		"orders.stale-" + id:        {Topic: "orders", Incarnation: id, Quarantined: true},
		"orders.stale-" + id + ".1": {Topic: "orders", Incarnation: id, Quarantined: true},
		odd:                         {Topic: odd, Incarnation: id},
	}
	if len(seen) != len(want) {
		t.Fatalf("candidates = %+v, want %d", seen, len(want))
	}
	for base, w := range want {
		c := seen[base]
		if c.Topic != w.Topic || c.Incarnation != w.Incarnation || c.Quarantined != w.Quarantined {
			t.Fatalf("candidate %s = %+v, want %+v", base, c, w)
		}
	}
}

// The periodic reclaim of a deleted topic's directory runs without the
// startup create gate, so under the topic's guard it purges only while
// the local record of the name is gone and the directory still carries
// the incarnation the leader confirmed gone. Anything else refuses with
// ErrNotAnOrphan and leaves the directory in place.
func TestReclaimOrphanTopicDirPurgesOnlyAGoneIncarnationsDirectory(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })

	if err := store.CreateTopic(ctx, topic.Topic{Name: "live", ID: "2222222222222222", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	l, err := logs.Get("live", 0)
	if err != nil {
		t.Fatal(err)
	}
	appendOld(t, l, 3, "live")
	mkTopicDir(t, dataDir, "gone")
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "gone"), "1111111111111111"); err != nil {
		t.Fatal(err)
	}
	mkTopicDir(t, dataDir, "legacy")

	for _, tc := range []struct{ name, topic, id string }{
		{"the local record still lists the topic", "live", "2222222222222222"},
		{"the directory carries another incarnation", "gone", "3333333333333333"},
		{"the directory has no marker", "legacy", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			purged, err := logs.ReclaimOrphanTopicDir(tc.topic, tc.id)
			if purged || !errors.Is(err, ErrNotAnOrphan) {
				t.Fatalf("ReclaimOrphanTopicDir(%s, %q) = %v, %v; want a refusal with ErrNotAnOrphan", tc.topic, tc.id, purged, err)
			}
			if _, err := os.Stat(topicDirT(t, dataDir, tc.topic)); err != nil {
				t.Fatalf("the refused directory is gone: %v", err)
			}
		})
	}
	if l, err := logs.Get("live", 0); err != nil || l.NextOffset() != 3 {
		t.Fatalf("the live topic after the refusals: err %v", err)
	}

	purged, err := logs.ReclaimOrphanTopicDir("gone", "1111111111111111")
	if !purged || err != nil {
		t.Fatalf("ReclaimOrphanTopicDir(gone) = %v, %v; want purged", purged, err)
	}
	if _, err := os.Stat(topicDirT(t, dataDir, "gone")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("topics/gone after the reclaim: %v", err)
	}
	if left := staleDirs(t, dataDir, "gone"); len(left) != 0 {
		t.Fatalf("the reclaim left %v behind", left)
	}

	bare := NewLogs(dataDir, storage.Options{}, nil, nil)
	if purged, err := bare.ReclaimOrphanTopicDir("legacy", "1111111111111111"); purged || !errors.Is(err, ErrNotAnOrphan) {
		t.Fatalf("a log map without a metastore: %v, %v; want a refusal", purged, err)
	}
}
