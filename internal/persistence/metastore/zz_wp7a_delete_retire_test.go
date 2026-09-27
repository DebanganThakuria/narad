package metastore

import (
	"encoding/json"
	"fmt"
	"testing"
)

// zzWP7aVersions reads the three per-topic versions a delete moves.
func zzWP7aVersions(f *fsmState, name string) [3]uint64 {
	return [3]uint64{f.versions.topicVersion(name), f.versions.assignmentVersion(name), f.versions.schemaVersion(name)}
}

// zzWP7aFSM is an FSM whose database skips fsync: these tests apply
// thousands of creates and deletes and check versions, not durability.
func zzWP7aFSM(t *testing.T) *fsmState {
	t.Helper()
	f := newFanoutFSM(t)
	f.db.NoSync = true
	return f
}

func zzWP7aDelete(t *testing.T, f *fsmState, name string) {
	t.Helper()
	data, err := json.Marshal(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.applyDeleteTopic(data); err != nil {
		t.Fatalf("applyDeleteTopic(%s): %v", name, err)
	}
}

// TestZZWP7aDeleteTopicRetiresVersionCells churns uniquely named topics
// through the FSM's create and delete. The delete used to bump the
// deleted name's three versions, leaving a cell per name ever deleted in
// every table; it now retires them, so the tables stay bounded by the
// live names plus fewer than maxRetiredKeys tombstones.
func TestZZWP7aDeleteTopicRetiresVersionCells(t *testing.T) {
	f := zzWP7aFSM(t)
	const live, churn = 5, maxRetiredKeys + 200
	for i := range live {
		fsmCreateTopic(t, f, fmt.Sprintf("live-%d", i))
	}
	for i := range churn {
		name := fmt.Sprintf("job-%05d", i)
		fsmCreateTopic(t, f, name)
		zzWP7aDelete(t, f, name)
	}
	for _, d := range []*keyedVersions{&f.versions.topics, &f.versions.assignments, &f.versions.schemas} {
		if cells := d.wp4bCells(); cells >= live+maxRetiredKeys {
			t.Fatalf("a version table holds %d cells after %d deletes, want under %d", cells, churn, live+maxRetiredKeys)
		}
	}
}

// TestZZWP7aDeleteTopicVersionsNeverLookCurrent is the property the
// retire must keep for every cache keyed on these versions: an entry
// (data, version read before loading it) whose version still equals the
// name's version holds the name's current data. It runs creates,
// deletes and recreates through the FSM, with enough unique deletes in
// between to prune the tombstones, and checks each domain on its own: a
// create writes only the topic record, a delete removes the record, its
// schemas and its assignments.
func TestZZWP7aDeleteTopicVersionsNeverLookCurrent(t *testing.T) {
	f := zzWP7aFSM(t)
	const topicDomain = 0
	type entry struct{ gen, version [3]uint64 }
	gen := map[string]*[3]uint64{}
	cache := map[string]entry{}
	read := func(name string) {
		v := zzWP7aVersions(f, name)
		g := gen[name]
		if e, ok := cache[name]; ok {
			for d := range v {
				if e.version[d] == v[d] && e.gen[d] != g[d] {
					t.Fatalf("%s: domain %d cached at version %d holds generation %d, current is %d", name, d, v[d], e.gen[d], g[d])
				}
			}
		}
		cache[name] = entry{gen: *g, version: v}
	}
	names := []string{"orders", "payments", "audit"}
	for _, name := range names {
		gen[name] = new([3]uint64)
	}
	for round := range 3 {
		for _, name := range names {
			fsmCreateTopic(t, f, name)
			gen[name][topicDomain]++
			read(name)
		}
		// Unique names past the prune threshold, so the tombstones of
		// the names above are pruned at least once per round.
		for i := range maxRetiredKeys + 10 {
			name := fmt.Sprintf("r%d-job-%05d", round, i)
			fsmCreateTopic(t, f, name)
			zzWP7aDelete(t, f, name)
		}
		for _, name := range names {
			read(name)
			before := zzWP7aVersions(f, name)
			zzWP7aDelete(t, f, name)
			for d := range gen[name] {
				gen[name][d]++
			}
			after := zzWP7aVersions(f, name)
			for d := range after {
				if after[d] <= before[d] {
					t.Fatalf("round %d: deleting %s moved domain %d from %d to %d, want it advanced", round, name, d, before[d], after[d])
				}
			}
			read(name)
		}
	}
}
