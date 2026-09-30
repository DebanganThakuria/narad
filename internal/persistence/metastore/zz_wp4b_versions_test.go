package metastore

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func (k *keyedVersions) wp4bCells() int {
	if t := k.table.Load(); t != nil {
		return len(t.cells)
	}
	return 0
}

// TestWP4BRetireTopicAdvancesLikeBumps: retiring a topic moves its three
// versions exactly as the three bumps it stands for.
func TestWP4BRetireTopicAdvancesLikeBumps(t *testing.T) {
	v := newMetadataDomainVersions()
	v.bumpTopic("a")
	v.bumpAssignment("a")
	v.bumpSchema("a")
	v.bumpTopic("b")
	topicA, assignA, schemaA, topicB := v.topicVersion("a"), v.assignmentVersion("a"), v.schemaVersion("a"), v.topicVersion("b")

	v.retireTopic("a")
	if v.topicVersion("a") <= topicA || v.assignmentVersion("a") <= assignA || v.schemaVersion("a") <= schemaA {
		t.Fatalf("retireTopic did not advance every domain: topic %d->%d assignment %d->%d schema %d->%d",
			topicA, v.topicVersion("a"), assignA, v.assignmentVersion("a"), schemaA, v.schemaVersion("a"))
	}
	if got := v.topicVersion("b"); got != topicB {
		t.Fatalf("retiring a moved another topic's version: %d -> %d", topicB, got)
	}
	// A recreate bumps the tombstoned cell back into use.
	before := v.topicVersion("a")
	v.bumpTopic("a")
	if got := v.topicVersion("a"); got <= before {
		t.Fatalf("recreate after retire: version %d, want above %d", got, before)
	}
	if _, tomb := v.topics.retired["a"]; tomb {
		t.Fatal("a recreated name is still marked retired")
	}
}

// TestWP4BRetiredKeysArePruned churns uniquely named topics. The tables
// used to keep a cell per name ever created, and every new name copied
// the whole table; now they hold the live names plus fewer than
// maxRetiredKeys tombstones, and every name still reads monotonically.
func TestWP4BRetiredKeysArePruned(t *testing.T) {
	v := newMetadataDomainVersions()
	const live, churn = 50, 5000
	for i := range live {
		name := fmt.Sprintf("live-%d", i)
		v.bumpTopic(name)
		v.bumpAssignment(name)
		v.bumpSchema(name)
	}
	liveTopic := v.topicVersion("live-7")
	lastSeen := map[string]uint64{}
	for i := range churn {
		name := fmt.Sprintf("job-%05d", i)
		v.bumpTopic(name)
		v.bumpAssignment(name)
		v.bumpSchema(name)
		lastSeen[name] = v.schemaVersion(name)
		v.retireTopic(name)
		if got := v.schemaVersion(name); got <= lastSeen[name] {
			t.Fatalf("%s: schema version %d after retire, want above %d", name, got, lastSeen[name])
		}
		lastSeen[name] = v.schemaVersion(name)
	}
	for _, d := range []*keyedVersions{&v.topics, &v.assignments, &v.schemas} {
		if cells := d.wp4bCells(); cells >= live+maxRetiredKeys {
			t.Fatalf("table holds %d cells after %d deletes, want under %d", cells, churn, live+maxRetiredKeys)
		}
	}
	if got := v.topicVersion("live-7"); got != liveTopic {
		t.Fatalf("a live topic's version moved on prunes: %d -> %d", liveTopic, got)
	}
	for name, seen := range lastSeen {
		if got := v.schemaVersion(name); got < seen {
			t.Fatalf("%s: schema version went back from %d to %d after a prune", name, seen, got)
		}
		v.bumpSchema(name) // recreate with a schema
		if got := v.schemaVersion(name); got <= seen {
			t.Fatalf("%s: recreated name reads %d, want above %d", name, got, seen)
		}
	}
}

// TestWP4BStaleCacheEntryNeverLooksCurrent models the caches that key
// on these versions: an entry is (the key's data generation, the version
// read before loading it). Under random bumps, deletes, recreates,
// prunes and restores, an entry whose version still equals the key's
// version must hold the key's current data.
func TestWP4BStaleCacheEntryNeverLooksCurrent(t *testing.T) {
	v := newMetadataDomainVersions()
	rng := rand.New(rand.NewSource(42))
	type entry struct{ gen, version uint64 }
	gen := map[string]uint64{}
	cache := map[string]entry{}
	names := make([]string, 8000)
	for i := range names {
		names[i] = fmt.Sprintf("n%d", i)
	}
	prunes := 0
	for step := range 150_000 {
		name := names[rng.Intn(len(names))]
		switch op := rng.Intn(100); {
		case op < 20:
			v.bumpSchema(name)
			gen[name]++
		case op < 45:
			before := v.schemas.wp4bCells()
			v.retireTopic(name)
			gen[name]++
			if v.schemas.wp4bCells() < before {
				prunes++
			}
		case op == 45 && step%50 == 0:
			v.bumpAll()
			for _, n := range names {
				gen[n]++ // a restore may have changed anything
			}
		default:
			// A read: a hit serves the entry, a miss reloads it.
			version := v.schemaVersion(name)
			if e, ok := cache[name]; ok && e.version == version {
				if e.gen != gen[name] {
					t.Fatalf("step %d: %s cached at version %d holds generation %d, current is %d", step, name, version, e.gen, gen[name])
				}
				continue
			}
			cache[name] = entry{gen: gen[name], version: version}
		}
	}
	if prunes == 0 {
		t.Fatal("no prune happened; the test exercised nothing")
	}
	t.Logf("prunes: %d", prunes)
}

// TestWP4BRetireConcurrentReadsAreMonotonic runs lock-free readers
// against creates, deletes (with prunes), recreates and restores under
// the race detector: no key's version may ever go backwards.
func TestWP4BRetireConcurrentReadsAreMonotonic(t *testing.T) {
	v := newMetadataDomainVersions()
	keys := []string{"orders", "payments", "job-1", "job-2", "job-3", "never-seen"}
	stop := make(chan struct{})
	violations := make(chan string, 16)
	var readers sync.WaitGroup
	for r := range 8 {
		readers.Go(func() {
			last := map[string][3]uint64{}
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := keys[(r+i)%len(keys)]
				got := [3]uint64{v.topicVersion(key), v.assignmentVersion(key), v.schemaVersion(key)}
				for d := range got {
					if got[d] < last[key][d] {
						violations <- fmt.Sprintf("%s domain %d went backwards: %d -> %d", key, d, last[key][d], got[d])
						return
					}
				}
				last[key] = got
			}
		})
	}
	var writers sync.WaitGroup
	for w := range 4 {
		writers.Go(func() {
			for i := range 6000 {
				switch {
				case i%1500 == 999 && w == 0:
					v.bumpAll()
				case i%3 == 0:
					name := fmt.Sprintf("job-%d", i%4)
					v.bumpTopic(name)
					v.bumpSchema(name)
				case i%3 == 1:
					v.retireTopic(fmt.Sprintf("job-%d", i%4))
				default:
					// Unique names drive the prunes.
					name := fmt.Sprintf("churn-%d-%d", w, i)
					v.bumpTopic(name)
					v.retireTopic(name)
					v.bumpAssignment(keys[i%2])
				}
			}
		})
	}
	writers.Wait()
	close(stop)
	readers.Wait()
	close(violations)
	for msg := range violations {
		t.Error(msg)
	}
}
