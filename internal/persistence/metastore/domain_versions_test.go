package metastore

import (
	"sync"
	"testing"
)

// TestDomainVersionsMaxOfFloorAndKey pins the "max(all, key)" rule: a
// key bump raises only that key, a bumpAll raises every domain to a
// common floor above every per-key version, and later key bumps rise
// above the floor again.
func TestDomainVersionsMaxOfFloorAndKey(t *testing.T) {
	v := newMetadataDomainVersions()

	if got := v.topicVersion("a"); got != 0 {
		t.Fatalf("fresh topicVersion(a) = %d, want 0", got)
	}
	v.bumpTopic("a")
	aAfterBump := v.topicVersion("a")
	if aAfterBump == 0 {
		t.Fatal("bumpTopic did not raise the key")
	}
	if got := v.topicVersion("b"); got != 0 {
		t.Fatalf("topicVersion(b) = %d after bumping a, want 0", got)
	}
	if got := v.assignmentVersion("a"); got != 0 {
		t.Fatalf("assignmentVersion(a) = %d after a topic bump, want 0 (domains are independent)", got)
	}

	v.bumpAssignment("a")
	v.bumpSchema("a")
	v.bumpRoutingMembers()
	v.bumpUsers()
	before := map[string]uint64{
		"topic":       v.topicVersion("a"),
		"assignment":  v.assignmentVersion("a"),
		"schema":      v.schemaVersion("a"),
		"routing":     v.routingMembersVersion(),
		"users":       v.usersVersion(),
		"other topic": v.topicVersion("b"),
	}

	v.bumpAll()
	floor := v.topicVersion("never-seen")
	if floor == 0 {
		t.Fatal("bumpAll did not raise the whole-domain floor")
	}
	for name, was := range before {
		if was >= floor {
			t.Fatalf("%s version %d before bumpAll is not below the new floor %d", name, was, floor)
		}
	}
	for name, got := range map[string]uint64{
		"topic":       v.topicVersion("a"),
		"assignment":  v.assignmentVersion("a"),
		"schema":      v.schemaVersion("a"),
		"routing":     v.routingMembersVersion(),
		"users":       v.usersVersion(),
		"other topic": v.topicVersion("b"),
	} {
		if got != floor {
			t.Fatalf("%s version after bumpAll = %d, want floor %d", name, got, floor)
		}
	}

	v.bumpTopic("a")
	if got := v.topicVersion("a"); got <= floor {
		t.Fatalf("topicVersion(a) after post-restore bump = %d, want > floor %d", got, floor)
	}
	if got := v.topicVersion("b"); got != floor {
		t.Fatalf("topicVersion(b) = %d, want floor %d (untouched key stays at the floor)", got, floor)
	}
}

// TestDomainVersionsConcurrentReadsAreMonotonic runs lock-free readers
// against concurrent key bumps and snapshot-style bumpAlls under the
// race detector: every reader must observe a non-decreasing sequence
// for every version it polls, and nothing may tear.
func TestDomainVersionsConcurrentReadsAreMonotonic(t *testing.T) {
	v := newMetadataDomainVersions()
	keys := []string{"orders", "payments", "logs", "audit"}

	const readers = 8
	const rounds = 2000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	violations := make(chan string, readers)

	for r := range readers {
		wg.Go(func() {
			key := keys[r%len(keys)]
			var lastTopic, lastAssign, lastSchema, lastMembers, lastUsers uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				if got := v.topicVersion(key); got < lastTopic {
					violations <- "topic version went backwards"
					return
				} else {
					lastTopic = got
				}
				if got := v.assignmentVersion(key); got < lastAssign {
					violations <- "assignment version went backwards"
					return
				} else {
					lastAssign = got
				}
				if got := v.schemaVersion(key); got < lastSchema {
					violations <- "schema version went backwards"
					return
				} else {
					lastSchema = got
				}
				if got := v.routingMembersVersion(); got < lastMembers {
					violations <- "routing members version went backwards"
					return
				} else {
					lastMembers = got
				}
				if got := v.usersVersion(); got < lastUsers {
					violations <- "users version went backwards"
					return
				} else {
					lastUsers = got
				}
			}
		})
	}

	var writers sync.WaitGroup
	for w := range 4 {
		writers.Go(func() {
			for i := range rounds {
				key := keys[(i+w)%len(keys)]
				switch i % 7 {
				case 0:
					v.bumpTopic(key)
				case 1:
					v.bumpAssignment(key)
				case 2:
					v.bumpSchema(key)
				case 3:
					v.bumpRoutingMembers()
				case 4:
					v.bumpUsers()
				case 5:
					v.bumpTopic("new-" + key)
				default:
					if i%97 == 6 {
						v.bumpAll()
					} else {
						v.bumpAssignment(key)
					}
				}
			}
		})
	}
	writers.Wait()
	close(stop)
	wg.Wait()
	close(violations)
	for msg := range violations {
		t.Error(msg)
	}

	// Every key bumped after the last bumpAll must sit at or above the
	// floor, and the floor itself must be below the latest key bumps.
	floor := v.topicVersion("never-bumped")
	for _, key := range keys {
		if got := v.topicVersion(key); got < floor {
			t.Fatalf("topicVersion(%s) = %d below floor %d", key, got, floor)
		}
	}
}
