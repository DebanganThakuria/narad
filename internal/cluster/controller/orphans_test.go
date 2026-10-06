package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// orphanController is a controller over store whose log lines land in
// the returned buffer.
func orphanController(store *fakeControllerStore) (*Controller, *strings.Builder) {
	var logs strings.Builder
	return &Controller{store: store, cfg: Config{Logger: slog.New(slog.NewTextHandler(&logs, nil))}.withDefaults()}, &logs
}

func orphanRows() []metastore.Assignment {
	return []metastore.Assignment{
		{Topic: "gone", Partition: 0, OwnerID: "a"},
		{Topic: "orders", Partition: 7, OwnerID: "a"},
	}
}

func TestOrphanRowsArePrunedOnceUsable(t *testing.T) {
	store := newFakeControllerStore("a")
	store.entryTypesUsable = true
	store.orphans = orphanRows()
	c, logs := orphanController(store)

	c.pruneOrphanAssignments(context.Background())

	if len(store.orphans) != 0 || store.prunes != 2 {
		t.Fatalf("after a pass: %d orphan rows left after %d prunes, want 0 after 2", len(store.orphans), store.prunes)
	}
	if store.leaderBarriers != 1 {
		t.Fatalf("%d leader barriers before the pass, want 1", store.leaderBarriers)
	}
	if !strings.Contains(logs.String(), "pruned assignment rows that belonged to no partition") || !strings.Contains(logs.String(), "rows=2") {
		t.Fatalf("log does not report the 2 rows pruned:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("an error was logged:\n%s", logs.String())
	}
}

func TestOrphanRowsAreLoggedWhileNotUsable(t *testing.T) {
	store := newFakeControllerStore("a")
	store.orphans = orphanRows()
	c, logs := orphanController(store)

	c.pruneOrphanAssignments(context.Background())
	c.pruneOrphanAssignments(context.Background())

	if len(store.orphans) != 2 {
		t.Fatalf("%d orphan rows left, want both kept while the prune entry type is not usable", len(store.orphans))
	}
	if store.prunes != 2 {
		t.Fatalf("%d prune attempts over two passes, want one per pass", store.prunes)
	}
	for _, key := range []string{"gone/0", "orders/7"} {
		line := "orphan assignment row for " + key + "; it is pruned once every member runs 3.1.0"
		if n := strings.Count(logs.String(), line); n != 1 {
			t.Fatalf("%q logged %d times over two passes, want once:\n%s", line, n, logs.String())
		}
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("the orphan rows were not logged at error:\n%s", logs.String())
	}

	store.entryTypesUsable = true
	c.pruneOrphanAssignments(context.Background())
	if len(store.orphans) != 0 {
		t.Fatalf("%d orphan rows left once the prune is usable, want 0", len(store.orphans))
	}
}

// A dead mark carries the heartbeat the controller judged, so a newer
// one that committed in between wins in the state machine.
func TestDeadMarkCarriesTheHeartbeatItWasDecidedFrom(t *testing.T) {
	store := newFakeControllerStore("a", "b")
	stale := time.Now().Add(-time.Hour).Unix()
	store.members[1].LastHeartbeat = stale
	store.members[0].LastHeartbeat = time.Now().Unix()
	c := &Controller{store: store, cfg: Config{DeadTimeout: time.Minute}.withDefaults()}

	c.checkHeartbeats(context.Background())

	if want := fmt.Sprintf("b@%d", stale); len(store.deadMarks) != 1 || store.deadMarks[0] != want {
		t.Fatalf("dead marks = %v, want [%s]", store.deadMarks, want)
	}
}
