package main

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// waitForLeadership blocks until the single-node metastore elects itself
// leader, failing the test after five seconds.
func waitForLeadership(t *testing.T, store *metastore.Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("metastore leader election timed out")
}
