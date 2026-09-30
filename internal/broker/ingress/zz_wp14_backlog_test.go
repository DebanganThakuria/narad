package ingress

import (
	"context"
	"testing"
)

// DispatchBacklog is the durable next seq minus the stored checkpoint:
// it counts what a restart would replay, drops as the checkpoint is
// stored, and survives a reopen because the checkpoint read at open
// seeds it.
func TestZZWP14DispatchBacklog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	if got := manager.DispatchBacklog(); got != 0 {
		t.Fatalf("backlog of a new WAL = %d, want 0", got)
	}
	for range 3 {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte("x")); err != nil {
			t.Fatalf("AcceptProduce() error = %v", err)
		}
	}
	if got := manager.DispatchBacklog(); got != 3 {
		t.Fatalf("backlog after 3 accepts = %d, want 3", got)
	}
	if err := manager.StoreProduceCheckpoint(manager.DurableProduceNext() - 1); err != nil {
		t.Fatalf("StoreProduceCheckpoint() error = %v", err)
	}
	if got := manager.DispatchBacklog(); got != 1 {
		t.Fatalf("backlog with one record past the checkpoint = %d, want 1", got)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	if got := reopened.DispatchBacklog(); got != 1 {
		t.Fatalf("backlog after reopen = %d, want the 1 record past the stored checkpoint", got)
	}
	if err := reopened.StoreProduceCheckpoint(reopened.DurableProduceNext()); err != nil {
		t.Fatalf("StoreProduceCheckpoint() error = %v", err)
	}
	if got := reopened.DispatchBacklog(); got != 0 {
		t.Fatalf("backlog with the checkpoint at the durable next seq = %d, want 0", got)
	}

	var nilManager *Manager
	if got := nilManager.DispatchBacklog(); got != 0 {
		t.Fatalf("nil manager backlog = %d, want 0", got)
	}
}
