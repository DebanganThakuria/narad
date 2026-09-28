package ingress

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// Healthy exposes the WAL's write latch: true while accepts work, false
// for good once a WAL sync fails and every later accept is refused.
func TestZZWP5ManagerHealthyReflectsWALLatch(t *testing.T) {
	var nilManager *Manager
	if nilManager.Healthy() {
		t.Fatal("nil manager reports healthy")
	}
	m, err := OpenManager(t.TempDir(), testWALOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	if _, err := m.AcceptProduce(ctx, "orders", "k", 0, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if !m.Healthy() {
		t.Fatal("Healthy() = false before any failure")
	}

	inj := faulttest.New(t)
	inj.FailNth(syncfile.OpSyncData, ".wal", 1, syscall.EIO)
	if _, err := m.AcceptProduce(ctx, "orders", "k", 0, []byte("fails")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("accept during the sync failure = %v, want EIO", err)
	}
	if m.Healthy() {
		t.Fatal("Healthy() = true after a WAL sync failure latched the log")
	}
	if _, err := m.AcceptProduce(ctx, "orders", "k", 0, []byte("after")); err == nil {
		t.Fatal("accept after the latch succeeded")
	}
	if m.Healthy() {
		t.Fatal("the latch cleared")
	}
}
