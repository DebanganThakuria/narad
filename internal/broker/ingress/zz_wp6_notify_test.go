package ingress

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// An accept that moves the durable frontier leaves one wakeup for the
// dispatcher, however many accepts land before it looks.
func TestZZWP6AcceptSignalsDurableAdvance(t *testing.T) {
	m, err := OpenManager(t.TempDir(), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	advanced := m.DurableProduceAdvanced()
	select {
	case <-advanced:
		t.Fatal("wakeup before any accept")
	default:
	}
	for range 3 {
		if _, err := m.AcceptProduce(context.Background(), "orders", "k", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-advanced:
	case <-time.After(time.Second):
		t.Fatal("no wakeup after an accept")
	}
	select {
	case <-advanced:
		t.Fatal("more than one wakeup pending")
	default:
	}
	var nilManager *Manager
	if nilManager.DurableProduceAdvanced() != nil {
		t.Fatal("a nil manager's channel must be nil")
	}
}
