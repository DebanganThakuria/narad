package replication

import (
	"context"
	"testing"
)

func TestLocalReplicateIsNoop(t *testing.T) {
	if err := NewLocal().Replicate(context.Background(), "orders", 1, 42, []byte("payload")); err != nil {
		t.Fatalf("Replicate() error = %v, want nil", err)
	}
}
