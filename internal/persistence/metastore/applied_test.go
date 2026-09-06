package metastore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestAppliedIndexAdvancesAndWaitAppliedFollowsIt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	before := s.AppliedIndex()
	if before == 0 {
		t.Fatalf("AppliedIndex = 0 after the store elected itself and applied the probe topic")
	}
	if err := s.WaitApplied(ctx, before); err != nil {
		t.Fatalf("WaitApplied(current) = %v, want nil", err)
	}

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	after := s.AppliedIndex()
	if after <= before {
		t.Fatalf("AppliedIndex after a write = %d, want > %d", after, before)
	}
	if err := s.WaitApplied(ctx, after); err != nil {
		t.Fatalf("WaitApplied(after write) = %v, want nil", err)
	}

	// An index the log has not reached is bounded by the context.
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.WaitApplied(waitCtx, after+1_000_000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitApplied(far future) = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("WaitApplied(far future) took %s, want about the context deadline", time.Since(start))
	}
}
