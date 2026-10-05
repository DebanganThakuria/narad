package schema

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
)

// largeArray is a valid payload for {"type":"array"} just above the
// size at which validation takes a slot of the node's limit.
func largeArray() []byte {
	n := limitedPayloadBytes/2 + 16
	return []byte("[" + strings.TrimSuffix(strings.Repeat("1,", n), ",") + "]")
}

// TestValidationLimitRefusesWhenSaturated covers the per-node
// validation limit: a large payload waits for a slot and is refused
// with a retryable 503-class error when none frees up in time, a
// cancelled request is not reported as busy, a small payload on an
// ordinary schema never waits, and any payload on a schema the cost
// analysis flagged (one persisted before the registration checks) does.
func TestValidationLimitRefusesWhenSaturated(t *testing.T) {
	ctx := context.Background()
	r := NewJSONSchema()
	r.SetValidationLimit(1, 150*time.Millisecond)
	if err := r.Load(ctx, "plain", 1, []byte(`{"type":"array"}`)); err != nil {
		t.Fatal(err)
	}
	// Persisted before the path count existed: 2^10 paths to the leaf.
	if err := r.Load(ctx, "dag", 1, []byte(dagSchema(10, "allOf"))); err != nil {
		t.Fatal(err)
	}
	large := largeArray()

	// A free slot: no waiting at all.
	if err := r.Validate(ctx, "plain", large); err != nil {
		t.Fatalf("large payload with a free slot: %v", err)
	}

	// Occupy the only slot.
	if err := r.limiter.acquire(ctx); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err := r.Validate(ctx, "plain", large)
	waited := time.Since(start)
	if !IsCapacityError(err) || !errors.Is(err, errs.ErrUnavailable) || errors.Is(err, context.Canceled) {
		t.Fatalf("large payload with no free slot: err = %v, want a capacity error that maps to 503", err)
	}
	if waited < 100*time.Millisecond {
		t.Errorf("gave up after %v; the wait is 150ms", waited)
	}
	if !strings.Contains(err.Error(), "capacity busy, retry") {
		t.Errorf("busy error text: %q", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	start = time.Now()
	err = r.Validate(cancelled, "plain", large)
	if !IsCapacityError(err) || !errors.Is(err, context.Canceled) || errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("cancelled request: err = %v, want a capacity error wrapping context.Canceled only", err)
	}
	if took := time.Since(start); took > 50*time.Millisecond {
		t.Errorf("a cancelled request waited %v", took)
	}

	deadline, cancelDeadline := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelDeadline()
	err = r.Validate(deadline, "plain", large)
	if !IsCapacityError(err) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("request deadline: err = %v, want a capacity error wrapping the deadline and ErrUnavailable", err)
	}

	// Small payloads on ordinary schemas never touch the limit.
	if err := r.Validate(ctx, "plain", []byte(`[1,2,3]`)); err != nil {
		t.Fatalf("small payload waited on the limit: %v", err)
	}
	// Any payload on a flagged schema does.
	if err := r.Validate(ctx, "dag", []byte(`"x"`)); !IsCapacityError(err) {
		t.Fatalf("small payload on a flagged schema: err = %v, want it to wait for a slot", err)
	}

	r.limiter.release()
	if err := r.Validate(ctx, "dag", []byte(`"x"`)); err != nil {
		t.Fatalf("flagged schema with a free slot: %v", err)
	}
	if err := r.Validate(ctx, "plain", large); err != nil {
		t.Fatalf("large payload after the slot was released: %v", err)
	}
	if got := len(r.limiter.slots); got != 0 {
		t.Fatalf("%d slots still held after every validation returned", got)
	}
}
