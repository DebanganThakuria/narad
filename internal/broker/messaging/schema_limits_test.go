package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// doublingChain is an acyclic chain of $defs applying the next level
// twice: 2^levels validation paths. Registration refuses it; one
// persisted before that is validated under the node's validation limit.
func doublingChain(levels int) string {
	var b strings.Builder
	b.WriteString(`{"$ref":"#/$defs/d0","$defs":{`)
	for i := 0; i < levels; i++ {
		fmt.Fprintf(&b, `"d%d":{"allOf":[{"$ref":"#/$defs/d%d"},{"$ref":"#/$defs/d%d"}]},`, i, i+1, i+1)
	}
	fmt.Fprintf(&b, `"d%d":{"type":"string"}}}`, levels)
	return b.String()
}

// TestProduceMapsSchemaLimitErrors checks how the produce path reports
// the validation bounds: a payload nested past the limit is an invalid
// payload (ErrInvalid, 400), while a validation that never ran because
// the node's validation slots stayed busy is not the payload's fault: it
// is ErrUnavailable (503, retry) and never ErrInvalid, and a request
// that ended while it waited is context.Canceled (499).
func TestProduceMapsSchemaLimitErrors(t *testing.T) {
	store := newTestStore(t)
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	reg := schema.NewJSONSchema()
	reg.SetValidationLimit(1, 30*time.Millisecond)
	e := NewEngine(store, reg, fixedPartitionManager{}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")

	ctx := context.Background()
	for _, name := range []string{"arrays", "legacy"} {
		if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PutSchema(ctx, "arrays", 1, []byte(`{"type":"array","items":{"$ref":"#"}}`)); err != nil {
		t.Fatal(err)
	}
	// Written straight to the metastore, as a schema registered before
	// the path count existed would be.
	if err := store.PutSchema(ctx, "legacy", 1, []byte(doublingChain(20))); err != nil {
		t.Fatal(err)
	}

	deep := []byte(strings.Repeat("[", 257) + strings.Repeat("]", 257))
	err := e.validateProducePayload(ctx, "arrays", deep)
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "deeper than 256") {
		t.Fatalf("payload nested 257 deep: err = %v, want an invalid-payload refusal naming the limit", err)
	}
	if err := e.validateProducePayload(ctx, "arrays", deep[1:len(deep)-1]); err != nil {
		t.Fatalf("payload nested 256 deep: %v", err)
	}

	large := []byte("[" + strings.TrimSuffix(strings.Repeat("[],", 8<<10), ",") + "]")
	if err := e.validateProducePayload(ctx, "arrays", large); err != nil {
		t.Fatalf("large payload with a free slot: %v", err)
	}

	// Occupy the only slot with a slow validation on the legacy schema,
	// then produce a large payload until it finds the slot taken.
	slow := make(chan error, 1)
	go func() { slow <- e.validateProducePayload(ctx, "legacy", []byte(`"x"`)) }()
	var busy error
	for deadline := time.Now().Add(20 * time.Second); busy == nil && time.Now().Before(deadline); {
		err := e.validateProducePayload(ctx, "arrays", large)
		switch {
		case err == nil:
			time.Sleep(time.Millisecond)
		case errors.Is(err, errs.ErrUnavailable):
			busy = err
		default:
			t.Fatalf("large payload while the legacy validation runs: %v", err)
		}
	}
	if busy == nil {
		t.Fatal("never saw the validation slot taken")
	}
	if errors.Is(busy, ErrInvalid) || !schema.IsCapacityError(busy) {
		t.Fatalf("busy node: err = %v, want a retryable 503 that does not blame the payload", busy)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	err = e.validateProducePayload(cancelled, "arrays", large)
	if err != nil && (errors.Is(err, ErrInvalid) || !errors.Is(err, context.Canceled)) {
		t.Fatalf("cancelled request: err = %v, want context.Canceled, not an invalid payload", err)
	}
	if err := <-slow; err != nil {
		t.Fatalf("legacy schema validation: %v", err)
	}
}
