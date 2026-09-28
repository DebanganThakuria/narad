package messaging

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// wp4bDropRegistry drops the topic's schemas, the way a retire does
// (registry first, then the engine's caches), right before each of the
// first drops Validate calls.
type wp4bDropRegistry struct {
	*schema.JSONSchema
	e     *Engine
	drops atomic.Int64
}

func (r *wp4bDropRegistry) Validate(ctx context.Context, topicName string, payload []byte) error {
	if r.drops.Add(-1) >= 0 {
		_ = r.JSONSchema.DropTopic(ctx, topicName)
		r.e.ForgetTopic(topicName)
	}
	return r.JSONSchema.Validate(ctx, topicName, payload)
}

// TestWP4BRepeatedRegistryDropStillValidates: a purge of an old
// incarnation drops the registry's schemas twice (the retired hook,
// then the purge itself) while a same-named successor is live. A
// produce that saw the first drop reloaded once and then, meeting the
// second drop, let its payload through unvalidated.
func TestWP4BRepeatedRegistryDropStillValidates(t *testing.T) {
	store := wp4bNewStore(t)
	reg := &wp4bDropRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)
	reg.e = e
	ctx := context.Background()
	const name = "orders"
	wp4bSchemaTopic(t, store, name)
	if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
		t.Fatal(err)
	}

	reg.drops.Store(2)
	if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
		t.Fatal("payload invalid under the live schema accepted after two registry drops")
	}
	reg.drops.Store(2)
	if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
		t.Fatalf("valid payload rejected after two registry drops: %v", err)
	}
}

// TestWP4BForgetRaceKeepsValidating races lookups and produces on live
// topics against forgets of those topics (some after a registry drop,
// as a retire does) and of others, under -race. Every lookup must
// succeed and no payload the schema rejects may get through.
func TestWP4BForgetRaceKeepsValidating(t *testing.T) {
	store := wp4bNewStore(t)
	reg := &wp4bFenceRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	names := []string{"orders", "payments"}
	for _, name := range names {
		wp4bSchemaTopic(t, store, name)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 8 {
		name := names[i%len(names)]
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := e.getTopic(ctx, name); err != nil {
					t.Error(err)
					return
				}
				if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
					t.Error(err)
					return
				}
				if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
					t.Error("payload invalid under the schema accepted")
					return
				}
			}
		})
	}
	wg.Go(func() {
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			switch n % 3 {
			case 0:
				e.ForgetTopic(fmt.Sprintf("other-%d", n))
			case 1:
				e.ForgetTopic(names[n%len(names)])
			default:
				_ = reg.DropTopic(ctx, names[n%len(names)])
				e.ForgetTopic(names[n%len(names)])
			}
			time.Sleep(50 * time.Microsecond)
		}
	})
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	// With the traffic stopped, the next produce is validated.
	for _, name := range names {
		if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
			t.Fatalf("%s: payload invalid under the schema accepted after the race", name)
		}
	}
}
