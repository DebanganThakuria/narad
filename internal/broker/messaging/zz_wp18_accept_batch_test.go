package messaging

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// zzWP18SchemaEngine is an engine over a real metastore and JSON Schema
// registry, with topic "orders" (3 partitions, qty must be a string)
// and an ingress WAL.
func zzWP18SchemaEngine(t *testing.T) (*Engine, *metastore.Store, *ingress.Manager) {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "orders-id-1", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSchema(ctx, "orders", 1, []byte(`{"type":"object","properties":{"qty":{"type":"string"}}}`)); err != nil {
		t.Fatal(err)
	}
	im := newTestIngressManager(t)
	e := newTestEngineWithIngress(t, t.TempDir(), store, schema.NewJSONSchema(), fixedPartitioner{picked: 1}, im, "node-self")
	return e, store, im
}

func zzWP18Replayed(t *testing.T, im *ingress.Manager) []ingress.ProduceRecord {
	t.Helper()
	var records []ingress.ProduceRecord
	if err := im.ReplayProduce(0, func(r ingress.ProduceRecord) error {
		records = append(records, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return records
}

// TestZZWP18AcceptProduceBatchAccepts checks a valid batch reaches the
// ingress WAL in order, each message validated against the schema,
// placed as a single produce would place it and stamped with the
// topic's incarnation.
func TestZZWP18AcceptProduceBatchAccepts(t *testing.T) {
	e, _, im := zzWP18SchemaEngine(t)
	msgs := []ProduceMessage{
		{Key: "a", Payload: []byte(`{"qty":"1"}`)},
		{Key: "a", Payload: []byte(`{"qty":"2"}`), Partition: 2, HasPartition: true},
		{Payload: []byte(`{"qty":"3"}`)},
	}
	accepted, err := e.AcceptProduceBatch(context.Background(), "orders", msgs)
	if err != nil {
		t.Fatalf("AcceptProduceBatch: %v", err)
	}
	wantParts := []int{1, 2, 1}
	records := zzWP18Replayed(t, im)
	if len(accepted) != 3 || len(records) != 3 {
		t.Fatalf("accepted %d, replayed %d, want 3 each", len(accepted), len(records))
	}
	for i, r := range records {
		if r.Key != msgs[i].Key || string(r.Payload) != string(msgs[i].Payload) ||
			r.TargetPartition != wantParts[i] || accepted[i].TargetPartition != wantParts[i] || r.TopicID != "orders-id-1" {
			t.Fatalf("record %d = %+v (receipt %+v)", i, r, accepted[i])
		}
	}
}

// TestZZWP18AcceptProduceBatchAllOrNothing checks that the first
// message a single produce would refuse fails the whole batch, with the
// error a single produce gets (so the same status) naming its index,
// and that nothing reaches the WAL.
func TestZZWP18AcceptProduceBatchAllOrNothing(t *testing.T) {
	e, store, im := zzWP18SchemaEngine(t)
	ctx := context.Background()
	good := ProduceMessage{Payload: []byte(`{"qty":"1"}`)}
	for _, tc := range []struct {
		name string
		msgs []ProduceMessage
		want string
	}{
		{"schema", []ProduceMessage{good, good, {Payload: []byte(`{"qty":1}`)}, {Payload: []byte(`{"qty":2}`)}}, "message 2: "},
		{"partition out of range", []ProduceMessage{good, {Payload: good.Payload, Partition: 3, HasPartition: true}}, "message 1: "},
		{"not json", []ProduceMessage{{Payload: []byte(`nope`)}}, "message 0: "},
	} {
		_, err := e.AcceptProduceBatch(ctx, "orders", tc.msgs)
		if !errors.Is(err, errs.ErrInvalidArgument) || !strings.HasPrefix(err.Error(), tc.want) {
			t.Fatalf("%s: error = %v, want invalid argument starting %q", tc.name, err, tc.want)
		}
	}
	if _, err := e.AcceptProduceBatch(ctx, "orders", nil); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("empty batch: error = %v, want invalid argument", err)
	}
	if _, err := e.AcceptProduceBatch(ctx, "missing", []ProduceMessage{good}); !errors.Is(err, errs.ErrTopicNotFound) {
		t.Fatalf("unknown topic: error = %v, want topic not found", err)
	}

	// A delayed fan-out child takes records only through fan-out.
	if err := store.CreateTopic(ctx, topic.Topic{Name: "late", Partitions: 1, Role: topic.RoleChild, Parent: "orders", FanoutDelayMs: 60_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.AcceptProduceBatch(ctx, "late", []ProduceMessage{good}); !errors.Is(err, errs.ErrDelayedChildProduce) {
		t.Fatalf("delayed child: error = %v, want %v", err, errs.ErrDelayedChildProduce)
	}

	if records := zzWP18Replayed(t, im); len(records) != 0 {
		t.Fatalf("refused batches left %d records in the WAL", len(records))
	}
}
