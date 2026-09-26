package messaging

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// Engine.AcceptProduce stamps the incarnation of the topic the payload
// was validated against into the ingress record. Without it, a record
// still waiting in the WAL when its topic is deleted and recreated under
// the same name could only be resolved by name, into the new topic.
func TestZZWP5AcceptProduceStampsTopicID(t *testing.T) {
	engine, manager := zzWP5AcceptEngine(t, "f76e7d4b965dd285")
	if _, err := engine.AcceptProduce(context.Background(), "orders", "customer-1", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	var records []ingress.ProduceRecord
	if err := manager.ReplayProduce(0, func(r ingress.ProduceRecord) error {
		records = append(records, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Topic != "orders" || records[0].TopicID != "f76e7d4b965dd285" {
		t.Fatalf("replayed %+v, want one orders record stamped with ID f76e7d4b965dd285", records)
	}
}
