package cluster

// The create body crosses TWO decoders: the HTTP handler's createRequest
// on the ingress node, then — because create is forwarded to the Raft
// leader — rpcCreateTopicBody's STRICT decode here. A field added to the
// handler but not to rpcCreateTopicBody is invisible on a laptop
// (single-node paths never forward) and a guaranteed 400 on a real
// cluster: exactly how create-as-child's `parent` field shipped broken
// in the first v1.1.0 build. This test sends a body carrying every
// field the HTTP handler can emit and asserts each one survives the
// strict decode and reaches CreateOpts.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type createOnlyBroker struct {
	broker.Broker
	got brokertopics.CreateOpts
}

func (b *createOnlyBroker) CreateTopic(_ context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
	b.got = opts
	return topic.Topic{Name: opts.Name, Partitions: opts.Partitions}, nil
}

func TestRPCCreateTopicDecodesEveryHandlerField(t *testing.T) {
	// Keep in lockstep with createRequest in
	// internal/transport/httpserver/handlers/topics/create.go — this is
	// the byte shape the ingress node forwards.
	body := []byte(`{
		"name": "orders-replica",
		"partitions": 6,
		"retention_ms": 3600000,
		"visibility_timeout_ms": 30000,
		"max_in_flight_per_partition": 64,
		"max_acked_ahead_per_partition": 256,
		"schema": {"type": "object"},
		"parent": "orders",
		"fanout_delay_ms": 60000,
		"owner": "svc-user"
	}`)
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Topic: "orders-replica", Body: body})
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}

	br := &createOnlyBroker{}
	s := NewRPCServer(br, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	res := s.handleCreateTopic(payload)
	if res.Status != http.StatusCreated {
		t.Fatalf("status = %d, body = %s; want 201 — a strict-decode 400 here means rpcCreateTopicBody is missing a handler field", res.Status, res.Body)
	}

	got := br.got
	if got.Name != "orders-replica" || got.Partitions != 6 || got.RetentionMs != 3_600_000 ||
		got.VisibilityTimeoutMs != 30_000 || got.MaxInFlightPerPartition != 64 ||
		got.MaxAckedAheadPerPartition != 256 || string(got.Schema) != `{"type": "object"}` ||
		got.Parent != "orders" || got.FanoutDelayMs != 60_000 || got.Owner != "svc-user" {
		t.Fatalf("CreateOpts = %+v; a field was dropped between the wire body and the broker", got)
	}
}
