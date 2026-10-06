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
	"slices"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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

// refusingCreateBroker refuses every create with err.
type refusingCreateBroker struct {
	broker.Broker
	err error
}

func (b *refusingCreateBroker) CreateTopic(context.Context, brokertopics.CreateOpts) (topic.Topic, error) {
	return topic.Topic{}, b.err
}

// A create the leader refuses because every live member is being
// decommissioned reaches the forwarding follower, and its client, as a
// 503 that says why and what to do, not a 500 "create topic failed".
func TestRPCCreateTopicRefusedWhileEveryMemberDrainsIs503(t *testing.T) {
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: []byte(`{"name":"orders","partitions":3}`)})
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	s := NewRPCServer(&refusingCreateBroker{err: metastore.ErrAllMembersDraining}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	res := s.handleCreateTopic(payload)
	if res.Status != http.StatusServiceUnavailable || !strings.Contains(string(res.Body), "abort a decommission or add a node") {
		t.Fatalf("status = %d, body = %s; want 503 saying to abort a decommission or add a node", res.Status, res.Body)
	}
}

// retentionBroker records the retention a forwarded create or alter
// hands the broker.
type retentionBroker struct {
	broker.Broker
	created  []int64
	altered  []int64
	topicFor string
}

func (b *retentionBroker) CreateTopic(_ context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
	b.created = append(b.created, opts.RetentionMs)
	return topic.Topic{Name: opts.Name}, nil
}

func (b *retentionBroker) UpdateTopicRetention(_ context.Context, name string, retentionMs int64) (topic.Topic, error) {
	b.altered = append(b.altered, retentionMs)
	return topic.Topic{Name: name}, nil
}

// A 3.0.x follower forwards retention_ms 0 both for an absent field and
// for an explicit 0, which meant the default in that release, so the
// leader keeps reading 0 as the default; only the -1 marker a 3.1.0
// ingress sends for an explicit 0 means keep forever. Master refused -1
// on a forwarded alter (400).
func TestLeaderReadsZeroFromAnOlderForwarderAsDefault(t *testing.T) {
	br := &retentionBroker{}
	s := NewRPCServer(br, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, body := range []string{`{"name":"orders","retention_ms":0}`, `{"name":"orders","retention_ms":-1}`} {
		payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Body: []byte(body)})
		if err != nil {
			t.Fatal(err)
		}
		if res := s.handleCreateTopic(payload); res.Status != http.StatusCreated {
			t.Fatalf("forwarded create %s: status %d body %s", body, res.Status, res.Body)
		}
	}
	for _, body := range []string{`{"retention_ms":0}`, `{"retention_ms":-1}`} {
		payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: []byte(body)})
		if err != nil {
			t.Fatal(err)
		}
		if res := s.handleAlterTopic(payload); res.Status != http.StatusOK {
			t.Fatalf("forwarded alter %s: status %d body %s", body, res.Status, res.Body)
		}
	}
	want := []int64{0, topic.RetentionKeepForever}
	if !slices.Equal(br.created, want) || !slices.Equal(br.altered, want) {
		t.Fatalf("broker retentions: create %v, alter %v; want %v for each (0 = default, -1 = keep forever)", br.created, br.altered, want)
	}

	// Anything below the marker is still refused.
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: []byte(`{"retention_ms":-2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if res := s.handleAlterTopic(payload); res.Status != http.StatusBadRequest {
		t.Fatalf("forwarded alter with retention_ms -2: status %d, want 400", res.Status)
	}
}
