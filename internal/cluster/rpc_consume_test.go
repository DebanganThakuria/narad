package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type consumeOnlyBroker struct {
	broker.Broker
	consumeFn func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error)
}

func (b *consumeOnlyBroker) Consume(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return b.consumeFn(ctx, topicName, opts)
}

func encodeConsumeReq(t *testing.T, req nodewire.ConsumeRequest) []byte {
	t.Helper()
	payload, err := nodewire.EncodeConsumeRequest(req)
	if err != nil {
		t.Fatalf("EncodeConsumeRequest() error = %v", err)
	}
	return payload
}

func TestRPCServerLocalOnlyConsumeUsesUnpinnedLocalScan(t *testing.T) {
	br := &consumeOnlyBroker{consumeFn: func(_ context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		if opts.Partition != nil {
			t.Fatalf("partition = %d, want nil local scan", *opts.Partition)
		}
		if opts.Offset != nil {
			t.Fatalf("offset = %d, want nil", *opts.Offset)
		}
		if opts.Wait != 0 {
			t.Fatalf("wait = %s, want 0 for remote local-only probe", opts.Wait)
		}
		return topic.Message{
			Topic:         topicName,
			Partition:     1,
			Offset:        7,
			Payload:       []byte(`{"id":1}`),
			ReceiptHandle: "h",
		}, true, nil
	}}
	s := &RPCServer{broker: br}

	res := s.handleConsume(encodeConsumeReq(t, nodewire.ConsumeRequest{
		Topic:     "orders",
		LocalOnly: true,
	}))

	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Status, http.StatusOK)
	}
}

func TestRPCServerLocalOnlyConsumeTreatsNotOwnerAsEmpty(t *testing.T) {
	br := &consumeOnlyBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		return topic.Message{}, false, brokermsg.ErrNotPartitionOwner
	}}
	s := &RPCServer{broker: br}

	res := s.handleConsume(encodeConsumeReq(t, nodewire.ConsumeRequest{
		Topic:     "orders",
		LocalOnly: true,
		WaitNanos: int64(time.Second),
	}))

	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Status, http.StatusNoContent)
	}
}

// RPC frames carry no caller deadline and broker.Consume runs under a
// Background context here, so an unclamped wire-supplied wait would park
// this server for however long the peer asked (e.g. 24h). The handler must
// clamp it to the package's consume wait ceiling.
func TestRPCServerConsumeClampsExcessiveWait(t *testing.T) {
	var gotWait time.Duration
	br := &consumeOnlyBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		gotWait = opts.Wait
		return topic.Message{}, false, nil
	}}
	s := &RPCServer{broker: br}

	res := s.handleConsume(encodeConsumeReq(t, nodewire.ConsumeRequest{
		Topic:     "orders",
		WaitNanos: int64(24 * time.Hour),
	}))

	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Status, http.StatusNoContent)
	}
	if gotWait != defaultMaxConsumeWait {
		t.Fatalf("broker wait = %s, want clamped to %s", gotWait, defaultMaxConsumeWait)
	}
}

func TestRPCServerConsumeNormalizesNegativeWait(t *testing.T) {
	var gotWait time.Duration
	br := &consumeOnlyBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		gotWait = opts.Wait
		return topic.Message{}, false, nil
	}}
	s := &RPCServer{broker: br}

	res := s.handleConsume(encodeConsumeReq(t, nodewire.ConsumeRequest{
		Topic:     "orders",
		WaitNanos: -int64(time.Second),
	}))

	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Status, http.StatusNoContent)
	}
	if gotWait != 0 {
		t.Fatalf("broker wait = %s, want 0 for a negative wire wait", gotWait)
	}
}
