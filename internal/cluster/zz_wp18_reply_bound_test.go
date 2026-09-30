package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// zzWP18EscapedRecords are records whose JSON encoding is several times
// their raw size: text that is valid UTF-8 but not JSON goes out as a
// JSON string, where a control byte or one of < > & takes 6 bytes, and a
// key gets the same escaping. A batch bounded by raw bytes alone built a
// reply of these past the cluster RPC frame.
var zzWP18EscapedRecords = []struct {
	name    string
	key     string
	payload []byte
}{
	{"zero-filled payload", "", make([]byte, 64<<10)},
	{"markup payload", "", []byte(strings.Repeat("<a>&", 16<<10))},
	{"control-byte key", strings.Repeat("\x01", 32<<10), []byte(`{"k":"v"}`)},
}

// fillKeyed commits n records with key and payload to partition 0.
func (o *zzWP18Owner) fillKeyed(t *testing.T, n int, key string, payload []byte) {
	t.Helper()
	records := make([]ingress.ProduceRecord, n)
	for i := range records {
		records[i] = ingress.ProduceRecord{Topic: "orders", Key: key, Payload: payload}
	}
	if _, err := o.engine.CommitAcceptedProduceBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
}

// zzWP18CheckRecord fails unless m carries the key and payload produced.
func zzWP18CheckRecord(t *testing.T, m topic.Message, key string, payload []byte) {
	t.Helper()
	got := []byte(m.Payload)
	var text string
	if json.Unmarshal(m.Payload, &text) == nil {
		// Text that is not JSON comes back as a JSON string.
		got = []byte(text)
	}
	if m.Key != key || !bytes.Equal(got, payload) {
		t.Fatalf("record at offset %d came back with a %d-byte key and %d-byte payload, want the %d and %d produced", m.Offset, len(m.Key), len(got), len(key), len(payload))
	}
}

// TestZZWP18ServerBatchConsumeEncodedBound checks the owner bounds a
// forwarded batch's reply by its encoded size: whatever the records'
// escaping costs, the reply fits one cluster RPC frame, and the records
// reserved but left out of it are given back at once, so the next
// batches take them instead of finding them hidden for a lease.
func TestZZWP18ServerBatchConsumeEncodedBound(t *testing.T) {
	for _, tc := range zzWP18EscapedRecords {
		t.Run(tc.name, func(t *testing.T) {
			owner := newZZWP18Owner(t, 1)
			const n = 100
			owner.fillKeyed(t, n, tc.key, tc.payload)
			seen := map[int64]bool{}
			for round := 0; len(seen) < n; round++ {
				if round == n {
					t.Fatalf("after %d batches only %d of %d records were delivered", round, len(seen), n)
				}
				res := zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: n})
				if res.Status == http.StatusNoContent {
					t.Fatalf("batch %d found nothing with %d of %d records never delivered: they are hidden until their leases lapse", round, n-len(seen), n)
				}
				if len(res.Body) > clusterwire.MaxStreamFramePayloadBytes {
					t.Fatalf("batch %d reply is %d bytes, over the %d-byte cluster RPC frame", round, len(res.Body), clusterwire.MaxStreamFramePayloadBytes)
				}
				msgs := zzWP18Batch(t, res)
				if len(msgs) > 1 && len(res.Body) > forwardedConsumeReplyBytes {
					t.Fatalf("batch %d reply of %d records is %d bytes, over the %d-byte reply bound", round, len(msgs), len(res.Body), forwardedConsumeReplyBytes)
				}
				for _, m := range msgs {
					if seen[m.Offset] {
						t.Fatalf("offset %d delivered twice within its lease", m.Offset)
					}
					seen[m.Offset] = true
					zzWP18CheckRecord(t, m, tc.key, tc.payload)
				}
			}
		})
	}
}

// TestZZWP18NonOwnerBatchConsumeEscapedPayloads is a batch consume
// (GET /consume?max=100) through the HTTP handler on a node that owns
// none of the topic, forwarded over real QUIC, of records whose JSON
// encoding is six times their size. The owner's reply used to overrun
// the RPC frame: the write aborted the stream, the gateway answered 204
// and the reserved records stayed hidden for their visibility timeout,
// every time. Now every record must be delivered, each once and
// ackable, within a few requests. A request may still answer 204 when
// the owner's reply outlasts the gateway's 500 ms probe budget, which
// JSON-encoding about 8 MiB of escaped records under -race on a 2-core
// runner can: the gateway then cancels and the owner gives the records
// back at once, so they are delivered by a later request. Records left
// hidden (the old failure) would never be delivered and fail the test.
func TestZZWP18NonOwnerBatchConsumeEscapedPayloads(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	const n = 100
	payload := make([]byte, 64<<10)
	owner.fillKeyed(t, n, "", payload)
	addr, client := owner.serveQUIC(t)
	router := newZZWP18Gateway(t, addr, client, 1)
	set := handlers.New(handlers.Deps{
		Broker: zzWP18GatewayBroker{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Router: router,
	})
	seen := map[string]bool{}
	for attempt := 0; len(seen) < n; attempt++ {
		if attempt == 20 {
			t.Fatalf("after %d batch consumes only %d of %d records were delivered", attempt, len(seen), n)
		}
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/topics/orders/consume?max=%d", n), nil)
		req.SetPathValue("topic", "orders")
		rec := httptest.NewRecorder()
		httpmessaging.Consume(set)(rec, req)
		if rec.Code == http.StatusNoContent {
			continue // a reply that outlasted the probe budget; its records come back
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("batch consume %d on a non-owner = %d with %d of %d records never delivered, want 200", attempt, rec.Code, n-len(seen), n)
		}
		var reply struct {
			Messages []topic.Message `json:"messages"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &reply); err != nil {
			t.Fatalf("reply of %d bytes: %v", rec.Body.Len(), err)
		}
		for _, m := range reply.Messages {
			if seen[m.ReceiptHandle] {
				t.Fatalf("receipt handle %s delivered twice", m.ReceiptHandle)
			}
			seen[m.ReceiptHandle] = true
			zzWP18CheckRecord(t, m, "", payload)
			h, err := consumer.DecodeHandle(m.ReceiptHandle)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			if !router.RouteAck(context.Background(), w, nil, "orders", h) || w.Code != http.StatusNoContent {
				t.Fatalf("forwarded ack of %s = %d, want 204", m.ReceiptHandle, w.Code)
			}
		}
	}
}

// BenchmarkZZWP18ServerConsumeBatchReply is the owner's side of a
// forwarded batch consume without the transport: decode the request,
// take max records from a broker that always has them, and build the
// reply. ns/op is per request.
func BenchmarkZZWP18ServerConsumeBatchReply(b *testing.B) {
	msg := topic.Message{Topic: "orders", Partition: 0, Offset: 7, Key: "user-42", Payload: []byte(`{"k":"v","n":1234567,"tags":["a","b"]}`), Timestamp: 1_700_000_000_000, ReceiptHandle: "0:7:9"}
	s := NewRPCServer(zzWP18BatchBroker{&zzWP9Broker{msg: msg, found: true}}, nil, nil)
	// Each reply's delivery record has expired by the next request, so
	// the expiry queue stays one long and the loop times the reply alone.
	now := time.Unix(0, 0)
	s.now = func() time.Time {
		now = now.Add(time.Hour)
		return now
	}
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("max=%d", n), func(b *testing.B) {
			payload, err := nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: n})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if res := s.handleConsume(context.Background(), requestKey{stream: 1, request: 1}, payload); res.Status != http.StatusOK {
					b.Fatalf("status %d", res.Status)
				}
			}
		})
	}
}
