package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type fakeBroker struct {
	createTopicFn             func(context.Context, brokertopics.CreateOpts) (topic.Topic, error)
	increaseTopicPartitionsFn func(context.Context, string, int) (topic.Topic, error)
	updateTopicRetentionFn    func(context.Context, string, int64) (topic.Topic, error)
	updateTopicCapsFn         func(context.Context, string, int64, int64) (topic.Topic, error)
	updateTopicSchemaFn       func(context.Context, string, []byte) (topic.Topic, error)
	deleteTopicFn             func(context.Context, string) error
	purgeTopicFn              func(context.Context, string) error
	getTopicFn                func(context.Context, string) (topic.Topic, error)
	getTopicDetailsFn         func(context.Context, string) (topic.Details, error)
	listTopicsFn              func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error)
	produceFn                 func(context.Context, string, string, []byte) (int64, int, error)
	producePartitions         []int
	acceptProduceFn           func(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error)
	acceptProducePartitions   []int
	consumeFn                 func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error)
	ackFn                     func(context.Context, string, string) error
	readyFn                   func(context.Context) error
}

func (f *fakeBroker) CreateTopic(ctx context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
	return f.createTopicFn(ctx, opts)
}

func (f *fakeBroker) IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error) {
	return f.increaseTopicPartitionsFn(ctx, name, newPartitions)
}

func (f *fakeBroker) UpdateTopicRetention(ctx context.Context, name string, retentionMs int64) (topic.Topic, error) {
	return f.updateTopicRetentionFn(ctx, name, retentionMs)
}

func (f *fakeBroker) UpdateTopicCaps(ctx context.Context, name string, maxInFlightPerPartition, maxAckedAheadPerPartition int64) (topic.Topic, error) {
	return f.updateTopicCapsFn(ctx, name, maxInFlightPerPartition, maxAckedAheadPerPartition)
}

func (f *fakeBroker) UpdateTopicSchema(ctx context.Context, name string, schema []byte) (topic.Topic, error) {
	return f.updateTopicSchemaFn(ctx, name, schema)
}

func (f *fakeBroker) DeleteTopic(ctx context.Context, name string) error {
	return f.deleteTopicFn(ctx, name)
}

func (f *fakeBroker) PurgeTopic(ctx context.Context, name string) error {
	if f.purgeTopicFn == nil {
		return nil
	}
	return f.purgeTopicFn(ctx, name)
}

func (f *fakeBroker) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	return f.getTopicFn(ctx, name)
}

func (f *fakeBroker) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	return f.getTopicDetailsFn(ctx, name)
}

func (f *fakeBroker) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return f.listTopicsFn(ctx, opts)
}

func (f *fakeBroker) Produce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (int64, int, error) {
	f.producePartitions = append([]int(nil), partition...)
	return f.produceFn(ctx, topicName, key, payload)
}

func (f *fakeBroker) AcceptProduce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (ingress.AcceptedProduce, error) {
	f.acceptProducePartitions = append([]int(nil), partition...)
	if f.acceptProduceFn != nil {
		return f.acceptProduceFn(ctx, topicName, key, payload, partition...)
	}
	return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 0, CreatedAtUnixMs: 123}, nil
}

func (f *fakeBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	return 0, nil
}

func (f *fakeBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	return make([]int64, len(records)), nil
}

func (f *fakeBroker) Consume(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return f.consumeFn(ctx, topicName, opts)
}

func (f *fakeBroker) Ack(ctx context.Context, topicName string, receiptHandle string) error {
	return f.ackFn(ctx, topicName, receiptHandle)
}
func (f *fakeBroker) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }
func (f *fakeBroker) Ready(ctx context.Context) error {
	if f.readyFn == nil {
		return nil
	}
	return f.readyFn(ctx)
}
func (f *fakeBroker) Close() error { return nil }

type fakeRouter struct {
	routeProduceFn     func(context.Context, http.ResponseWriter, *http.Request, string, string, []byte) bool
	routeConsumeFn     func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int)
	routeConsumeRemote func(context.Context, http.ResponseWriter, *http.Request, string) (bool, bool)
	routeAckFn         func(context.Context, http.ResponseWriter, *http.Request, string, int, []byte) bool
	routeCreateTopicFn func(context.Context, http.ResponseWriter, *http.Request, []byte) bool
	routeAlterTopicFn  func(context.Context, http.ResponseWriter, *http.Request, string, []byte) bool
}

func (f *fakeRouter) RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool {
	if f.routeProduceFn == nil {
		return false
	}
	return f.routeProduceFn(ctx, w, r, topicName, key, body)
}

func (f *fakeRouter) RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) (bool, *int) {
	if f.routeConsumeFn == nil {
		return false, nil
	}
	return f.routeConsumeFn(ctx, w, r, topicName, pinnedPartition)
}

func (f *fakeRouter) RouteConsumeRemote(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) (bool, bool) {
	if f.routeConsumeRemote == nil {
		return false, false
	}
	return f.routeConsumeRemote(ctx, w, r, topicName)
}

func (f *fakeRouter) RouteAck(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, partition int, body []byte) bool {
	if f.routeAckFn == nil {
		return false
	}
	return f.routeAckFn(ctx, w, r, topicName, partition, body)
}

func (f *fakeRouter) RouteCreateTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool {
	if f.routeCreateTopicFn == nil {
		return false
	}
	return f.routeCreateTopicFn(ctx, w, r, body)
}

func (f *fakeRouter) RouteAlterTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, body []byte) bool {
	if f.routeAlterTopicFn == nil {
		return false
	}
	return f.routeAlterTopicFn(ctx, w, r, topicName, body)
}

func (f *fakeRouter) RouteDeleteTopic(context.Context, http.ResponseWriter, *http.Request, string) bool {
	return false
}

func (f *fakeRouter) BroadcastDeleteTopic(context.Context, string) error {
	return nil
}

func (f *fakeRouter) RouteGetTopic(context.Context, *http.Request, string, topic.Details) (topic.Details, error) {
	return topic.Details{}, nil
}

func newTestSet(b broker.Broker, router handlers.Router) *handlers.Set {
	return handlers.New(handlers.Deps{
		Broker:         b,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
		Router:         router,
	})
}

func TestProduceHandlerAcceptsToBroker(t *testing.T) {
	var gotTopic, gotKey string
	var gotPayload []byte
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotTopic = topicName
		gotKey = key
		gotPayload = append([]byte(nil), payload...)
		return ingress.AcceptedProduce{
			Topic:           topicName,
			TargetPartition: 2,
			CreatedAtUnixMs: 123,
		}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"key":"customer-1","message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if gotTopic != "orders" || gotKey != "customer-1" || string(gotPayload) != `{"id":1}` {
		t.Fatalf("Produce() broker args = topic=%q key=%q payload=%q", gotTopic, gotKey, string(gotPayload))
	}
	if res.Body.Len() != 0 {
		t.Fatalf("Produce() body = %q, want empty", res.Body.String())
	}
	if got := res.Header().Get("Content-Type"); got != "" {
		t.Fatalf("Produce() Content-Type = %q, want empty", got)
	}
}

func TestProduceHandlerDoesNotRouteBeforeAccept(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{acceptProduceFn: func(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
		brokerCalled = true
		return ingress.AcceptedProduce{Topic: "orders", TargetPartition: 1, CreatedAtUnixMs: 123}, nil
	}}, &fakeRouter{routeProduceFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName, key string, body []byte) bool {
		routerCalled = topicName == "orders" && key == "customer-1" && string(body) == `{"key":"customer-1","message":{"id":1}}`
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"key":"customer-1","message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if routerCalled {
		t.Fatal("Produce() called router")
	}
	if !brokerCalled {
		t.Fatal("Produce() did not call broker")
	}
}

func TestProduceHandlerRejectsInvalidRequest(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestProduceHandlerRejectsInvalidPartitionQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?partition=bad", bytes.NewBufferString(`{"message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestProduceHandlerSkipsRouterForPinnedPartition(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	broker := &fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		brokerCalled = topicName == "orders" && key == "customer-1" && string(payload) == `{"id":1}`
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}
	s := newTestSet(broker, &fakeRouter{routeProduceFn: func(context.Context, http.ResponseWriter, *http.Request, string, string, []byte) bool {
		routerCalled = true
		return false
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?partition=2", bytes.NewBufferString(`{"key":"customer-1","message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if routerCalled {
		t.Fatal("Produce() called router for pinned partition")
	}
	if !brokerCalled {
		t.Fatal("Produce() did not call broker for pinned partition")
	}
	if len(broker.acceptProducePartitions) != 1 || broker.acceptProducePartitions[0] != 2 {
		t.Fatalf("Produce() pinned partitions = %v, want [2]", broker.acceptProducePartitions)
	}
}

func TestProduceHandlerGeneratesKeyWhenMissing(t *testing.T) {
	var gotKey string
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if gotKey == "" {
		t.Fatal("Produce() did not generate a key")
	}
}

func TestProduceHandlerIgnoresUnknownFields(t *testing.T) {
	var gotPayload []byte
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotPayload = append([]byte(nil), payload...)
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"ignored":true,"key":"customer-1","message":{"id":1},"also_ignored":{"nested":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if string(gotPayload) != `{"id":1}` {
		t.Fatalf("Produce() payload = %q, want raw message", string(gotPayload))
	}
}

func TestProduceHandlerTreatsNullKeyAsMissing(t *testing.T) {
	var gotKey string
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, _ []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"key":null,"message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if gotKey == "" {
		t.Fatal("Produce() did not generate a key")
	}
}

func TestProduceHandlerPreservesStringMessagePayload(t *testing.T) {
	var gotPayload []byte
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, _ string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotPayload = append([]byte(nil), payload...)
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":"hello\nworld"}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if string(gotPayload) != `"hello\nworld"` {
		t.Fatalf("Produce() payload = %q, want quoted string payload", string(gotPayload))
	}
}

func TestProduceHandlerAcceptsWithGeneratedKeyAndDoesNotRoute(t *testing.T) {
	routerCalled := false
	var gotKey string
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, _ string, key string, _ []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		return ingress.AcceptedProduce{Topic: "orders", TargetPartition: 0, CreatedAtUnixMs: 123}, nil
	}}, &fakeRouter{routeProduceFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName, key string, body []byte) bool {
		routerCalled = true
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		if key == "" {
			t.Fatal("router key is empty")
		}

		var req produceRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		if req.Key != key {
			t.Fatalf("forwarded body key = %q, want %q", req.Key, key)
		}
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if routerCalled {
		t.Fatal("Produce() called router")
	}
	if gotKey == "" {
		t.Fatal("Produce() did not generate a key")
	}
}

func TestProduceHandlerPreservesExplicitKeyWhenAccepting(t *testing.T) {
	routerCalled := false
	var gotKey string
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, _ string, key string, _ []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		return ingress.AcceptedProduce{Topic: "orders", TargetPartition: 0, CreatedAtUnixMs: 123}, nil
	}}, &fakeRouter{routeProduceFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName, key string, body []byte) bool {
		routerCalled = true
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		if key != "customer-1" {
			t.Fatalf("key = %q, want customer-1", key)
		}
		if string(body) != `{"key":"customer-1","message":{"id":1}}` {
			t.Fatalf("forwarded body = %s", body)
		}
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"key":"customer-1","message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if routerCalled {
		t.Fatal("Produce() called router")
	}
	if gotKey != "customer-1" {
		t.Fatalf("key = %q, want customer-1", gotKey)
	}
}

func TestProduceHandlerMapsBrokerError(t *testing.T) {
	s := newTestSet(&fakeBroker{acceptProduceFn: func(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
		return ingress.AcceptedProduce{}, errs.ErrTopicNotFound
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusNotFound {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusNotFound)
	}
}

func TestConsumeHandlerCallsBrokerWithParsedQuery(t *testing.T) {
	var gotTopic string
	var gotOpts brokermsg.ConsumeOpts
	s := newTestSet(&fakeBroker{consumeFn: func(_ context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		gotTopic = topicName
		gotOpts = opts
		return topic.Message{Topic: topicName, Partition: 1, Offset: 5, Payload: []byte(`{"id":1}`), ReceiptHandle: "h1"}, true, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?partition=1&offset=5&wait=3s", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusOK)
	}
	if gotTopic != "orders" {
		t.Fatalf("Consume() topic = %q, want orders", gotTopic)
	}
	if gotOpts.Partition == nil || *gotOpts.Partition != 1 {
		t.Fatalf("Consume() partition = %+v, want 1", gotOpts.Partition)
	}
	if gotOpts.Offset == nil || *gotOpts.Offset != 5 {
		t.Fatalf("Consume() offset = %+v, want 5", gotOpts.Offset)
	}
	if gotOpts.Wait != time.Second {
		t.Fatalf("Consume() wait = %v, want %v", gotOpts.Wait, time.Second)
	}
}

func TestConsumeHandlerReturnsNoContentWhenNotFound(t *testing.T) {
	s := newTestSet(&fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		return topic.Message{}, false, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestConsumeHandlerTreatsQueueNotOwnerAsNoContent(t *testing.T) {
	s := newTestSet(&fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		return topic.Message{}, false, brokermsg.ErrNotPartitionOwner
	}}, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestConsumeHandlerRoutesBeforeBroker(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		brokerCalled = true
		return topic.Message{}, false, nil
	}}, &fakeRouter{routeConsumeFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string, pinnedPartition *int) (bool, *int) {
		routerCalled = topicName == "orders" && pinnedPartition != nil && *pinnedPartition == 2
		w.WriteHeader(http.StatusAccepted)
		return true, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?partition=2", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if !routerCalled {
		t.Fatal("Consume() did not call router")
	}
	if brokerCalled {
		t.Fatal("Consume() called broker after routing")
	}
}

func TestConsumeHandlerUsesRouterSelectedLocalPartition(t *testing.T) {
	var gotOpts brokermsg.ConsumeOpts
	localPartition := 3
	s := newTestSet(&fakeBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		gotOpts = opts
		return topic.Message{Topic: "orders", Partition: localPartition, Offset: 1, Payload: []byte(`{"id":1}`), ReceiptHandle: "h1"}, true, nil
	}}, &fakeRouter{routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
		return false, &localPartition
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusOK)
	}
	if gotOpts.Partition == nil || *gotOpts.Partition != localPartition {
		t.Fatalf("Consume() partition = %+v, want %d", gotOpts.Partition, localPartition)
	}
}

func TestConsumeHandlerFallsBackToRemoteWhenLocalPartitionIsEmpty(t *testing.T) {
	localPartition := 3
	var waits []time.Duration
	var partitions []int
	s := newTestSet(&fakeBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		waits = append(waits, opts.Wait)
		if opts.Partition == nil {
			partitions = append(partitions, -1)
		} else {
			partitions = append(partitions, *opts.Partition)
		}
		return topic.Message{}, false, nil
	}}, &fakeRouter{
		routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
			return false, &localPartition
		},
		routeConsumeRemote: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string) (bool, bool) {
			if topicName != "orders" {
				t.Fatalf("topic = %q, want orders", topicName)
			}
			w.WriteHeader(http.StatusAccepted)
			return true, true
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if len(waits) != 2 || waits[0] != 0 || waits[1] != 0 {
		t.Fatalf("local consume waits = %v, want [0s 0s]", waits)
	}
	if len(partitions) != 2 || partitions[0] != localPartition || partitions[1] != -1 {
		t.Fatalf("local consume partitions = %v, want [%d -1]", partitions, localPartition)
	}
}

func TestConsumeHandlerLongPollsLocalAfterRemoteEmpty(t *testing.T) {
	localPartition := 3
	var waits []time.Duration
	var partitions []int
	s := newTestSet(&fakeBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		waits = append(waits, opts.Wait)
		if opts.Partition == nil {
			partitions = append(partitions, -1)
		} else {
			partitions = append(partitions, *opts.Partition)
		}
		if len(waits) < 3 {
			return topic.Message{}, false, nil
		}
		return topic.Message{Topic: "orders", Partition: localPartition, Offset: 1, Payload: []byte(`{"id":1}`), ReceiptHandle: "h1"}, true, nil
	}}, &fakeRouter{
		routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
			return false, &localPartition
		},
		routeConsumeRemote: func(context.Context, http.ResponseWriter, *http.Request, string) (bool, bool) {
			return false, true
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusOK)
	}
	if len(waits) != 3 || waits[0] != 0 || waits[1] != 0 || waits[2] != time.Second {
		t.Fatalf("consume waits = %v, want [0s 0s 1s]", waits)
	}
	if len(partitions) != 3 || partitions[0] != localPartition || partitions[1] != -1 || partitions[2] != -1 {
		t.Fatalf("consume partitions = %v, want [%d -1 -1]", partitions, localPartition)
	}
}

func TestConsumeHandlerLocalOnlyProbeDoesNotRoute(t *testing.T) {
	var gotOpts brokermsg.ConsumeOpts
	routerCalled := false
	s := newTestSet(&fakeBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		gotOpts = opts
		return topic.Message{}, false, nil
	}}, &fakeRouter{routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
		routerCalled = true
		return false, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?local_only=1&wait=1s", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if routerCalled {
		t.Fatal("local-only consume called router")
	}
	if gotOpts.Partition != nil {
		t.Fatalf("Consume() partition = %+v, want nil", gotOpts.Partition)
	}
	if gotOpts.Wait != 0 {
		t.Fatalf("Consume() wait = %v, want 0", gotOpts.Wait)
	}
}

func TestConsumeHandlerLocalOnlyProbeTreatsNotOwnerAsEmpty(t *testing.T) {
	s := newTestSet(&fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		return topic.Message{}, false, brokermsg.ErrNotPartitionOwner
	}}, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?local_only=1", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestConsumeHandlerRejectsInvalidQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?partition=bad", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Consume(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Consume() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestAckHandlerCallsBroker(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Topic: "orders", Partition: 1, Offset: 2, Nonce: 3})
	var gotTopic, gotHandle string
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, topicName string, receiptHandle string) error {
		gotTopic = topicName
		gotHandle = receiptHandle
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"`+handle+`"}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotTopic != "orders" || gotHandle != handle {
		t.Fatalf("Ack() broker args = topic=%q handle=%q", gotTopic, gotHandle)
	}
}

func TestAckHandlerRoutesWithDecodedPartition(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Topic: "orders", Partition: 2, Offset: 4, Nonce: 9})
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, string) error {
		brokerCalled = true
		return nil
	}}, &fakeRouter{routeAckFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string, partition int, body []byte) bool {
		routerCalled = topicName == "orders" && partition == 2 && string(body) == `{"receipt_handle":"`+handle+`"}`
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"`+handle+`"}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if !routerCalled {
		t.Fatal("Ack() did not call router")
	}
	if brokerCalled {
		t.Fatal("Ack() called broker after routing")
	}
}

func TestAckHandlerIgnoresUndecodableHandleForRouting(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, string) error {
		brokerCalled = true
		return nil
	}}, &fakeRouter{routeAckFn: func(context.Context, http.ResponseWriter, *http.Request, string, int, []byte) bool {
		routerCalled = true
		return false
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"bad-handle"}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if routerCalled {
		t.Fatal("Ack() called router for undecodable handle")
	}
	if !brokerCalled {
		t.Fatal("Ack() did not call broker")
	}
}

func TestAckHandlerRejectsMissingHandle(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestAckHandlerIgnoresUnknownField(t *testing.T) {
	var gotHandle string
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, _ string, receiptHandle string) error {
		gotHandle = receiptHandle
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"x","extra":1}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotHandle != "x" {
		t.Fatalf("Ack() handle = %q, want x", gotHandle)
	}
}

func TestAckHandlerIgnoresUnusedTrailingJSON(t *testing.T) {
	var gotHandle string
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, _ string, receiptHandle string) error {
		gotHandle = receiptHandle
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"x"} {}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotHandle != "x" {
		t.Fatalf("Ack() handle = %q, want x", gotHandle)
	}
}

func TestAckHandlerParsesEscapedReceiptHandle(t *testing.T) {
	var gotHandle string
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, _ string, receiptHandle string) error {
		gotHandle = receiptHandle
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"abc\u002d123"}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotHandle != "abc-123" {
		t.Fatalf("Ack() handle = %q, want abc-123", gotHandle)
	}
}

func TestAckHandlerMapsBrokerError(t *testing.T) {
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, string) error {
		return errs.ErrHandleStale
	}}, nil)

	handle := consumer.EncodeHandle(consumer.Handle{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2})
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"`+handle+`"}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusGone {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusGone)
	}
}

func TestProduceRequestValidate(t *testing.T) {
	if err := (produceRequest{}).Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
	if err := (produceRequest{Message: json.RawMessage(`{"id":1}`)}).Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
	if err := (produceRequest{Message: json.RawMessage(`{"id":`)}).Validate(); err == nil {
		t.Fatal("Validate() error = nil, want invalid JSON error")
	}
}

func TestParseProduceRequestExtractsMessageValues(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantKey     string
		wantMessage string
	}{
		{"object", `{"key":"k1","message":{"id":1},"ignored":true}`, "k1", `{"id":1}`},
		{"array", `{"message":[1,{"ok":true}]}`, "", `[1,{"ok":true}]`},
		{"string", `{"message":"hello\nworld"}`, "", `"hello\nworld"`},
		{"number", `{"message":123.45}`, "", `123.45`},
		{"boolean", `{"message":true}`, "", `true`},
		{"null message", `{"message":null}`, "", `null`},
		{"null key", `{"key":null,"message":{"id":1}}`, "", `{"id":1}`},
		{"escaped key", `{"key":"cust\u002d1","message":{"id":1}}`, "cust-1", `{"id":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProduceRequest([]byte(tt.body))
			if err != nil {
				t.Fatalf("parseProduceRequest() error = %v", err)
			}
			if got.Key != tt.wantKey {
				t.Fatalf("key = %q, want %q", got.Key, tt.wantKey)
			}
			if string(got.Message) != tt.wantMessage {
				t.Fatalf("message = %q, want %q", string(got.Message), tt.wantMessage)
			}
		})
	}
}

func TestParseProduceRequestFieldErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"bad key type", `{"key":123,"message":{"id":1}}`},
		{"malformed message", `{"message":{"id":1`},
		{"malformed key string", `{"key":"bad\uZZZZ","message":{"id":1}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseProduceRequest([]byte(tt.body)); err == nil {
				t.Fatal("parseProduceRequest() error = nil, want error")
			}
		})
	}
}

func TestParseProduceRequestMissingMessage(t *testing.T) {
	got, err := parseProduceRequest([]byte(`{"key":"k1","ignored":true}`))
	if err != nil {
		t.Fatalf("parseProduceRequest() error = %v", err)
	}
	if got.Key != "k1" {
		t.Fatalf("key = %q, want k1", got.Key)
	}
	if len(got.Message) != 0 {
		t.Fatalf("message = %q, want empty", string(got.Message))
	}
}

func TestParseAckRequestFieldCases(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantHandle string
		wantErr    bool
	}{
		{"simple", `{"receipt_handle":"h1"}`, "h1", false},
		{"escaped", `{"receipt_handle":"abc\u002d123"}`, "abc-123", false},
		{"unknown ignored", `{"receipt_handle":"h1","ignored":true}`, "h1", false},
		{"null handle", `{"receipt_handle":null}`, "", false},
		{"missing handle", `{"ignored":true}`, "", false},
		{"bad handle type", `{"receipt_handle":123}`, "", true},
		{"bad handle string", `{"receipt_handle":"bad\uZZZZ"}`, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAckRequest([]byte(tt.body))
			if tt.wantErr {
				if err == nil {
					t.Fatal("parseAckRequest() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAckRequest() error = %v", err)
			}
			if got.ReceiptHandle != tt.wantHandle {
				t.Fatalf("receipt handle = %q, want %q", got.ReceiptHandle, tt.wantHandle)
			}
		})
	}
}

func TestParseConsumeQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/?partition=1&offset=4&wait=5s&local_only=1", nil)
	got, localOnly, ok := parseConsumeQuery(s, res, req)
	if !ok {
		t.Fatal("parseConsumeQuery() ok = false, want true")
	}
	if !localOnly {
		t.Fatal("parseConsumeQuery() localOnly = false, want true")
	}
	if got.Partition == nil || *got.Partition != 1 {
		t.Fatalf("parseConsumeQuery() partition = %+v", got.Partition)
	}
	if got.Offset == nil || *got.Offset != 4 {
		t.Fatalf("parseConsumeQuery() offset = %+v", got.Offset)
	}
	if got.Wait != time.Second {
		t.Fatalf("parseConsumeQuery() wait = %v, want %v", got.Wait, time.Second)
	}

	res = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/?wait=-2s", nil)
	got, localOnly, ok = parseConsumeQuery(s, res, req)
	if !ok || got.Wait != 0 {
		t.Fatalf("parseConsumeQuery() negative wait = %v, %v; want 0, true", got.Wait, ok)
	}
	if localOnly {
		t.Fatal("parseConsumeQuery() localOnly = true, want false")
	}

	res = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/?offset=bad", nil)
	if _, _, ok := parseConsumeQuery(s, res, req); ok {
		t.Fatal("parseConsumeQuery() ok = true, want false")
	}
}
