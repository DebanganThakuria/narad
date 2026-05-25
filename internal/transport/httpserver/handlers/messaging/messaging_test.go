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
	getTopicFn                func(context.Context, string) (topic.Topic, error)
	getTopicDetailsFn         func(context.Context, string) (topic.Details, error)
	listTopicsFn              func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error)
	produceFn                 func(context.Context, string, string, []byte) (int64, int, error)
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

func (f *fakeBroker) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	return f.getTopicFn(ctx, name)
}

func (f *fakeBroker) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	return f.getTopicDetailsFn(ctx, name)
}

func (f *fakeBroker) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return f.listTopicsFn(ctx, opts)
}

func (f *fakeBroker) Produce(ctx context.Context, topicName, key string, payload []byte) (int64, int, error) {
	return f.produceFn(ctx, topicName, key, payload)
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
	routeConsumeFn     func(context.Context, http.ResponseWriter, *http.Request, string, *int) bool
	routeAckFn         func(context.Context, http.ResponseWriter, *http.Request, string, int, []byte) bool
	routeCreateTopicFn func(context.Context, http.ResponseWriter, *http.Request, []byte) bool
}

func (f *fakeRouter) RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool {
	if f.routeProduceFn == nil {
		return false
	}
	return f.routeProduceFn(ctx, w, r, topicName, key, body)
}

func (f *fakeRouter) RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) bool {
	if f.routeConsumeFn == nil {
		return false
	}
	return f.routeConsumeFn(ctx, w, r, topicName, pinnedPartition)
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

func newTestSet(b broker.Broker, router handlers.Router) *handlers.Set {
	return handlers.New(handlers.Deps{
		Broker:         b,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
		Router:         router,
	})
}

func TestProduceHandlerCallsBroker(t *testing.T) {
	var gotTopic, gotKey string
	var gotPayload []byte
	s := newTestSet(&fakeBroker{produceFn: func(_ context.Context, topicName, key string, payload []byte) (int64, int, error) {
		gotTopic = topicName
		gotKey = key
		gotPayload = append([]byte(nil), payload...)
		return 7, 2, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"key":"customer-1","message":{"id":1}}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusOK)
	}
	if gotTopic != "orders" || gotKey != "customer-1" || string(gotPayload) != `{"id":1}` {
		t.Fatalf("Produce() broker args = topic=%q key=%q payload=%q", gotTopic, gotKey, string(gotPayload))
	}

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if body["offset"].(float64) != 7 || body["partition"].(float64) != 2 {
		t.Fatalf("Produce() body = %+v", body)
	}
}

func TestProduceHandlerRoutesBeforeBroker(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{produceFn: func(context.Context, string, string, []byte) (int64, int, error) {
		brokerCalled = true
		return 0, 0, nil
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
	if !routerCalled {
		t.Fatal("Produce() did not call router")
	}
	if brokerCalled {
		t.Fatal("Produce() called broker after routing")
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

func TestProduceHandlerMapsBrokerError(t *testing.T) {
	s := newTestSet(&fakeBroker{produceFn: func(context.Context, string, string, []byte) (int64, int, error) {
		return 0, 0, errs.ErrTopicNotFound
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

func TestConsumeHandlerRoutesBeforeBroker(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		brokerCalled = true
		return topic.Message{}, false, nil
	}}, &fakeRouter{routeConsumeFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string, pinnedPartition *int) bool {
		routerCalled = topicName == "orders" && pinnedPartition != nil && *pinnedPartition == 2
		w.WriteHeader(http.StatusAccepted)
		return true
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

func TestParseConsumeQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/?partition=1&offset=4&wait=5s", nil)
	got, ok := parseConsumeQuery(s, res, req)
	if !ok {
		t.Fatal("parseConsumeQuery() ok = false, want true")
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
	got, ok = parseConsumeQuery(s, res, req)
	if !ok || got.Wait != 0 {
		t.Fatalf("parseConsumeQuery() negative wait = %v, %v; want 0, true", got.Wait, ok)
	}

	res = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/?offset=bad", nil)
	if _, ok := parseConsumeQuery(s, res, req); ok {
		t.Fatal("parseConsumeQuery() ok = true, want false")
	}
}
