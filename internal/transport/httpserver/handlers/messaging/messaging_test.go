package messaging

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	ackFn                     func(context.Context, string, consumer.Handle) error
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

func (f *fakeBroker) Ack(ctx context.Context, topicName string, handle consumer.Handle) error {
	return f.ackFn(ctx, topicName, handle)
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
	routeAckFn         func(context.Context, http.ResponseWriter, *http.Request, string, consumer.Handle) bool
	routeCreateTopicFn func(context.Context, http.ResponseWriter, *http.Request, []byte) bool
	routeAlterTopicFn  func(context.Context, http.ResponseWriter, *http.Request, string, []byte) bool
}

type readTrackingBody struct {
	read bool
}

func (b *readTrackingBody) Read([]byte) (int, error) {
	b.read = true
	return 0, io.EOF
}

func (*readTrackingBody) Close() error {
	return nil
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

func (f *fakeRouter) RouteAck(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, handle consumer.Handle) bool {
	if f.routeAckFn == nil {
		return false
	}
	return f.routeAckFn(ctx, w, r, topicName, handle)
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

func (f *fakeRouter) RouteCreateUser(context.Context, http.ResponseWriter, *http.Request, []byte) bool {
	return false
}

func (f *fakeRouter) RouteUpdateUser(context.Context, http.ResponseWriter, *http.Request, string, []byte) bool {
	return false
}

func (f *fakeRouter) RouteDeleteUser(context.Context, http.ResponseWriter, *http.Request, string) bool {
	return false
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

func TestProduceHandlerAcceptsRawBodyToBroker(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?key=customer-1", bytes.NewBufferString(`{"id":1}`))
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
		return ingress.AcceptedProduce{TargetPartition: 1, CreatedAtUnixMs: 123}, nil
	}}, &fakeRouter{routeProduceFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName, key string, body []byte) bool {
		routerCalled = topicName == "orders" && key == "customer-1" && string(body) == `{"id":1}`
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?key=customer-1", bytes.NewBufferString(`{"id":1}`))
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

func TestProduceHandlerAcceptsNonJSONRawBody(t *testing.T) {
	var gotPayload []byte
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, _ string, _ string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotPayload = append([]byte(nil), payload...)
		return ingress.AcceptedProduce{TargetPartition: 0, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?key=customer-1", bytes.NewBufferString(`not-json`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if string(gotPayload) != "not-json" {
		t.Fatalf("Produce() payload = %q, want raw body", string(gotPayload))
	}
}

func TestProduceHandlerDoesNotInterpretLegacyJSONEnvelope(t *testing.T) {
	var gotKey string
	var gotPayload []byte
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, _ string, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		gotPayload = append([]byte(nil), payload...)
		return ingress.AcceptedProduce{TargetPartition: 0, CreatedAtUnixMs: 123}, nil
	}}, nil)

	body := `{"key":"body-key","message":{"id":1}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(body))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if gotKey == "" || gotKey == "body-key" {
		t.Fatalf("Produce() key = %q, want generated key unrelated to body", gotKey)
	}
	if string(gotPayload) != body {
		t.Fatalf("Produce() payload = %q, want full raw body", string(gotPayload))
	}
}

func TestProduceHandlerRejectsEmptyBody(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestProduceHandlerRejectsInvalidPartitionQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?partition=bad", bytes.NewBufferString(`{"id":1}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestProduceHandlerPassesPinnedPartition(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?key=customer-1&partition=2", bytes.NewBufferString(`{"id":1}`))
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

func TestProduceHandlerGeneratesKeyWhenMissingOrEmpty(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"missing", "/v1/topics/orders/produce"},
		{"empty", "/v1/topics/orders/produce?key="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotKey string
			s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
				gotKey = key
				return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
			}}, nil)

			req := httptest.NewRequest(http.MethodPost, tt.url, bytes.NewBufferString(`{"id":1}`))
			req.SetPathValue("topic", "orders")
			res := httptest.NewRecorder()

			Produce(s).ServeHTTP(res, req)

			if res.Code != http.StatusAccepted {
				t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
			}
			if gotKey == "" {
				t.Fatal("Produce() did not generate a key")
			}
		})
	}
}

func TestProduceHandlerParsesEscapedKey(t *testing.T) {
	var gotKey string
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?key=customer%2D1%2Bnorth", bytes.NewBufferString(`{"id":1}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if gotKey != "customer-1+north" {
		t.Fatalf("Produce() key = %q, want customer-1+north", gotKey)
	}
}

func TestProduceHandlerIgnoresUnknownQueryParams(t *testing.T) {
	var gotPayload []byte
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotPayload = append([]byte(nil), payload...)
		return ingress.AcceptedProduce{Topic: topicName, TargetPartition: 2, CreatedAtUnixMs: 123}, nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?ignored=true&key=customer-1&also_ignored=1", bytes.NewBufferString(`{"id":1}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Produce(s).ServeHTTP(res, req)

	if res.Code != http.StatusAccepted {
		t.Fatalf("Produce() status = %d, want %d", res.Code, http.StatusAccepted)
	}
	if string(gotPayload) != `{"id":1}` {
		t.Fatalf("Produce() payload = %q, want raw body", string(gotPayload))
	}
}

func TestProduceHandlerAcceptsWithGeneratedKeyAndDoesNotRoute(t *testing.T) {
	routerCalled := false
	var gotKey string
	s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, _ string, key string, _ []byte, _ ...int) (ingress.AcceptedProduce, error) {
		gotKey = key
		return ingress.AcceptedProduce{TargetPartition: 0, CreatedAtUnixMs: 123}, nil
	}}, &fakeRouter{routeProduceFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName, key string, body []byte) bool {
		routerCalled = true
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		if key == "" {
			t.Fatal("router key is empty")
		}
		if string(body) != `{"id":1}` {
			t.Fatalf("forwarded body = %q, want raw body", string(body))
		}
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))
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
		return ingress.AcceptedProduce{TargetPartition: 0, CreatedAtUnixMs: 123}, nil
	}}, &fakeRouter{routeProduceFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName, key string, body []byte) bool {
		routerCalled = true
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		if key != "customer-1" {
			t.Fatalf("key = %q, want customer-1", key)
		}
		if string(body) != `{"id":1}` {
			t.Fatalf("forwarded body = %q, want raw body", string(body))
		}
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce?key=customer-1", bytes.NewBufferString(`{"id":1}`))
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

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))
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
		return topic.Message{Partition: localPartition, Offset: 1, Payload: []byte(`{"id":1}`), ReceiptHandle: "h1"}, true, nil
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
		return topic.Message{Partition: localPartition, Offset: 1, Payload: []byte(`{"id":1}`), ReceiptHandle: "h1"}, true, nil
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
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 1, Offset: 2, Nonce: 3})
	wantHandle := consumer.Handle{Partition: 1, Offset: 2, Nonce: 3}
	var gotTopic string
	var gotHandle consumer.Handle
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, topicName string, h consumer.Handle) error {
		gotTopic = topicName
		gotHandle = h
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(handle), nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotTopic != "orders" || gotHandle != wantHandle {
		t.Fatalf("Ack() broker args = topic=%q handle=%+v", gotTopic, gotHandle)
	}
}

func TestAckHandlerRoutesWithDecodedPartition(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 2, Offset: 4, Nonce: 9})
	wantHandle := consumer.Handle{Partition: 2, Offset: 4, Nonce: 9}
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error {
		brokerCalled = true
		return nil
	}}, &fakeRouter{routeAckFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string, h consumer.Handle) bool {
		routerCalled = topicName == "orders" && h == wantHandle
		w.WriteHeader(http.StatusAccepted)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(handle), nil)
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

func TestAckHandlerRejectsUndecodableHandleBeforeRouting(t *testing.T) {
	brokerCalled := false
	routerCalled := false
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error {
		brokerCalled = true
		return nil
	}}, &fakeRouter{routeAckFn: func(context.Context, http.ResponseWriter, *http.Request, string, consumer.Handle) bool {
		routerCalled = true
		return false
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?receipt_handle=bad-handle", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
	if routerCalled {
		t.Fatal("Ack() called router for undecodable handle")
	}
	if brokerCalled {
		t.Fatal("Ack() called broker for undecodable handle")
	}
}

func TestAckHandlerRejectsMissingHandle(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestAckHandlerIgnoresExtraQueryParams(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 1, Nonce: 2})
	wantHandle := consumer.Handle{Partition: 0, Offset: 1, Nonce: 2}
	var gotHandle consumer.Handle
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		gotHandle = h
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?ignored=1&receipt_handle="+url.QueryEscape(handle)+"&extra=2", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotHandle != wantHandle {
		t.Fatalf("Ack() handle = %+v, want %+v", gotHandle, wantHandle)
	}
}

func TestAckHandlerDoesNotReadBody(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 1, Nonce: 2})
	body := &readTrackingBody{}
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error {
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(handle), body)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if body.read {
		t.Fatal("Ack() read request body")
	}
}

func TestAckHandlerParsesEscapedReceiptHandle(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 1, Nonce: 2})
	wantHandle := consumer.Handle{Partition: 0, Offset: 1, Nonce: 2}
	var gotHandle consumer.Handle
	s := newTestSet(&fakeBroker{ackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		gotHandle = h
		return nil
	}}, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(handle), nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if gotHandle != wantHandle {
		t.Fatalf("Ack() handle = %+v, want %+v", gotHandle, wantHandle)
	}
}

func TestAckHandlerMapsBrokerError(t *testing.T) {
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error {
		return errs.ErrHandleStale
	}}, nil)

	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 1, Nonce: 2})
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(handle), nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()

	Ack(s).ServeHTTP(res, req)

	if res.Code != http.StatusGone {
		t.Fatalf("Ack() status = %d, want %d", res.Code, http.StatusGone)
	}
}

func TestProduceQueryFromRawQuery(t *testing.T) {
	tests := []struct {
		name             string
		rawQuery         string
		wantKey          string
		wantPartition    int
		wantHasPartition bool
		wantErr          bool
	}{
		{name: "empty"},
		{name: "key only", rawQuery: "key=customer-1", wantKey: "customer-1"},
		{name: "empty key", rawQuery: "key=", wantKey: ""},
		{name: "key without value", rawQuery: "key", wantKey: ""},
		{name: "escaped key value", rawQuery: "key=customer%2D1%2Bnorth", wantKey: "customer-1+north"},
		{name: "encoded key name", rawQuery: "k%65y=customer-1", wantKey: "customer-1"},
		{name: "partition only", rawQuery: "partition=2", wantPartition: 2, wantHasPartition: true},
		{name: "key and partition", rawQuery: "ignored=1&key=customer-1&partition=3", wantKey: "customer-1", wantPartition: 3, wantHasPartition: true},
		{name: "duplicate key", rawQuery: "key=a&key=b", wantErr: true},
		{name: "duplicate partition", rawQuery: "partition=1&partition=2", wantErr: true},
		{name: "empty partition", rawQuery: "partition=", wantErr: true},
		{name: "partition without value", rawQuery: "partition", wantErr: true},
		{name: "negative partition", rawQuery: "partition=-1", wantErr: true},
		{name: "bad partition", rawQuery: "partition=bad", wantErr: true},
		{name: "overflow partition", rawQuery: "partition=999999999999999999999999", wantErr: true},
		{name: "bad key escape", rawQuery: "key=bad%ZZ", wantErr: true},
		{name: "bad partition escape", rawQuery: "partition=%ZZ", wantErr: true},
		{name: "bad encoded parameter name", rawQuery: "k%ZZy=value", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := produceQueryFromRawQuery(tt.rawQuery)
			if tt.wantErr {
				if err == nil {
					t.Fatal("produceQueryFromRawQuery() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("produceQueryFromRawQuery() error = %v", err)
			}
			if got.key != tt.wantKey {
				t.Fatalf("key = %q, want %q", got.key, tt.wantKey)
			}
			if got.hasPartition != tt.wantHasPartition {
				t.Fatalf("hasPartition = %v, want %v", got.hasPartition, tt.wantHasPartition)
			}
			if got.partition != tt.wantPartition {
				t.Fatalf("partition = %d, want %d", got.partition, tt.wantPartition)
			}
		})
	}
}

func TestReceiptHandleFromRawQueryFieldCases(t *testing.T) {
	tests := []struct {
		name       string
		rawQuery   string
		wantHandle string
		wantFound  bool
		wantErr    bool
	}{
		{"simple", "receipt_handle=1:2:3", "1:2:3", true, false},
		{"escaped value", "receipt_handle=1%3A2%3A3", "1:2:3", true, false},
		{"unknown ignored", "ignored=true&receipt_handle=1:2:3", "1:2:3", true, false},
		{"empty handle", "receipt_handle=", "", true, false},
		{"missing handle", "ignored=true", "", false, false},
		{"encoded key", "receipt%5Fhandle=1:2:3", "1:2:3", true, false},
		{"bad escape in key", "receipt%ZZhandle=1:2:3", "", false, true},
		{"bad escape in value", "receipt_handle=orders%ZZ1", "", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found, err := receiptHandleFromRawQuery(tt.rawQuery)
			if tt.wantErr {
				if err == nil {
					t.Fatal("receiptHandleFromRawQuery() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("receiptHandleFromRawQuery() error = %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("found = %v, want %v", found, tt.wantFound)
			}
			if got != tt.wantHandle {
				t.Fatalf("receipt handle = %q, want %q", got, tt.wantHandle)
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

func (f *fakeBroker) AttachChild(context.Context, string, string) error    { return nil }
func (f *fakeBroker) DetachChild(context.Context, string, string) error    { return nil }
func (f *fakeBroker) ChildrenOf(context.Context, string) ([]string, error) { return nil, nil }

func (f *fakeBroker) ReadFanoutSlab(context.Context, string, int, int64, int, int64, time.Duration) (topic.FanoutSlab, error) {
	return topic.FanoutSlab{}, nil
}

func (f *fakeBroker) FanoutCursorStats(context.Context, string) ([]topic.FanoutCursorStat, error) {
	return nil, nil
}

func (f *fakeRouter) RouteAttachChild(context.Context, http.ResponseWriter, *http.Request, string, string) bool {
	return false
}

func (f *fakeRouter) RouteDetachChild(context.Context, http.ResponseWriter, *http.Request, string, string) bool {
	return false
}

func (f *fakeRouter) CollectFanoutCursors(_ context.Context, _ string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool) {
	return local, true
}
