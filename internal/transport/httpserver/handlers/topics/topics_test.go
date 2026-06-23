package topics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/debanganthakuria/narad/internal/domain/topic"
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
}

type fakeRouter struct {
	routeCreateTopicFn     func(context.Context, http.ResponseWriter, *http.Request, []byte) bool
	routeAlterTopicFn      func(context.Context, http.ResponseWriter, *http.Request, string, []byte) bool
	routeDeleteTopicFn     func(context.Context, http.ResponseWriter, *http.Request, string) bool
	broadcastDeleteTopicFn func(context.Context, string) error
	routeGetTopicFn        func(context.Context, *http.Request, string, topic.Details) (topic.Details, error)
}

func (f *fakeRouter) RouteProduce(context.Context, http.ResponseWriter, *http.Request, string, string, []byte) bool {
	return false
}

func (f *fakeRouter) RouteConsume(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
	return false, nil
}

func (f *fakeRouter) RouteConsumeRemote(context.Context, http.ResponseWriter, *http.Request, string) (bool, bool) {
	return false, false
}

func (f *fakeRouter) RouteAck(context.Context, http.ResponseWriter, *http.Request, string, int, []byte) bool {
	return false
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

func (f *fakeRouter) RouteDeleteTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) bool {
	if f.routeDeleteTopicFn == nil {
		return false
	}
	return f.routeDeleteTopicFn(ctx, w, r, topicName)
}

func (f *fakeRouter) BroadcastDeleteTopic(ctx context.Context, topicName string) error {
	if f.broadcastDeleteTopicFn == nil {
		return nil
	}
	return f.broadcastDeleteTopicFn(ctx, topicName)
}

func (f *fakeRouter) RouteGetTopic(ctx context.Context, r *http.Request, topicName string, details topic.Details) (topic.Details, error) {
	if f.routeGetTopicFn == nil {
		return details, nil
	}
	return f.routeGetTopicFn(ctx, r, topicName, details)
}

func newTestSetWithRouter(b broker.Broker, router handlers.Router) *handlers.Set {
	return handlers.New(handlers.Deps{
		Broker:         b,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
		Router:         router,
	})
}

func newTestSet(b broker.Broker) *handlers.Set {
	return newTestSetWithRouter(b, nil)
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

func (f *fakeBroker) Produce(context.Context, string, string, []byte, ...int) (int64, int, error) {
	return 0, 0, errors.New("unexpected Produce call")
}

func (f *fakeBroker) AcceptProduce(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
	return ingress.AcceptedProduce{}, nil
}

func (f *fakeBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	return 0, nil
}

func (f *fakeBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	return make([]int64, len(records)), nil
}

func (f *fakeBroker) Consume(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return topic.Message{}, false, errors.New("unexpected Consume call")
}

func (f *fakeBroker) Ack(context.Context, string, string) error {
	return errors.New("unexpected Ack call")
}
func (f *fakeBroker) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }
func (f *fakeBroker) Ready(context.Context) error                               { return nil }
func (f *fakeBroker) Close() error                                              { return nil }

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, fmt.Errorf("boom")
}

func TestCreateHandler(t *testing.T) {
	var captured brokertopics.CreateOpts
	s := newTestSet(&fakeBroker{createTopicFn: func(_ context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
		captured = opts
		return topic.Topic{Name: opts.Name, Partitions: opts.Partitions}, nil
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":"orders","partitions":3,"schema":{"type":"object"}}`))
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)

	if res.Code != http.StatusCreated {
		t.Fatalf("Create() status = %d, want %d", res.Code, http.StatusCreated)
	}
	if captured.Name != "orders" || captured.Partitions != 3 {
		t.Fatalf("Create() broker opts = %+v", captured)
	}
	if string(captured.Schema) != `{"type":"object"}` {
		t.Fatalf("Create() schema = %q, want schema object", string(captured.Schema))
	}
}

func TestCreateHandlerRoutesToLeader(t *testing.T) {
	routed := false
	s := newTestSetWithRouter(&fakeBroker{createTopicFn: func(_ context.Context, _ brokertopics.CreateOpts) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected CreateTopic call")
	}}, &fakeRouter{routeCreateTopicFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, body []byte) bool {
		routed = true
		if string(body) != `{"name":"orders","partitions":3}` {
			t.Fatalf("forwarded body = %s", body)
		}
		w.WriteHeader(http.StatusCreated)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":"orders","partitions":3}`))
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)

	if !routed {
		t.Fatal("Create() did not route to leader")
	}
	if res.Code != http.StatusCreated {
		t.Fatalf("Create() status = %d, want %d", res.Code, http.StatusCreated)
	}
}

func TestCreateHandlerFallsBackToLocalWhenRouterDoesNotForward(t *testing.T) {
	called := false
	s := newTestSetWithRouter(&fakeBroker{createTopicFn: func(_ context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
		called = true
		return topic.Topic{Name: opts.Name, Partitions: opts.Partitions}, nil
	}}, &fakeRouter{routeCreateTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ []byte) bool {
		return false
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":"orders","partitions":3}`))
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)

	if !called {
		t.Fatal("Create() did not fall back to local broker")
	}
	if res.Code != http.StatusCreated {
		t.Fatalf("Create() status = %d, want %d", res.Code, http.StatusCreated)
	}
}

func TestCreateHandlerRejectsInvalidJSONBeforeRouting(t *testing.T) {
	routed := false
	s := newTestSetWithRouter(&fakeBroker{createTopicFn: func(_ context.Context, _ brokertopics.CreateOpts) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected CreateTopic call")
	}}, &fakeRouter{routeCreateTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ []byte) bool {
		routed = true
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":`))
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)

	if routed {
		t.Fatal("Create() routed invalid JSON")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("Create() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestCreateHandlerRejectsReadErrorsBeforeRouting(t *testing.T) {
	routed := false
	s := newTestSetWithRouter(&fakeBroker{createTopicFn: func(_ context.Context, _ brokertopics.CreateOpts) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected CreateTopic call")
	}}, &fakeRouter{routeCreateTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ []byte) bool {
		routed = true
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", io.NopCloser(errReader{}))
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)

	if routed {
		t.Fatal("Create() routed unreadable body")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("Create() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestGetHandler(t *testing.T) {
	s := newTestSet(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{Topic: topic.Topic{Name: name}}, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestGetHandlerUsesRouterMergedStats(t *testing.T) {
	routerCalled := false
	s := newTestSetWithRouter(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{
			Topic:      topic.Topic{Name: name, Partitions: 2},
			Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}, {Index: 1, NextOffset: 2}},
		}, nil
	}}, &fakeRouter{routeGetTopicFn: func(_ context.Context, _ *http.Request, topicName string, details topic.Details) (topic.Details, error) {
		routerCalled = true
		if topicName != "orders" {
			t.Fatalf("topicName = %q, want orders", topicName)
		}
		details.Partitions = []topic.PartitionStats{{Index: 0, NextOffset: 10}, {Index: 1, NextOffset: 20}}
		return details, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if !routerCalled {
		t.Fatal("Get() did not call router")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusOK)
	}
	var body topic.Details
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(body.Partitions) != 2 || body.Partitions[0].NextOffset != 10 || body.Partitions[1].NextOffset != 20 {
		t.Fatalf("Get() partitions = %+v", body.Partitions)
	}
}

func TestGetHandlerFallsBackToLocalWhenRouterNil(t *testing.T) {
	s := newTestSet(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{
			Topic:      topic.Topic{Name: name, Partitions: 1},
			Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 3}},
		}, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusOK)
	}
	var body topic.Details
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(body.Partitions) != 1 || body.Partitions[0].NextOffset != 3 {
		t.Fatalf("Get() partitions = %+v", body.Partitions)
	}
}

func TestGetHandlerPartitionQuerySkipsRouter(t *testing.T) {
	routerCalled := false
	s := newTestSetWithRouter(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{
			Topic:      topic.Topic{Name: name, Partitions: 2},
			Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}, {Index: 1, NextOffset: 2}},
		}, nil
	}}, &fakeRouter{routeGetTopicFn: func(_ context.Context, _ *http.Request, _ string, details topic.Details) (topic.Details, error) {
		routerCalled = true
		return details, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders?partition=1", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if routerCalled {
		t.Fatal("Get() routed partition-scoped request")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusOK)
	}
	var body topic.Details
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(body.Partitions) != 1 || body.Partitions[0].Index != 1 || body.Partitions[0].NextOffset != 2 {
		t.Fatalf("Get() partitions = %+v", body.Partitions)
	}
}

func TestGetHandlerRejectsInvalidPartitionQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{
			Topic:      topic.Topic{Name: name, Partitions: 1},
			Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}},
		}, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders?partition=bad", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestGetHandlerReturnsRouterError(t *testing.T) {
	s := newTestSetWithRouter(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{Topic: topic.Topic{Name: name, Partitions: 1}, Partitions: []topic.PartitionStats{{Index: 0}}}, nil
	}}, &fakeRouter{routeGetTopicFn: func(_ context.Context, _ *http.Request, _ string, _ topic.Details) (topic.Details, error) {
		return topic.Details{}, errors.New("boom")
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusInternalServerError)
	}
}

func TestGetHandlerRejectsOutOfRangePartitionQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{
			Topic:      topic.Topic{Name: name, Partitions: 1},
			Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}},
		}, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders?partition=1", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestGetHandlerPreservesTopicFieldsForPartitionQuery(t *testing.T) {
	s := newTestSet(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return topic.Details{
			Topic:      topic.Topic{Name: name, Partitions: 2},
			Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}, {Index: 1, NextOffset: 2}},
		}, nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders?partition=1", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusOK)
	}
	var body topic.Details
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if body.Name != "orders" {
		t.Fatalf("Get() topic name = %q, want orders", body.Name)
	}
	if len(body.Partitions) != 1 || body.Partitions[0].Index != 1 {
		t.Fatalf("Get() partitions = %+v", body.Partitions)
	}
}

func TestGetHandlerMissingTopicReturnsBadRequest(t *testing.T) {
	s := newTestSet(&fakeBroker{})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics/", nil)
	res := httptest.NewRecorder()
	Get(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Get() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestDeleteHandlerRequiresTopic(t *testing.T) {
	s := newTestSet(&fakeBroker{deleteTopicFn: func(context.Context, string) error { return nil }})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/", nil)
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestDeleteHandlerCallsBroker(t *testing.T) {
	called := false
	s := newTestSet(&fakeBroker{deleteTopicFn: func(_ context.Context, name string) error {
		called = name == "orders"
		return nil
	}})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if !called {
		t.Fatal("Delete() did not call broker with topic name")
	}
}

func TestDeleteHandlerRoutesToLeader(t *testing.T) {
	routed := false
	brokerCalled := false
	s := newTestSetWithRouter(&fakeBroker{deleteTopicFn: func(_ context.Context, _ string) error {
		brokerCalled = true
		return errors.New("unexpected DeleteTopic call")
	}}, &fakeRouter{routeDeleteTopicFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string) bool {
		routed = true
		if topicName != "orders" {
			t.Fatalf("topicName = %q, want orders", topicName)
		}
		w.WriteHeader(http.StatusNoContent)
		return true
	}})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if !routed {
		t.Fatal("Delete() did not route to leader")
	}
	if brokerCalled {
		t.Fatal("Delete() called broker after routing")
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestDeleteHandlerFallsBackToLocalWhenRouterDoesNotForward(t *testing.T) {
	called := false
	s := newTestSetWithRouter(&fakeBroker{deleteTopicFn: func(_ context.Context, name string) error {
		called = name == "orders"
		return nil
	}}, &fakeRouter{routeDeleteTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string) bool {
		return false
	}})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if !called {
		t.Fatal("Delete() did not fall back to local broker")
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestDeleteHandlerBroadcastsAfterLocalDelete(t *testing.T) {
	deleted := false
	broadcasted := false
	s := newTestSetWithRouter(&fakeBroker{deleteTopicFn: func(_ context.Context, name string) error {
		deleted = name == "orders"
		return nil
	}}, &fakeRouter{
		routeDeleteTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string) bool {
			return false
		},
		broadcastDeleteTopicFn: func(_ context.Context, topicName string) error {
			broadcasted = topicName == "orders"
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if !deleted {
		t.Fatal("Delete() did not delete topic locally before broadcast")
	}
	if !broadcasted {
		t.Fatal("Delete() did not broadcast remote purge after local delete")
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestDeleteHandlerBroadcastsAfterLocalPurgeFailure(t *testing.T) {
	broadcasted := false
	s := newTestSetWithRouter(&fakeBroker{deleteTopicFn: func(_ context.Context, name string) error {
		if name != "orders" {
			t.Fatalf("DeleteTopic() name = %q, want orders", name)
		}
		return brokertopics.PurgeError{Topic: name, Err: errors.New("disk remove failed")}
	}}, &fakeRouter{
		routeDeleteTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string) bool {
			return false
		},
		broadcastDeleteTopicFn: func(_ context.Context, topicName string) error {
			broadcasted = topicName == "orders"
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if !broadcasted {
		t.Fatal("Delete() did not broadcast remote purge after local purge failure")
	}
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusInternalServerError)
	}
}

func TestDeleteHandlerReturnsBroadcastError(t *testing.T) {
	s := newTestSetWithRouter(&fakeBroker{deleteTopicFn: func(_ context.Context, _ string) error {
		return nil
	}}, &fakeRouter{
		routeDeleteTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string) bool {
			return false
		},
		broadcastDeleteTopicFn: func(context.Context, string) error {
			return errors.New("boom")
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("Delete() status = %d, want %d", res.Code, http.StatusInternalServerError)
	}
}

func TestListHandlerParsesLimitAndToken(t *testing.T) {
	var gotOpts metastore.ListOptions
	s := newTestSet(&fakeBroker{listTopicsFn: func(_ context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
		gotOpts = opts
		return []topic.Topic{{Name: "orders"}}, "next", nil
	}})

	req := httptest.NewRequest(http.MethodGet, "/v1/topics?limit=7&page_token=abc", nil)
	res := httptest.NewRecorder()
	List(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("List() status = %d, want %d", res.Code, http.StatusOK)
	}
	if gotOpts.Limit != 7 || gotOpts.PageToken != "abc" {
		t.Fatalf("List() opts = %+v", gotOpts)
	}
}

func TestParseLimit(t *testing.T) {
	s := newTestSet(&fakeBroker{listTopicsFn: func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
		return nil, "", nil
	}})

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	if got, ok := parseLimit(s, res, req); !ok || got != defaultLimit {
		t.Fatalf("parseLimit() default = %d, %v; want %d, true", got, ok, defaultLimit)
	}

	res = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/topics?limit=2005", nil)
	if got, ok := parseLimit(s, res, req); !ok || got != maxLimit {
		t.Fatalf("parseLimit() clamped = %d, %v; want %d, true", got, ok, maxLimit)
	}

	res = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/topics?limit=bad", nil)
	if _, ok := parseLimit(s, res, req); ok {
		t.Fatal("parseLimit() returned ok for invalid limit")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("parseLimit() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestAlterRequestValidate(t *testing.T) {
	negative := int64(-1)
	validSchema := json.RawMessage(`{"type":"object"}`)
	invalidSchema := json.RawMessage(`{"type":`)

	cases := []struct {
		name    string
		req     alterRequest
		wantErr bool
	}{
		{name: "empty request invalid", req: alterRequest{}, wantErr: true},
		{name: "negative retention invalid", req: alterRequest{RetentionMs: &negative}, wantErr: true},
		{name: "negative in flight invalid", req: alterRequest{MaxInFlightPerPartition: &negative}, wantErr: true},
		{name: "negative acked ahead invalid", req: alterRequest{MaxAckedAheadPerPartition: &negative}, wantErr: true},
		{name: "invalid schema", req: alterRequest{Schema: invalidSchema}, wantErr: true},
		{name: "valid caps update", req: alterRequest{MaxInFlightPerPartition: new(int64(0))}, wantErr: false},
		{name: "valid schema", req: alterRequest{Schema: validSchema}, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if tc.wantErr && err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestAlterHandlerRoutesToLeader(t *testing.T) {
	routed := false
	s := newTestSetWithRouter(&fakeBroker{updateTopicRetentionFn: func(context.Context, string, int64) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected UpdateTopicRetention call")
	}}, &fakeRouter{routeAlterTopicFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, topicName string, body []byte) bool {
		routed = true
		if topicName != "orders" {
			t.Fatalf("topicName = %q, want orders", topicName)
		}
		if string(body) != `{"retention_ms":1}` {
			t.Fatalf("forwarded body = %s", body)
		}
		w.WriteHeader(http.StatusOK)
		return true
	}})

	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", bytes.NewBufferString(`{"retention_ms":1}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Alter(s).ServeHTTP(res, req)

	if !routed {
		t.Fatal("Alter() did not route to leader")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("Alter() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestAlterHandlerFallsBackToLocalWhenRouterDoesNotForward(t *testing.T) {
	called := false
	s := newTestSetWithRouter(&fakeBroker{updateTopicRetentionFn: func(_ context.Context, name string, retentionMs int64) (topic.Topic, error) {
		called = name == "orders" && retentionMs == 1
		return topic.Topic{Name: name}, nil
	}}, &fakeRouter{routeAlterTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string, _ []byte) bool {
		return false
	}})

	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", bytes.NewBufferString(`{"retention_ms":1}`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Alter(s).ServeHTTP(res, req)

	if !called {
		t.Fatal("Alter() did not fall back to local broker")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("Alter() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestAlterHandlerRejectsInvalidJSONBeforeRouting(t *testing.T) {
	routed := false
	s := newTestSetWithRouter(&fakeBroker{updateTopicRetentionFn: func(context.Context, string, int64) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected UpdateTopicRetention call")
	}}, &fakeRouter{routeAlterTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string, _ []byte) bool {
		routed = true
		return true
	}})

	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", bytes.NewBufferString(`{"retention_ms":`))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Alter(s).ServeHTTP(res, req)

	if routed {
		t.Fatal("Alter() routed invalid JSON")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("Alter() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestAlterHandlerRejectsReadErrorsBeforeRouting(t *testing.T) {
	routed := false
	s := newTestSetWithRouter(&fakeBroker{updateTopicRetentionFn: func(context.Context, string, int64) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected UpdateTopicRetention call")
	}}, &fakeRouter{routeAlterTopicFn: func(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ string, _ []byte) bool {
		routed = true
		return true
	}})

	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", io.NopCloser(errReader{}))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Alter(s).ServeHTTP(res, req)

	if routed {
		t.Fatal("Alter() routed unreadable body")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("Alter() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestAlterHandlerAppliesOperationsInOrder(t *testing.T) {
	calls := []string{}
	one := int64(1)
	two := int64(2)
	s := newTestSet(&fakeBroker{
		updateTopicRetentionFn: func(context.Context, string, int64) (topic.Topic, error) {
			calls = append(calls, "retention")
			return topic.Topic{Name: "orders", MaxInFlightPerPartition: 5, MaxAckedAheadPerPartition: 6}, nil
		},
		updateTopicCapsFn: func(context.Context, string, int64, int64) (topic.Topic, error) {
			calls = append(calls, "caps")
			return topic.Topic{Name: "orders", MaxInFlightPerPartition: 1, MaxAckedAheadPerPartition: 2}, nil
		},
		increaseTopicPartitionsFn: func(context.Context, string, int) (topic.Topic, error) {
			calls = append(calls, "partitions")
			return topic.Topic{Name: "orders", Partitions: 4}, nil
		},
		updateTopicSchemaFn: func(context.Context, string, []byte) (topic.Topic, error) {
			calls = append(calls, "schema")
			return topic.Topic{Name: "orders"}, nil
		},
		getTopicFn: func(context.Context, string) (topic.Topic, error) {
			calls = append(calls, "get")
			return topic.Topic{Name: "orders", MaxInFlightPerPartition: 5, MaxAckedAheadPerPartition: 6}, nil
		},
	})

	body := `{"retention_ms":1,"max_in_flight_per_partition":1,"max_acked_ahead_per_partition":2,"partitions":4,"schema":{"type":"object"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", bytes.NewBufferString(body))
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Alter(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Alter() status = %d, want %d", res.Code, http.StatusOK)
	}
	want := []string{"retention", "caps", "partitions", "schema"}
	if len(calls) != len(want) {
		t.Fatalf("Alter() calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("Alter() calls[%d] = %q, want %q (all=%v)", i, calls[i], want[i], calls)
		}
	}
	_ = one
	_ = two
}
