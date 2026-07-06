package httpserver

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
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

func (f *fakeBroker) PurgeTopic(context.Context, string) error { return nil }

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
	return f.produceFn(ctx, topicName, key, payload)
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

func newTestSet(b broker.Broker) *handlers.Set {
	return handlers.New(handlers.Deps{
		Broker:         b,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
	})
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewSetsServerConfig(t *testing.T) {
	cfg := config.HTTPConfig{
		Addr:          ":0",
		ReadTimeout:   config.Duration(2 * time.Second),
		WriteTimeout:  config.Duration(3 * time.Second),
		IdleTimeout:   config.Duration(4 * time.Second),
		ShutdownGrace: config.Duration(5 * time.Second),
	}
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	srv := New(cfg, handler, newTestLogger())

	if srv.srv.Addr != cfg.Addr {
		t.Fatalf("server addr = %q, want %q", srv.srv.Addr, cfg.Addr)
	}
	if srv.srv.Handler == nil {
		t.Fatal("server handler was not set")
	}
	if srv.srv.ErrorLog == nil {
		t.Fatal("server error log was not set")
	}
	if srv.logger == nil {
		t.Fatal("server logger was not set")
	}
	if srv.cfg.Addr != cfg.Addr {
		t.Fatalf("stored cfg addr = %q, want %q", srv.cfg.Addr, cfg.Addr)
	}
	if srv.cfg.ShutdownGrace.D() != 5*time.Second {
		t.Fatalf("stored shutdown grace = %v, want %v", srv.cfg.ShutdownGrace.D(), 5*time.Second)
	}
	if srv.srv.ReadTimeout != 2*time.Second || srv.srv.WriteTimeout != 3*time.Second || srv.srv.IdleTimeout != 4*time.Second {
		t.Fatalf("server timeouts = read=%v write=%v idle=%v", srv.srv.ReadTimeout, srv.srv.WriteTimeout, srv.srv.IdleTimeout)
	}
}

func TestServerRunStartsAndShutsDown(t *testing.T) {
	cfg := config.HTTPConfig{
		Addr:          "127.0.0.1:0",
		ReadTimeout:   config.Duration(time.Second),
		WriteTimeout:  config.Duration(time.Second),
		IdleTimeout:   config.Duration(time.Second),
		ShutdownGrace: config.Duration(time.Second),
	}
	srv := New(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), newTestLogger())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRecoverMiddlewareReturnsInternalServerError(t *testing.T) {
	handler := Recover(newTestLogger())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("Recover() status = %d, want %d", res.Code, http.StatusInternalServerError)
	}
	if got := strings.TrimSpace(res.Body.String()); got != `{"error":"internal server panic"}` {
		t.Fatalf("Recover() body = %q", got)
	}
}

func TestNewRouterRecordsMetricsForPanickingHandler(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	router := NewRouter(newTestSet(&fakeBroker{listTopicsFn: func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
		panic("boom")
	}}), newTestLogger(), m, reg, nil)

	res := httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/topics", nil))

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("GET /v1/topics status = %d, want %d", res.Code, http.StatusInternalServerError)
	}
	got := testutil.ToFloat64(m.HTTPRequestsTotal.WithLabelValues("GET /v1/topics", http.MethodGet, "500"))
	if got != 1 {
		t.Fatalf("http_requests_total{500} = %v, want 1 (panicking requests must be recorded)", got)
	}
	if errs := testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("http", "5xx")); errs != 1 {
		t.Fatalf("errors_total{http,5xx} = %v, want 1", errs)
	}
}

func TestNewRouterServesHealthAndMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	router := NewRouter(newTestSet(&fakeBroker{}), newTestLogger(), m, reg, nil)

	res := httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", res.Code, http.StatusOK)
	}
	if res.Header().Get("X-Request-ID") != "" {
		t.Fatal("GET /healthz returned request id header")
	}

	res = httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestNewRouterWithoutMetricsReturnsNotFoundOnMetrics(t *testing.T) {
	router := NewRouter(newTestSet(&fakeBroker{}), newTestLogger(), nil, nil, nil)
	res := httptest.NewRecorder()

	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if res.Code != http.StatusNotFound {
		t.Fatalf("GET /metrics status = %d, want %d", res.Code, http.StatusNotFound)
	}
}
