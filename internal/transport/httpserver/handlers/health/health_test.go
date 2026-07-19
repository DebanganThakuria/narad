package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
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
	return newTestSetWithShutdownCtx(b, nil)
}

func newTestSetWithShutdownCtx(b broker.Broker, shutdownCtx context.Context) *handlers.Set {
	return handlers.New(handlers.Deps{
		Broker:         b,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
		ShutdownCtx:    shutdownCtx,
	})
}

func TestHealthzHandler(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()

	Healthz(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Healthz() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestHealthzHandlerReturnsUnavailableWhenShutdownStarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := newTestSetWithShutdownCtx(&fakeBroker{}, ctx)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()

	Healthz(s).ServeHTTP(res, req)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("Healthz() status = %d, want %d", res.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthzHandlerUsesBackgroundWhenShutdownCtxMissing(t *testing.T) {
	s := newTestSetWithShutdownCtx(&fakeBroker{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()

	Healthz(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Healthz() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestReadyzHandlerReturnsReady(t *testing.T) {
	s := newTestSet(&fakeBroker{readyFn: func(context.Context) error { return nil }})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	res := httptest.NewRecorder()

	Readyz(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Readyz() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestReadyzHandlerReturnsUnavailable(t *testing.T) {
	s := newTestSet(&fakeBroker{readyFn: func(context.Context) error { return errors.New("not ready") }})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	res := httptest.NewRecorder()

	Readyz(s).ServeHTTP(res, req)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("Readyz() status = %d, want %d", res.Code, http.StatusServiceUnavailable)
	}
}

func (f *fakeBroker) AttachChild(context.Context, string, string, int64) error { return nil }
func (f *fakeBroker) DetachChild(context.Context, string, string) error        { return nil }

func (f *fakeBroker) ReadFanoutSlab(context.Context, string, int, topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	return topic.FanoutSlab{}, nil
}

func (f *fakeBroker) PartitionTransferInfo(context.Context, string, int) (messaging.PartitionTransferInfo, error) {
	return messaging.PartitionTransferInfo{}, nil
}

func (f *fakeBroker) ReadPartitionSegment(context.Context, string, int, int64, int64, int64) ([]byte, error) {
	return nil, nil
}

func (f *fakeBroker) FanoutCursorStats(context.Context, string) ([]topic.FanoutCursorStat, error) {
	return nil, nil
}

func (f *fakeBroker) ExtendAck(context.Context, string, consumer.Handle) error { return nil }

func (f *fakeBroker) Nack(context.Context, string, consumer.Handle) error { return nil }
