package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
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

func newTestSet(b broker.Broker) *Set {
	logs := runtime.NewLogs("", storage.DefaultOptions(), nil, nil)
	return New(Deps{
		Broker:         b,
		Logs:           logs,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
	})
}

func TestNewPanicsOnMissingDeps(t *testing.T) {
	t.Run("missing broker", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("New() did not panic for missing broker")
			}
		}()
		New(Deps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	})

	t.Run("missing logger", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("New() did not panic for missing logger")
			}
		}()
		New(Deps{Broker: &fakeBroker{}})
	})
}

func TestWriteJSONAndWriteError(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	res := httptest.NewRecorder()

	s.WriteJSON(res, http.StatusCreated, map[string]string{"status": "ok"})
	if res.Code != http.StatusCreated {
		t.Fatalf("WriteJSON() status = %d, want %d", res.Code, http.StatusCreated)
	}
	if got := res.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("WriteJSON() content type = %q, want application/json", got)
	}

	res = httptest.NewRecorder()
	s.WriteError(res, http.StatusBadRequest, "bad request")
	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if body["error"] != "bad request" {
		t.Fatalf("WriteError() body = %+v", body)
	}
}

func TestWriteErrorLogsServerErrorsOnly(t *testing.T) {
	var buf bytes.Buffer
	s := New(Deps{
		Broker: &fakeBroker{},
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})

	s.WriteError(httptest.NewRecorder(), http.StatusBadRequest, "bad request")
	if got := buf.String(); got != "" {
		t.Fatalf("4xx log output = %q, want empty", got)
	}

	s.WriteError(httptest.NewRecorder(), http.StatusInternalServerError, "internal failure")
	got := buf.String()
	if !strings.Contains(got, "http server error") ||
		!strings.Contains(got, "status=500") ||
		!strings.Contains(got, "error=\"internal failure\"") {
		t.Fatalf("5xx log output = %q, want server error log", got)
	}
}

func TestWriteBrokerErrorLogsUnderlyingInternalError(t *testing.T) {
	var buf bytes.Buffer
	s := New(Deps{
		Broker: &fakeBroker{},
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})

	res := httptest.NewRecorder()
	s.WriteBrokerError(res, "produce", errors.New("boom"))
	if res.Code != http.StatusInternalServerError {
		t.Fatalf("WriteBrokerError() status = %d, want %d", res.Code, http.StatusInternalServerError)
	}

	got := buf.String()
	if !strings.Contains(got, "http server error") ||
		!strings.Contains(got, "op=produce") ||
		!strings.Contains(got, "err=boom") {
		t.Fatalf("broker 5xx log output = %q, want underlying error", got)
	}
}

type validatedReq struct {
	Name string `json:"name"`
}

func (r *validatedReq) Validate() error {
	if r.Name == "" {
		return errors.New("name required")
	}
	return nil
}

func TestDecodeJSONAndDecodeAndValidate(t *testing.T) {
	type req struct {
		Name string `json:"name"`
	}

	s := newTestSet(&fakeBroker{})

	res := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"name":"orders","extra":1}`))
	var decoded req
	if s.DecodeJSON(res, httpReq, &decoded) {
		t.Fatal("DecodeJSON() returned true for unknown field")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("DecodeJSON() status = %d, want %d", res.Code, http.StatusBadRequest)
	}

	res = httptest.NewRecorder()
	httpReq = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"name":"orders"} {"name":"payments"}`))
	decoded = req{}
	if s.DecodeJSON(res, httpReq, &decoded) {
		t.Fatal("DecodeJSON() returned true for multiple JSON values")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("DecodeJSON() status = %d, want %d", res.Code, http.StatusBadRequest)
	}

	res = httptest.NewRecorder()
	httpReq = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"name":""}`))
	var validated validatedReq
	if s.DecodeAndValidate(res, httpReq, &validated) {
		t.Fatal("DecodeAndValidate() returned true for invalid payload")
	}
	if res.Code != http.StatusBadRequest {
		t.Fatalf("DecodeAndValidate() status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestWriteBrokerErrorMapsStatuses(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	cases := []struct {
		name   string
		err    error
		status int
	}{
		{"not found", errs.ErrTopicNotFound, http.StatusNotFound},
		{"already exists", errs.ErrTopicAlreadyExists, http.StatusConflict},
		{"invalid argument", errs.ErrInvalidArgument, http.StatusBadRequest},
		{"partition required", errs.ErrPartitionRequired, http.StatusBadRequest},
		{"not partition owner", errs.ErrNotPartitionOwner, http.StatusMisdirectedRequest},
		{"malformed handle", errs.ErrHandleMalformed, http.StatusBadRequest},
		{"stale handle", errs.ErrHandleStale, http.StatusGone},
		{"acked ahead full", errs.ErrAckedAheadFull, http.StatusServiceUnavailable},
		{"internal", errors.New("boom"), http.StatusInternalServerError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			s.WriteBrokerError(res, "op", tc.err)
			if res.Code != tc.status {
				t.Fatalf("WriteBrokerError() status = %d, want %d", res.Code, tc.status)
			}
		})
	}
}
