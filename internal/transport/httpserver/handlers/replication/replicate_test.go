package replication

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	brokerruntime "github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func newReplicationTestSet(t *testing.T) (*handlers.Set, *brokerruntime.Logs) {
	t.Helper()
	opts := storage.DefaultOptions()
	opts.Codec = codec.NewNoopCodec()
	opts.FlushBytes = 1 << 20
	opts.FlushRecords = 1000
	opts.FlushInterval = time.Hour
	opts.Retention = storage.RetentionConfig{CheckInterval: time.Hour}
	logs := brokerruntime.NewLogs(t.TempDir(), opts, nil, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	return &handlers.Set{Deps: handlers.Deps{
		Logs:        logs,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		ShutdownCtx: context.Background(),
	}}, logs
}

func TestReplicateAcceptsRawPayloadRequest(t *testing.T) {
	set, logs := newReplicationTestSet(t)
	handler := Replicate(set)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/replicate", bytes.NewReader([]byte(`{"id":1}`)))
	req.Header.Set("Content-Type", replicationwire.RawContentType)
	req.Header.Set(replicationwire.HeaderTopic, "orders")
	req.Header.Set(replicationwire.HeaderPartition, "0")
	req.Header.Set(replicationwire.HeaderOffset, "0")
	req.Header.Set(replicationwire.HeaderLeaderID, "node-a")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", res.Code, res.Body.String())
	}
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("log get: %v", err)
	}
	payload, err := log.Read(0)
	if err != nil {
		t.Fatalf("log read: %v", err)
	}
	if string(payload) != `{"id":1}` {
		t.Fatalf("payload = %s, want %s", payload, `{"id":1}`)
	}
	if got := log.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
}

func TestReplicateKeepsJSONFallback(t *testing.T) {
	set, logs := newReplicationTestSet(t)
	handler := Replicate(set)
	body := []byte(`{"topic":"orders","partition":0,"offset":0,"payload":"eyJpZCI6MX0=","leader_id":"node-a"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/replicate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", res.Code, res.Body.String())
	}
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("log get: %v", err)
	}
	payload, err := log.Read(0)
	if err != nil {
		t.Fatalf("log read: %v", err)
	}
	if string(payload) != `{"id":1}` {
		t.Fatalf("payload = %s, want %s", payload, `{"id":1}`)
	}
}

func TestReplicateAcceptsBatchPayloadRequest(t *testing.T) {
	set, logs := newReplicationTestSet(t)
	handler := Replicate(set)
	body, err := replicationwire.EncodeBatchPayload([][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
	})
	if err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/replicate", bytes.NewReader(body))
	req.Header.Set("Content-Type", replicationwire.BatchContentType)
	req.Header.Set(replicationwire.HeaderTopic, "orders")
	req.Header.Set(replicationwire.HeaderPartition, "0")
	req.Header.Set(replicationwire.HeaderOffset, "0")
	req.Header.Set(replicationwire.HeaderLeaderID, "node-a")
	req.Header.Set(replicationwire.HeaderRecordCount, "2")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", res.Code, res.Body.String())
	}
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("log get: %v", err)
	}
	for offset, want := range []string{`{"id":1}`, `{"id":2}`} {
		payload, err := log.Read(int64(offset))
		if err != nil {
			t.Fatalf("log read %d: %v", offset, err)
		}
		if string(payload) != want {
			t.Fatalf("payload %d = %s, want %s", offset, payload, want)
		}
	}
	if got := log.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() = %d, want 2", got)
	}
}

func TestReplicateAcceptsDuplicateSamePayload(t *testing.T) {
	set, logs := newReplicationTestSet(t)
	handler := Replicate(set)
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/replicate", bytes.NewReader([]byte(`{"id":1}`)))
		req.Header.Set("Content-Type", replicationwire.RawContentType)
		req.Header.Set(replicationwire.HeaderTopic, "orders")
		req.Header.Set(replicationwire.HeaderPartition, "0")
		req.Header.Set(replicationwire.HeaderOffset, "0")
		req.Header.Set(replicationwire.HeaderLeaderID, "node-a")
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		if res.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204: %s", res.Code, res.Body.String())
		}
	}

	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("log get: %v", err)
	}
	if got := log.NextOffset(); got != 1 {
		t.Fatalf("NextOffset() = %d, want 1", got)
	}
}

func TestReplicateOffsetMismatchReturnsReplicaNextOffset(t *testing.T) {
	set, _ := newReplicationTestSet(t)
	handler := Replicate(set)
	first := httptest.NewRequest(http.MethodPost, "/internal/v1/replicate", bytes.NewReader([]byte(`{"id":1}`)))
	first.Header.Set("Content-Type", replicationwire.RawContentType)
	first.Header.Set(replicationwire.HeaderTopic, "orders")
	first.Header.Set(replicationwire.HeaderPartition, "0")
	first.Header.Set(replicationwire.HeaderOffset, "0")
	first.Header.Set(replicationwire.HeaderLeaderID, "node-a")
	firstRes := httptest.NewRecorder()
	handler.ServeHTTP(firstRes, first)
	if firstRes.Code != http.StatusNoContent {
		t.Fatalf("first status = %d, want 204: %s", firstRes.Code, firstRes.Body.String())
	}

	conflict := httptest.NewRequest(http.MethodPost, "/internal/v1/replicate", bytes.NewReader([]byte(`{"id":2}`)))
	conflict.Header.Set("Content-Type", replicationwire.RawContentType)
	conflict.Header.Set(replicationwire.HeaderTopic, "orders")
	conflict.Header.Set(replicationwire.HeaderPartition, "0")
	conflict.Header.Set(replicationwire.HeaderOffset, "0")
	conflict.Header.Set(replicationwire.HeaderLeaderID, "node-a")
	conflictRes := httptest.NewRecorder()
	handler.ServeHTTP(conflictRes, conflict)

	if conflictRes.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", conflictRes.Code)
	}
	if got := conflictRes.Header().Get(replicationwire.HeaderReplicaNextOffset); got != "1" {
		t.Fatalf("replica next header = %q, want 1", got)
	}
}
