package httpserver

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/crypto/bcrypt"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security"
)

// wp1bDiscardRW is a ResponseWriter that keeps only the status, so the
// edge benchmark measures the router and middleware, not a recorder.
type wp1bDiscardRW struct {
	h      http.Header
	status int
}

func (d *wp1bDiscardRW) Header() http.Header         { return d.h }
func (d *wp1bDiscardRW) Write(b []byte) (int, error) { return len(b), nil }
func (d *wp1bDiscardRW) WriteHeader(s int)           { d.status = s }

func wp1bEdgeRouter(b *testing.B, withAuth, withMetrics bool) http.Handler {
	b.Helper()
	fb := &fakeBroker{
		ackFn: func(context.Context, string, consumer.Handle) error { return nil },
		consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
			return topic.Message{
				Topic: "orders", Partition: 3, Offset: 12345,
				Payload:       []byte(`{"a":1,"b":"hello","c":[1,2,3],"d":true,"e":null}`),
				Timestamp:     1790000000,
				ReceiptHandle: consumer.EncodeHandle(consumer.Handle{Partition: 3, Offset: 12345, Nonce: 991234567}),
			}, true, nil
		},
	}
	var auth *security.Authenticator
	if withAuth {
		hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
		if err != nil {
			b.Fatal(err)
		}
		auth = security.New(staticUserStore{users: map[string]user.User{
			"alice": {Username: "alice", PasswordHash: hash, Grants: []user.Grant{
				{Action: user.ActionProduce, Patterns: []string{"orders*"}},
				{Action: user.ActionConsume, Patterns: []string{"orders*"}},
			}},
		}}, newTestLogger())
	}
	var m *metrics.Metrics
	var reg *prometheus.Registry
	if withMetrics {
		reg = prometheus.NewRegistry()
		m = metrics.New(reg)
	}
	return NewRouterWithOptions(newTestSet(fb), newTestLogger(), m, reg, auth, RouterOptions{ConsumeInFlightPerIdentity: 1024})
}

func wp1bBenchRequest(b *testing.B, h http.Handler, method, target string, body []byte, want int) {
	b.Helper()
	req, err := http.NewRequest(method, "http://x"+target, nil)
	if err != nil {
		b.Fatal(err)
	}
	req.SetBasicAuth("alice", "pw")
	req.Header.Set("X-Narad-Client", "bench")
	req.Header.Set("Content-Type", "application/json")
	rd := bytes.NewReader(body)
	req.ContentLength = int64(len(body))
	rw := &wp1bDiscardRW{h: http.Header{}}
	// One untimed request first, so the auth cache is warm and the
	// one-off bcrypt verification stays out of the measurement.
	req.Body = io.NopCloser(rd)
	h.ServeHTTP(rw, req)
	b.ReportAllocs()
	for b.Loop() {
		rd.Reset(body)
		req.Body = io.NopCloser(rd)
		clear(rw.h)
		rw.status = 0
		h.ServeHTTP(rw, req)
		if rw.status != want && (want != http.StatusOK || rw.status != 0) {
			b.Fatalf("status %d, want %d", rw.status, want)
		}
	}
}

// BenchmarkWP1BEdge drives produce, ack and consume through the real
// router with a no-op broker, with and without the auth middleware.
func BenchmarkWP1BEdge(b *testing.B) {
	payload := []byte(strings.Repeat("x", 100))
	for _, cfg := range []struct {
		name          string
		auth, metrics bool
	}{{"metrics", false, true}, {"metrics+auth", true, true}} {
		h := wp1bEdgeRouter(b, cfg.auth, cfg.metrics)
		b.Run(cfg.name+"/produce", func(b *testing.B) {
			wp1bBenchRequest(b, h, http.MethodPost, "/v1/topics/orders/produce", payload, http.StatusAccepted)
		})
		b.Run(cfg.name+"/produce_keyed", func(b *testing.B) {
			wp1bBenchRequest(b, h, http.MethodPost, "/v1/topics/orders/produce?key=k1", payload, http.StatusAccepted)
		})
		b.Run(cfg.name+"/ack", func(b *testing.B) {
			wp1bBenchRequest(b, h, http.MethodPost, "/v1/topics/orders/ack?receipt_handle=3%3A12345%3A991234567", nil, http.StatusNoContent)
		})
		b.Run(cfg.name+"/consume", func(b *testing.B) {
			wp1bBenchRequest(b, h, http.MethodGet, "/v1/topics/orders/consume", nil, http.StatusOK)
		})
	}
}
