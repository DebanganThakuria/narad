package messaging

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// produceConsumeAuthzMatrix drives produce/consume/ack through the
// grant matcher: the identity below may produce to orders-* and consume
// from logs only.
func TestDataPlaneAuthorization(t *testing.T) {
	id := user.User{Username: "svc", Grants: []user.Grant{
		{Action: user.ActionProduce, Patterns: []string{"orders-*"}},
		{Action: user.ActionConsume, Patterns: []string{"logs"}},
	}}

	cases := []struct {
		name   string
		method string
		target string
		topic  string
		want   int
	}{
		{"produce granted", http.MethodPost, "/v1/topics/orders-eu/produce", "orders-eu", http.StatusAccepted},
		{"produce denied", http.MethodPost, "/v1/topics/logs/produce", "logs", http.StatusForbidden},
		{"consume denied", http.MethodGet, "/v1/topics/orders-eu/consume", "orders-eu", http.StatusForbidden},
		{"ack denied", http.MethodPost, "/v1/topics/orders-eu/ack", "orders-eu", http.StatusForbidden},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, topicName, _ string, _ []byte, _ ...int) (ingress.AcceptedProduce, error) {
				return ingress.AcceptedProduce{Topic: topicName}, nil
			}}, nil)
			var h http.HandlerFunc
			switch {
			case c.method == http.MethodPost && strings.HasSuffix(c.target, "/produce"):
				h = Produce(s)
			case c.method == http.MethodGet:
				h = Consume(s)
			default:
				h = Ack(s)
			}
			req := httptest.NewRequest(c.method, c.target, bytes.NewBufferString(`{"x":1}`))
			req.SetPathValue("topic", c.topic)
			req = req.WithContext(security.WithIdentity(req.Context(), id))
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != c.want {
				t.Fatalf("status = %d, want %d (body %s)", res.Code, c.want, res.Body)
			}
		})
	}
}

// A principal that may not produce to a topic is refused with 403 on a
// node being decommissioned too: the drain is admin-only topology, and a
// retryable 503 would invite retries of a request that can never pass.
func TestProduceIsAuthorizedBeforeTheDrainRefusal(t *testing.T) {
	id := user.User{Username: "svc", Grants: []user.Grant{
		{Action: user.ActionConsume, Patterns: []string{"orders"}},
	}}
	drain := &handlers.DrainGate{}
	drain.SetDraining(true)
	s := handlers.New(handlers.Deps{
		Broker: &fakeBroker{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Drain: drain,
	})
	for _, tc := range []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{"produce", "/v1/topics/orders/produce", Produce(s)},
		{"batch", "/v1/topics/orders/produce/batch", ProduceBatch(s, nil)},
	} {
		req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewBufferString(`{"messages":[{"value":{"id":1}}]}`))
		req.SetPathValue("topic", "orders")
		req = req.WithContext(security.WithIdentity(req.Context(), id))
		res := httptest.NewRecorder()
		tc.handler.ServeHTTP(res, req)
		if res.Code != http.StatusForbidden {
			t.Fatalf("%s without a produce grant on a draining node: status %d, want 403 (body %s)", tc.name, res.Code, res.Body)
		}
		if res.Header().Get("Retry-After") != "" || strings.Contains(res.Body.String(), "decommissioned") {
			t.Fatalf("%s without a produce grant learned the node is draining: Retry-After %q, body %s", tc.name, res.Header().Get("Retry-After"), res.Body)
		}
	}
}
