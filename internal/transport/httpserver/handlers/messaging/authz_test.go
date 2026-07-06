package messaging

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
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
