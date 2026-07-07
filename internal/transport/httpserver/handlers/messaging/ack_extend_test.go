package messaging

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/errs"
)

func ackRequest(query string) *http.Request {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 1, Offset: 2, Nonce: 3})
	req := httptest.NewRequest(http.MethodPost,
		"/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(handle)+query, nil)
	req.SetPathValue("topic", "orders")
	return req
}

// The extend parameter picks the broker call: absent → Ack,
// extend=true → ExtendAck, extend=0 → Nack.
func TestAckHandlerDispatchesByExtendParam(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"plain ack", "", "ack"},
		{"extend true", "&extend=true", "extend"},
		{"extend 1", "&extend=1", "extend"},
		{"extend false is a plain ack", "&extend=false", "ack"},
		{"nack via extend 0", "&extend=0", "nack"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called string
			s := newTestSet(&fakeBroker{
				ackFn: func(context.Context, string, consumer.Handle) error {
					called = "ack"
					return nil
				},
				extendAckFn: func(context.Context, string, consumer.Handle) error {
					called = "extend"
					return nil
				},
				nackFn: func(context.Context, string, consumer.Handle) error {
					called = "nack"
					return nil
				},
			}, nil)

			res := httptest.NewRecorder()
			Ack(s).ServeHTTP(res, ackRequest(tc.query))

			if res.Code != http.StatusNoContent {
				t.Fatalf("status = %d body=%s, want 204", res.Code, res.Body)
			}
			if called != tc.want {
				t.Fatalf("broker call = %q, want %q", called, tc.want)
			}
		})
	}
}

func TestAckHandlerRejectsBadExtendValue(t *testing.T) {
	called := false
	s := newTestSet(&fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error {
		called = true
		return nil
	}}, nil)

	res := httptest.NewRecorder()
	Ack(s).ServeHTTP(res, ackRequest("&extend=maybe"))

	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", res.Code)
	}
	if called {
		t.Fatal("broker called despite invalid extend value")
	}
}

// Extend and nack forward to the partition owner exactly like ack.
func TestExtendAndNackRouteToOwner(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"extend", "&extend=true"},
		{"nack", "&extend=0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brokerCalled := false
			routed := ""
			s := newTestSet(&fakeBroker{
				extendAckFn: func(context.Context, string, consumer.Handle) error {
					brokerCalled = true
					return nil
				},
				nackFn: func(context.Context, string, consumer.Handle) error {
					brokerCalled = true
					return nil
				},
			}, &fakeRouter{
				routeExtendAckFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ consumer.Handle) bool {
					routed = "extend"
					w.WriteHeader(http.StatusNoContent)
					return true
				},
				routeNackFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ consumer.Handle) bool {
					routed = "nack"
					w.WriteHeader(http.StatusNoContent)
					return true
				},
			})

			res := httptest.NewRecorder()
			Ack(s).ServeHTTP(res, ackRequest(tc.query))

			if res.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", res.Code)
			}
			if routed != tc.name {
				t.Fatalf("routed = %q, want %q", routed, tc.name)
			}
			if brokerCalled {
				t.Fatal("broker called after routing")
			}
		})
	}
}

// A lapsed handle maps to 410 Gone for extend and nack, same as ack.
func TestExtendAndNackMapStaleTo410(t *testing.T) {
	s := newTestSet(&fakeBroker{
		extendAckFn: func(context.Context, string, consumer.Handle) error {
			return errs.ErrHandleStale
		},
		nackFn: func(context.Context, string, consumer.Handle) error {
			return errs.ErrHandleStale
		},
	}, nil)

	for _, query := range []string{"&extend=true", "&extend=0"} {
		res := httptest.NewRecorder()
		Ack(s).ServeHTTP(res, ackRequest(query))
		if res.Code != http.StatusGone {
			t.Fatalf("status for %s = %d, want 410", query, res.Code)
		}
	}
}
