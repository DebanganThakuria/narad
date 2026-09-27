package messaging

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// zzWP18BatchRouter is a fakeRouter with the batch consume surface: it
// writes what write writes and reports batch.
type zzWP18BatchRouter struct {
	*fakeRouter
	write func(w http.ResponseWriter)
	batch bool
	max   int
}

func (r *zzWP18BatchRouter) RouteConsumeBatch(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ *int, max int) (bool, bool, *int) {
	r.max = max
	r.write(w)
	return true, r.batch, nil
}

// TestZZWP18BatchConsumeUsesBatchRouter checks the handler hands the
// batch size to the router's batch form and sends what it gets back: a
// batch the owner built as it is, a single message wrapped as a batch of
// one, and a failure unchanged.
func TestZZWP18BatchConsumeUsesBatchRouter(t *testing.T) {
	one := zzWP12Msg(0, 1)
	two := zzWP12Msg(0, 2)
	batchBody := `{"messages":[` + string(one.AppendJSON(nil)) + `,` + string(two.AppendJSON(nil)) + "]}\n"
	for _, tc := range []struct {
		name   string
		batch  bool
		write  func(w http.ResponseWriter)
		status int
		body   string
	}{
		{"owner batch", true, func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(batchBody))
		}, http.StatusOK, batchBody},
		{"single message", false, func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(append(one.AppendJSON(nil), '\n'))
		}, http.StatusOK, `{"messages":[` + string(one.AppendJSON(nil)) + "]}\n"},
		{"nothing", true, func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }, http.StatusNoContent, ""},
		{"owner down", true, func(w http.ResponseWriter) { http.Error(w, "owner down", http.StatusServiceUnavailable) }, http.StatusServiceUnavailable, "owner down\n"},
	} {
		router := &zzWP18BatchRouter{fakeRouter: &fakeRouter{}, write: tc.write, batch: tc.batch}
		br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) {
			t.Fatal("a forwarded batch consume reached the local broker")
			return nil, nil
		}}
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=25", nil)
		req.SetPathValue("topic", "orders")
		res := httptest.NewRecorder()
		Consume(newTestSet(br, router))(res, req)
		if res.Code != tc.status || res.Body.String() != tc.body {
			t.Fatalf("%s: %d %q, want %d %q", tc.name, res.Code, res.Body, tc.status, tc.body)
		}
		if router.max != 25 {
			t.Fatalf("%s: router asked for %d records, want 25", tc.name, router.max)
		}
		if tc.status == http.StatusOK && res.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("%s: Content-Type %q", tc.name, res.Header().Get("Content-Type"))
		}
	}
}
