package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A batch consume reserves up to 100 records a request, and a GET needs
// no preflight, so a page on another origin could keep a topic's
// records hidden with a browser's cached credentials. It must carry the
// client header; a single consume stays open to plain clients.
func TestRequireAPIContentTypeGuardsBatchConsume(t *testing.T) {
	served := 0
	h := RequireAPIContentType()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.WriteHeader(http.StatusOK)
	}))
	do := func(method, target string, header bool) int {
		req := httptest.NewRequest(method, target, nil)
		if header {
			req.Header.Set(ClientHeader, "test")
		}
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res.Code
	}
	for _, target := range []string{
		"/v1/topics/orders/consume?max=5",
		"/v1/topics/orders/consume?wait=10s&max=100",
		"/v1/topics/orders/consume?%6Dax=5",
		"/v1/topics/orders/consume?" + strings.Repeat("x=1&", 10001) + "max=100",
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			served = 0
			if got := do(method, target, false); got != http.StatusBadRequest || served != 0 {
				t.Fatalf("%s %s without %s: status = %d, served = %d, want 400 and not served", method, target, ClientHeader, got, served)
			}
			if got := do(method, target, true); got != http.StatusOK {
				t.Fatalf("%s %s with %s: status = %d, want 200", method, target, ClientHeader, got)
			}
		}
	}
	for _, target := range []string{
		"/v1/topics/orders/consume",
		"/v1/topics/orders/consume?wait=10s",
		"/v1/topics/orders/consume?max=",
		"/v1/topics/max/consume",
		"/v1/topics/orders?max=5",
	} {
		if got := do(http.MethodGet, target, false); got != http.StatusOK {
			t.Fatalf("GET %s without %s: status = %d, want 200", target, ClientHeader, got)
		}
	}
}

// The mux serves HEAD on the GET consume route, so the router must
// refuse a header-less HEAD batch consume as it does a GET one.
func TestRouterRefusesHeaderlessHeadBatchConsume(t *testing.T) {
	router := NewRouter(newTestSet(&fakeBroker{}), newTestLogger(), nil, nil, nil)
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req := httptest.NewRequest(method, "/v1/topics/orders/consume?max=100", nil)
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest {
			t.Fatalf("%s batch consume without %s through the router: status = %d, want 400", method, ClientHeader, res.Code)
		}
	}
}
