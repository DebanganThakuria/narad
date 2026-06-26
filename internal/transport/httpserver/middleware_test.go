package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChainAppliesMiddlewaresInDeclarationOrder(t *testing.T) {
	var calls []string
	mw := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls = append(calls, name+":before")
				next.ServeHTTP(w, r)
				calls = append(calls, name+":after")
			})
		}
	}

	handler := Chain(mw("a"), mw("b"))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, "handler")
		w.WriteHeader(http.StatusNoContent)
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	want := []string{"a:before", "b:before", "handler", "b:after", "a:after"}
	if len(calls) != len(want) {
		t.Fatalf("calls length = %d, want %d (%v)", len(calls), len(want), calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls[%d] = %q, want %q (all calls: %v)", i, calls[i], want[i], calls)
		}
	}
}
