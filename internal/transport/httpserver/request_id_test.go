package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestIDPreservesIncomingHeader(t *testing.T) {
	var gotContextID string
	handler := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContextID = requestIDFrom(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set(HeaderRequestID, "req-fixed")
	res := httptest.NewRecorder()

	handler.ServeHTTP(res, req)

	if got := res.Header().Get(HeaderRequestID); got != "req-fixed" {
		t.Fatalf("response request id = %q, want %q", got, "req-fixed")
	}
	if gotContextID != "req-fixed" {
		t.Fatalf("context request id = %q, want %q", gotContextID, "req-fixed")
	}
}

func TestRequestIDGeneratesHeaderWhenMissing(t *testing.T) {
	var gotContextID string
	handler := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContextID = requestIDFrom(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	res := httptest.NewRecorder()
	handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	generated := res.Header().Get(HeaderRequestID)
	if generated == "" {
		t.Fatal("response request id = empty, want generated id")
	}
	if gotContextID != generated {
		t.Fatalf("context request id = %q, want %q", gotContextID, generated)
	}
}

func TestNewRequestIDUsesPodPrefixAndSequence(t *testing.T) {
	oldPrefix := requestIDPrefix
	oldSeq := requestIDSeq.Load()
	t.Cleanup(func() {
		requestIDPrefix = oldPrefix
		requestIDSeq.Store(oldSeq)
	})

	requestIDPrefix = "req-narad-0"
	requestIDSeq.Store(0)

	first := newRequestID()
	second := newRequestID()

	if !strings.HasPrefix(first, "req-narad-0-") {
		t.Fatalf("request id = %q, want pod prefix", first)
	}
	if first == second {
		t.Fatalf("request IDs should be unique, got %q twice", first)
	}
	if !strings.HasSuffix(first, "-1") {
		t.Fatalf("first request id = %q, want sequence suffix -1", first)
	}
	if !strings.HasSuffix(second, "-2") {
		t.Fatalf("second request id = %q, want sequence suffix -2", second)
	}
}

func TestSanitizeRequestIDPart(t *testing.T) {
	got := sanitizeRequestIDPart("narad-0.dev_cluster\nbad")
	want := "narad-0.dev_cluster-bad"
	if got != want {
		t.Fatalf("sanitizeRequestIDPart() = %q, want %q", got, want)
	}
}

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

func TestRecorderWriteDefaultsStatusAndCountsBytes(t *testing.T) {
	base := httptest.NewRecorder()
	rec := &recorder{ResponseWriter: base}

	n, err := rec.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != 5 {
		t.Fatalf("Write() bytes = %d, want 5", n)
	}
	if rec.status != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.status, http.StatusOK)
	}
	if rec.bytes != 5 {
		t.Fatalf("bytes = %d, want 5", rec.bytes)
	}
}
