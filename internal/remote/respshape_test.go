package remote_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/remote"
)

// The response shapes the classifier relies on, against the real
// router: every answer of a Narad handler is Narad's JSON error, and a
// route the router lacks is Go's exact not-found.
func TestResponseShapesAgainstTheRealRouter(t *testing.T) {
	tg := newRealTarget(t, false)
	get := func(path string) (*http.Response, []byte) {
		t.Helper()
		resp, err := tg.srv.Client().Get(tg.url() + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := remote.ReadBody(resp, remote.MaxReadAnswerBytes)
		if err != nil {
			t.Fatal(err)
		}
		return resp, body
	}

	resp, body := get("/v1/topics/missing")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("describe missing topic: %d", resp.StatusCode)
	}
	if _, ok := remote.NaradError(resp, body); !ok || remote.IsGoNotFound(resp, body) {
		t.Fatalf("a handler's 404 must be Narad's JSON shape: %q %q", resp.Header.Get("Content-Type"), body)
	}

	resp, body = get("/v1/no-such-route")
	if !remote.IsGoNotFound(resp, body) {
		t.Fatalf("an unregistered route must be Go's not-found: %d %q %q", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
	if _, ok := remote.NaradError(resp, body); ok {
		t.Fatal("Go's not-found parsed as Narad's error shape")
	}
}

// A bare ServeMux registered like a target that predates batch produce
// answers the missing batch route with Go's not-found.
func TestIsGoNotFoundOnABareMux(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/topics/{topic}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"orders"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/topics/orders/produce/batch", "application/json", strings.NewReader(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := remote.ReadBody(resp, remote.MaxProduceAnswerBytes)
	if !remote.IsGoNotFound(resp, body) {
		t.Fatalf("bare mux missing route: %d %q %q", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}

	// A plain-text 404 that is not Go's exact answer (a proxy's) is not.
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found\n"))
	}))
	defer edge.Close()
	resp, err = http.Get(edge.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = remote.ReadBody(resp, 1024)
	if remote.IsGoNotFound(resp, body) {
		t.Fatal("a proxy's plain-text 404 read as Go's not-found")
	}
}

func TestReadBodyCapsAndCloses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 10_000)))
	}))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := remote.ReadBody(resp, 100)
	if err != nil || len(body) != 100 {
		t.Fatalf("ReadBody = %d bytes, %v", len(body), err)
	}
}
