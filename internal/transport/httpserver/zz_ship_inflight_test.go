package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// A batch produce still reading its body counts one against single
// produces of the same identity, as it does against other batches: the
// cap bounds the produce requests one identity has open, and the batch
// bodies being read are among them. Single produces used to be admitted
// on the weight in flight alone, so at a cap of C an identity could hold
// C batch bodies being read plus C single produces.
func TestShipProduceCapCountsBatchReadersForSingles(t *testing.T) {
	l := newInFlightLimiterFor(2, "produce")
	gate := l.gate()
	batchReq := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce/batch", nil)
	a, ok := gate(httptest.NewRecorder(), batchReq)
	if !ok {
		t.Fatal("first batch refused before reading")
	}
	b, ok := gate(httptest.NewRecorder(), batchReq)
	if !ok {
		t.Fatal("second batch refused before reading")
	}

	served := 0
	single := l.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.WriteHeader(http.StatusAccepted)
	}), nil)
	produce := func() int {
		w := httptest.NewRecorder()
		single.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil))
		return w.Code
	}

	if code := produce(); code != http.StatusTooManyRequests || served != 0 {
		t.Fatalf("single produce beside two batches reading at a cap of 2: code %d, served %d; want 429 before the handler", code, served)
	}
	// One batch gives its reading slot back: a single produce fits again.
	b.Release()
	if code := produce(); code != http.StatusAccepted || served != 1 {
		t.Fatalf("single produce beside one batch reading at a cap of 2: code %d, served %d; want 202", code, served)
	}
	// The remaining batch raises to one: its weight still leaves room for
	// one single produce, and a raise is not refused for it.
	if !a.Raise(httptest.NewRecorder(), 1) {
		t.Fatal("a batch of one refused at its raise with the cap otherwise free")
	}
	if code := produce(); code != http.StatusAccepted || served != 2 {
		t.Fatalf("single produce beside a raised batch of one at a cap of 2: code %d, served %d; want 202", code, served)
	}
	a.Release()
	l.mu.Lock()
	counts, reading := len(l.counts), len(l.reading)
	l.mu.Unlock()
	if counts != 0 || reading != 0 {
		t.Fatalf("after every release: %d weights, %d reading slots left; want none", counts, reading)
	}
}
