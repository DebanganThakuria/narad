package httpserver

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A batch produce takes its identity's in-flight gate before it reads
// its body: a body costs memory before its message count is known, so
// the requests still uploading or decoding one count against the cap
// too. With a cap of 1, a batch whose upload has stalled keeps a second
// batch of the same identity out with 429, before that one's body is
// read. Before, the gate was taken only after the whole body had been
// read and decoded, so any number of batches could be reading at once.
func TestWP22ProduceBatchGateCoversTheBodyRead(t *testing.T) {
	br := &zzWP18RouterBroker{fakeBroker: &fakeBroker{}}
	srv := httptest.NewServer(NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, nil, RouterOptions{ProduceInFlightPerIdentity: 1}))
	t.Cleanup(srv.Close)
	body := `{"messages":[{"payload":1}]}`

	pr, pw := io.Pipe()
	stalled := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/topics/orders/produce/batch", pr)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			stalled <- 0
			return
		}
		resp.Body.Close()
		stalled <- resp.StatusCode
	}()
	// The stalled upload has sent its first bytes, so its request is in
	// the handler, reading.
	if _, err := pw.Write([]byte(body[:5])); err != nil {
		t.Fatal(err)
	}

	post := func() int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/topics/orders/produce/batch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	refused := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if post() == http.StatusTooManyRequests {
			refused = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !refused {
		t.Error("a second batch was admitted while the identity's only slot was held by a batch still reading its body")
	}

	// The stalled batch finishes, and its slot comes back.
	if _, err := pw.Write([]byte(body[5:])); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	if code := <-stalled; code != http.StatusAccepted {
		t.Fatalf("the stalled batch = %d, want 202", code)
	}
	if code := post(); code != http.StatusAccepted {
		t.Fatalf("a batch after the slot came back = %d, want 202", code)
	}
}

// Requests that are reading their bodies do not hold each other's raise
// back: at a cap of 4, two batches of 4 that both read their bodies do
// not both fail. The first raise takes the whole cap; the second is
// refused, and its reading slot is given back when it is released.
func TestWP22InFlightHoldsRaiseAgainstWeightOnly(t *testing.T) {
	l := newInFlightLimiterFor(4, "produce")
	gate := l.gate()
	r := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce/batch", nil)
	a, ok := gate(httptest.NewRecorder(), r)
	if !ok {
		t.Fatal("first batch refused before reading")
	}
	b, ok := gate(httptest.NewRecorder(), r)
	if !ok {
		t.Fatal("second batch refused before reading")
	}
	if !a.Raise(httptest.NewRecorder(), 4) {
		t.Fatal("the first raise to the whole cap was refused for the second batch's reading slot")
	}
	w := httptest.NewRecorder()
	if b.Raise(w, 4) || w.Code != http.StatusTooManyRequests {
		t.Fatalf("the second raise beside a full cap: code %d, want 429", w.Code)
	}
	// Full: a third batch is refused before it reads anything.
	if _, ok := gate(httptest.NewRecorder(), r); ok {
		t.Fatal("a batch was admitted to read with the cap already taken")
	}
	b.Release()
	a.Release()
	l.mu.Lock()
	counts, reading := len(l.counts), len(l.reading)
	l.mu.Unlock()
	if counts != 0 || reading != 0 {
		t.Fatalf("after every release: %d weights, %d reading slots left; want none", counts, reading)
	}
	// A lone batch larger than the cap is still served.
	c, ok := gate(httptest.NewRecorder(), r)
	if !ok || !c.Raise(httptest.NewRecorder(), 100) {
		t.Fatal("a lone batch larger than the cap was refused")
	}
	c.Release()
}
