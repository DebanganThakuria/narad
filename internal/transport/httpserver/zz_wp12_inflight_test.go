package httpserver

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// A batch consume holds as much of its identity's in-flight budget as
// records it may reserve: with a cap of 10, a batch of 8 in flight
// leaves room for a single consume but not for a batch of 5, and a
// batch larger than the cap is still served when it is alone.
func TestZZWP12ConsumeInFlightCapCountsBatch(t *testing.T) {
	br := &consumeBroker{release: make(chan struct{}), started: make(chan struct{}, 8)}
	router := NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, nil, RouterOptions{ConsumeInFlightPerIdentity: 10})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	get := func(query string) int {
		t.Helper()
		resp, err := http.Get(srv.URL + "/v1/topics/orders/consume" + query)
		if err != nil {
			t.Error(err)
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	var wg sync.WaitGroup
	wg.Go(func() { get("?max=8") })
	<-br.started

	if code := get("?max=5"); code != http.StatusTooManyRequests {
		t.Fatalf("batch of 5 beside a batch of 8 at cap 10: status %d, want 429", code)
	}
	if code := get("?max=3"); code != http.StatusTooManyRequests {
		t.Fatalf("batch of 3 beside a batch of 8 at cap 10: status %d, want 429", code)
	}
	wg.Go(func() { get("") })
	select {
	case <-br.started:
	case <-time.After(2 * time.Second):
		t.Fatal("a single consume did not fit beside a batch of 8 at cap 10")
	}
	close(br.release)
	wg.Wait()

	// Alone, a batch bigger than the cap counts as the whole cap.
	br2 := &consumeBroker{release: make(chan struct{}), started: make(chan struct{}, 8)}
	close(br2.release)
	srv2 := httptest.NewServer(NewRouterWithOptions(newTestSet(br2), newTestLogger(), nil, nil, nil, RouterOptions{ConsumeInFlightPerIdentity: 10}))
	t.Cleanup(srv2.Close)
	resp, err := http.Get(srv2.URL + "/v1/topics/orders/consume?max=50")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatal("a lone batch larger than the cap was refused")
	}
}

func TestZZWP12ConsumeWeight(t *testing.T) {
	for query, want := range map[string]int{
		"":                      1,
		"wait=5s":               1,
		"max=10":                10,
		"wait=1s&max=100":       100,
		"m%61x=7":               7,
		"max=0":                 1,
		"max=101":               1,
		"max=abc":               1,
		"max=4&max=9":           4,
		"partition=2&wait=%31s": 1,
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/topics/t/consume?"+query, nil)
		if got := httpmessaging.ConsumeWeight(r); got != want {
			t.Errorf("ConsumeWeight(%q) = %d, want %d", query, got, want)
		}
	}
}
