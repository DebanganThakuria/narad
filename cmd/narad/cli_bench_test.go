package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// benchServer accepts every produce, hands out ready messages one per
// consume until they run out, then answers each empty long-poll after
// emptyWait, the way a broker holds a poll open before its 204.
func benchServer(t *testing.T, ready int64, emptyWait time.Duration) *httptest.Server {
	t.Helper()
	var left atomic.Int64
	left.Store(ready)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/produce"):
			w.WriteHeader(http.StatusAccepted)
		case strings.HasSuffix(r.URL.Path, "/consume"):
			n := left.Add(-1)
			if n < 0 {
				time.Sleep(emptyWait)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"topic":"bench","partition":0,"offset":%d,"payload":{"bench":"x"},"receipt_handle":"h%d"}`, n, n)
		case strings.HasSuffix(r.URL.Path, "/ack"):
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

var consumeLine = regexp.MustCompile(`consume: (\d+) msgs in ([^ ]+) \((\d+) msg/s\)`)

// The consume rate covers the drain, up to the last message consumed,
// not the empty polls each worker needs to decide the topic is drained.
// It used to include them, which cut a 50,000-message drain's rate by
// the two empty long-polls' worth of time.
func TestBenchConsumeRateExcludesTheIdleTail(t *testing.T) {
	srv := benchServer(t, 300, 400*time.Millisecond)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", srv.URL)

	start := time.Now()
	_, stderr, err := captureCLIOutput(t, func() error {
		return route([]string{"bench", "bench", "--count", "300", "--workers", "4", "--consume"})
	}, "")
	if err != nil {
		t.Fatalf("bench: %v", err)
	}
	wall := time.Since(start)
	m := consumeLine.FindStringSubmatch(stderr)
	if m == nil {
		t.Fatalf("no consume line in:\n%s", stderr)
	}
	if m[1] != "300" {
		t.Fatalf("consumed %s, want 300", m[1])
	}
	took, err := time.ParseDuration(m[2])
	if err != nil {
		t.Fatalf("parse %q: %v", m[2], err)
	}
	// Every worker sits through two empty polls (800ms here) before it
	// stops, so the wall time is at least that; the drain itself is not.
	if wall < 800*time.Millisecond {
		t.Fatalf("wall time %s: the fake broker's empty polls did not hold", wall)
	}
	if took >= 400*time.Millisecond {
		t.Errorf("consume reported %s for a drain served at once: it counted the empty polls", took)
	}
	if rate, _ := strconv.Atoi(m[3]); rate < 750 {
		t.Errorf("consume rate %d msg/s, want the drain's rate (well over 750)", rate)
	}
	if strings.Contains(stderr, "were not drained") {
		t.Errorf("a full drain was reported as short:\n%s", stderr)
	}
}

// A drain that stops short says how many it left behind, so a stuck
// partition cannot pass as a slower consume rate.
func TestBenchConsumeReportsAShortDrain(t *testing.T) {
	srv := benchServer(t, 250, 50*time.Millisecond)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", srv.URL)

	_, stderr, err := captureCLIOutput(t, func() error {
		return route([]string{"bench", "bench", "--count", "300", "--workers", "2", "--consume"})
	}, "")
	if err != nil {
		t.Fatalf("bench: %v", err)
	}
	if !strings.Contains(stderr, "consume: 250 msgs") {
		t.Fatalf("want 250 consumed in:\n%s", stderr)
	}
	if !strings.Contains(stderr, "50 of the 300 produced were not drained") {
		t.Errorf("short drain not reported:\n%s", stderr)
	}
}
