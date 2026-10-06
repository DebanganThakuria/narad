package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// clusterAPIRecorder is a fake API that records each request and answers
// with a fixed status and body.
type clusterAPIRecorder struct {
	mu       sync.Mutex
	requests []string
	status   int
	body     string
}

func (r *clusterAPIRecorder) serve(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.requests = append(r.requests, req.Method+" "+req.URL.RequestURI())
		status, body := r.status, r.body
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", srv.URL)
}

func (r *clusterAPIRecorder) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requests) == 0 {
		return ""
	}
	return r.requests[len(r.requests)-1]
}

func TestClusterMovesAbortCommand(t *testing.T) {
	api := &clusterAPIRecorder{status: http.StatusAccepted, body: `{"move":{"topic":"orders","partition":3}}`}
	api.serve(t)

	if err := route([]string{"cluster", "moves", "abort", "orders", "3", "--target", "narad-2"}); err != nil {
		t.Fatalf("moves abort: %v", err)
	}
	if got, want := api.last(), "POST /v1/cluster/moves/orders/3/abort?target=narad-2"; got != want {
		t.Fatalf("request = %q, want %q", got, want)
	}
	if err := route([]string{"cluster", "moves", "abort", "orders", "x"}); err == nil || !strings.Contains(err.Error(), "partition must be a number") {
		t.Fatalf("abort with partition x = %v, want a usage error", err)
	}
	if err := route([]string{"cluster", "moves", "--detail"}); err != nil {
		t.Fatalf("moves --detail: %v", err)
	}
	if got := api.last(); got != "GET /v1/cluster/moves?detail=true" {
		t.Fatalf("request = %q, want the moves list with detail", got)
	}
	if err := route([]string{"cluster", "members", "--detail"}); err != nil {
		t.Fatalf("members --detail: %v", err)
	}
	if got := api.last(); got != "GET /v1/cluster/members?detail=true" {
		t.Fatalf("request = %q, want the members list with detail", got)
	}

	api.status, api.body = http.StatusConflict, `{"error":"no move is in flight for orders/3"}`
	err := route([]string{"cluster", "moves", "abort", "orders", "3"})
	if err == nil || !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "no move is in flight") {
		t.Fatalf("abort refused by the server = %v, want the 409 and its message", err)
	}
}

func TestClusterDecommissionDryRunFlag(t *testing.T) {
	api := &clusterAPIRecorder{status: http.StatusOK, body: `{"member":"narad-2","would_decommission":true,"reasons":[]}`}
	api.serve(t)

	if err := route([]string{"cluster", "decommission", "narad-2", "--dry-run"}); err != nil {
		t.Fatalf("decommission --dry-run: %v", err)
	}
	if got, want := api.last(), "POST /v1/cluster/members/narad-2/decommission?dry_run=true"; got != want {
		t.Fatalf("request = %q, want %q", got, want)
	}
	if err := route([]string{"cluster", "decommission", "narad-2", "--dry-run", "--cancel"}); err == nil {
		t.Fatal("--dry-run with --cancel was accepted; a cancel cannot be dry-run")
	}

	// A refusal names every reason, not only the first.
	api.status = http.StatusConflict
	api.body = `{"error":"decommission of narad-0 refused: removing it would leave 2 voters","reasons":[` +
		`{"code":"below_min_voters","message":"removing it would leave 2 voters"},` +
		`{"code":"no_receivers","message":"no alive node can take its partitions"}]}`
	err := route([]string{"cluster", "decommission", "narad-0"})
	if err == nil || !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "no_receivers") {
		t.Fatalf("refused decommission = %v, want the 409 with every reason", err)
	}
	if got := api.last(); got != "POST /v1/cluster/members/narad-0/decommission" {
		t.Fatalf("request = %q, want the decommission itself", got)
	}
}
