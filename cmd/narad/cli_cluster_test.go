package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// `narad cluster members forget <id>` POSTs to the member's forget
// route and prints the answer; `narad cluster members` still lists.
func TestClusterMembersForgetPostsToTheForgetRoute(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.EscapedPath())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/forget") {
			_, _ = w.Write([]byte(`{"id":"narad-3","voter":true}`))
			return
		}
		_, _ = w.Write([]byte(`{"members":[]}`))
	}))
	t.Cleanup(srv.Close)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", srv.URL)

	out, _, err := captureCLIOutput(t, func() error { return route([]string{"cluster", "members", "forget", "narad-3"}) }, "")
	if err != nil {
		t.Fatalf("cluster members forget: %v", err)
	}
	if !strings.Contains(out, `"voter": true`) && !strings.Contains(out, `"voter":true`) {
		t.Fatalf("output %q does not show the answer", out)
	}
	if _, _, err := captureCLIOutput(t, func() error { return route([]string{"cluster", "members"}) }, ""); err != nil {
		t.Fatalf("cluster members: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"POST /v1/cluster/members/narad-3/forget", "GET /v1/cluster/members"}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Fatalf("requests = %q, want %q", seen, want)
	}
}
