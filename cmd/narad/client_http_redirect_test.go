package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// A server URL that answers a redirect (an edge rewriting the scheme or
// the host) never gets the request body or the caller's credentials
// sent on: the remote password and the Basic header would otherwise go
// to wherever Location points, plain http included.
func TestCLIRefusesToFollowARedirectWithTheRemotePassword(t *testing.T) {
	for _, code := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect, http.StatusFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			resetRemoteCLI(t)
			var leaked atomic.Int32
			// Any request at all here would have followed the redirect.
			elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				leaked.Add(1)
				w.WriteHeader(http.StatusCreated)
			}))
			defer elsewhere.Close()
			edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, elsewhere.URL+r.URL.Path, code)
			}))
			defer edge.Close()

			cliStdin = strings.NewReader("the-remote-password-0123456789\n")
			err := route([]string{
				"remote", "add", "b", "--server", edge.URL, "--user", "alice", "--password", "pw",
				"--url", "https://narad-b.example", "--username", "repl", "--remote-password-stdin",
			})
			if err == nil || !strings.Contains(err.Error(), "redirect") {
				t.Fatalf("remote add through a redirect: %v, want a refusal naming the redirect", err)
			}
			if n := leaked.Load(); n != 0 {
				t.Fatalf("the redirect target got %d requests", n)
			}
		})
	}
}
