package e2e

import (
	"net/http"
	"strings"
	"testing"
)

// A browser can send a cross-origin POST with a "simple" content type
// and no preflight, carrying cached Basic credentials. The API refuses
// such requests with 415; the same request with an API content type or
// the client header goes through.
func TestCrossSiteSimpleRequestsAreRefused(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	send := func(contentType, clientHeader string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, env.Server.URL+"/v1/topics", strings.NewReader(`{"name":"xsite","partitions":3}`))
		if err != nil {
			t.Fatal(err)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if clientHeader != "" {
			req.Header.Set("X-Narad-Client", clientHeader)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	for _, simple := range []string{"text/plain", "application/x-www-form-urlencoded", ""} {
		if resp := send(simple, ""); resp.StatusCode != http.StatusUnsupportedMediaType {
			t.Fatalf("POST with Content-Type %q: status %d, want 415", simple, resp.StatusCode)
		}
	}
	if resp := send("application/json", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST with application/json: status %d, want 201", resp.StatusCode)
	}
	if resp := send("text/plain", "test"); resp.StatusCode == http.StatusUnsupportedMediaType {
		t.Fatal("POST with the client header was refused")
	}
}
