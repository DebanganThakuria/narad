package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// authReq issues an HTTP request with optional Basic credentials against
// the running secured server.
func (e *env) authReq(t *testing.T, method, path string, body any, username, password string) *http.Response {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, e.url(path), reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if username != "" {
		req.SetBasicAuth(username, password)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return res
}

func TestSecurityEndToEnd(t *testing.T) {
	e := newTestEnv(t, withSecurity())

	// Unauthenticated request is rejected with a challenge.
	res := e.authReq(t, http.MethodGet, "/v1/topics", nil, "", "")
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no creds: status = %d, want 401", res.StatusCode)
	}
	res.Body.Close()

	// Probes stay open.
	res = e.authReq(t, http.MethodGet, "/healthz", nil, "", "")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("healthz without creds: status = %d, want 200", res.StatusCode)
	}
	res.Body.Close()

	admin := func() (string, string) { return e.adminUser, e.adminPass }

	// Admin creates a scoped producer user.
	au, ap := admin()
	res = e.authReq(t, http.MethodPost, "/v1/users", map[string]any{
		"username": "producer",
		"password": "prodpw",
		"grants":   []map[string]any{{"action": "produce", "patterns": []string{"orders-*"}}},
	}, au, ap)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create producer: status = %d", res.StatusCode)
	}
	res.Body.Close()

	// Admin creates the topic (owns it).
	res = e.authReq(t, http.MethodPost, "/v1/topics", map[string]any{"name": "orders-eu", "partitions": 3}, au, ap)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: status = %d", res.StatusCode)
	}
	res.Body.Close()

	// producer may produce to orders-eu ...
	res = e.authReq(t, http.MethodPost, "/v1/topics/orders-eu/produce", map[string]any{"v": 1}, "producer", "prodpw")
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("producer produce: status = %d, want 202", res.StatusCode)
	}
	res.Body.Close()

	// ... but not consume (no consume grant) ...
	res = e.authReq(t, http.MethodGet, "/v1/topics/orders-eu/consume", nil, "producer", "prodpw")
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("producer consume: status = %d, want 403", res.StatusCode)
	}
	res.Body.Close()

	// ... and cannot delete the admin-owned topic.
	res = e.authReq(t, http.MethodDelete, "/v1/topics/orders-eu", nil, "producer", "prodpw")
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("producer delete: status = %d, want 403", res.StatusCode)
	}
	res.Body.Close()

	// A non-admin cannot manage users.
	res = e.authReq(t, http.MethodGet, "/v1/users", nil, "producer", "prodpw")
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("producer list users: status = %d, want 403", res.StatusCode)
	}
	res.Body.Close()

	// Wrong password is rejected.
	res = e.authReq(t, http.MethodGet, "/v1/topics", nil, "producer", "wrong")
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong password: status = %d, want 401", res.StatusCode)
	}
	res.Body.Close()

	// Admin can delete its own topic.
	res = e.authReq(t, http.MethodDelete, "/v1/topics/orders-eu", nil, au, ap)
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("admin delete: status = %d, want 204", res.StatusCode)
	}
	res.Body.Close()
}
