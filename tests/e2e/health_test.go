package e2e

import (
	"net/http"
	"testing"
)

func TestHealthz_AlwaysOK(t *testing.T) {
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}

	var body map[string]string
	decodeJSON(t, resp, &body)
	if body["status"] != "ok" {
		t.Errorf("status: got %q want %q", body["status"], "ok")
	}
}

func TestReadyz_OkWhenBrokerReady(t *testing.T) {
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/readyz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}

	var body map[string]string
	decodeJSON(t, resp, &body)
	if body["status"] != "ready" {
		t.Errorf("status: got %q want %q", body["status"], "ready")
	}
}
