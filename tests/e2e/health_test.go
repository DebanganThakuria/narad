package e2e

import (
	"net/http"
	"testing"
)

func TestHealthz_AlwaysOK(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}

	body := readJSON[map[string]string](t, resp)
	if body["status"] != "ok" {
		t.Errorf("status: got %q want %q", body["status"], "ok")
	}
}

func TestReadyz_OkWhenBrokerReady(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/readyz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}

	body := readJSON[map[string]string](t, resp)
	if body["status"] != "ready" {
		t.Errorf("status: got %q want %q", body["status"], "ready")
	}
}
