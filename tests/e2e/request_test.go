package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// ---- request helpers -------------------------------------------------------

func (e *env) url(path string) string { return e.Server.URL + path }

func (e *env) get(path string) *http.Response {
	e.t.Helper()
	return getJSON(e.t, e.url(path))
}

// post issues a JSON POST. Successful topic creations additionally block
// until the controller has assigned every partition — creation returns
// before assignments propagate, and without the wait the test's next
// produce or consume races the reconcile loop.
func (e *env) post(path string, body any) *http.Response {
	e.t.Helper()
	resp := jsonReq(e.t, http.MethodPost, e.url(path), body)
	if path == "/v1/topics" && resp.StatusCode == http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var created topic.Topic
		if err := json.Unmarshal(raw, &created); err == nil {
			e.awaitPartitionAssignments(created.Name, created.Partitions)
		}
		resp.Body = io.NopCloser(bytes.NewReader(raw))
	}
	return resp
}

func (e *env) patch(path string, body any) *http.Response {
	e.t.Helper()
	return jsonReq(e.t, http.MethodPatch, e.url(path), body)
}

func (e *env) del(path string) *http.Response {
	e.t.Helper()
	return jsonReq(e.t, http.MethodDelete, e.url(path), nil)
}

// rawPost sends a raw string body.
func (e *env) rawPost(path, rawBody string) *http.Response {
	e.t.Helper()
	return rawReq(e.t, http.MethodPost, e.url(path), []byte(rawBody))
}

// jsonReq sends a JSON-encoded body with the given method and URL.
func jsonReq(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// getJSON issues a GET and returns the response.
func getJSON(t *testing.T, url string) *http.Response {
	t.Helper()
	return jsonReq(t, http.MethodGet, url, nil)
}

// rawReq sends raw bytes.
func rawReq(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// ---- response helpers ------------------------------------------------------

// readJSON decodes the response body into T and closes the body.
func readJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return v
}

// readError extracts the "error" field from a JSON error response.
func readError(t *testing.T, resp *http.Response) string {
	t.Helper()
	return readJSON[map[string]string](t, resp)["error"]
}

// readBody drains and returns the response body as a string.
func readBody(resp *http.Response) string {
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func expectStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("status: got %d, want %d (body: %s)", resp.StatusCode, want, readBody(resp))
	}
}

func expectOK(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusOK)
}

func expectBadRequest(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusBadRequest)
}

func expectNotFound(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusNotFound)
}

func expectConflict(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusConflict)
}
