package main

// The remote child CLI against a fake API: what each verb sends, how a
// refused delete is explained, and the wait command's exit statuses.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

// fakeAPI records requests and answers from a handler per route.
type fakeAPI struct {
	mu    sync.Mutex
	calls []string
	body  map[string]map[string]any
}

func (f *fakeAPI) record(r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, r.Method+" "+r.URL.RequestURI())
	if f.body == nil {
		f.body = map[string]map[string]any{}
	}
	var b map[string]any
	if raw, _ := io.ReadAll(r.Body); len(raw) > 0 {
		_ = json.Unmarshal(raw, &b)
	}
	f.body[r.Method+" "+r.URL.Path] = b
}

func (f *fakeAPI) sent(key string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.body[key]
}

func (f *fakeAPI) callList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func newFakeAPI(t *testing.T, handle func(w http.ResponseWriter, r *http.Request)) (*fakeAPI, string) {
	t.Helper()
	f := &fakeAPI{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		w.Header().Set("Content-Type", "application/json")
		handle(w, r)
	}))
	t.Cleanup(srv.Close)
	withTempConfigDir(t)
	clearConnEnv(t)
	return f, srv.URL
}

// captureStdout runs fn and returns what it printed.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	return <-done
}

const unshipped409 = `{"error":"remote child \"orders-to-b\" has unshipped records","lag_messages":120,"lag_complete":true,"dispatch_backlog":{"narad-1":3}}`

func TestCLIAttachRemoteSendsTheRemoteBody(t *testing.T) {
	f, url := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"orders-to-b"}`))
	})
	captureStdout(t, func() {
		if err := route([]string{
			"topic", "attach", "orders", "orders-to-b", "--remote", "b", "--remote-topic", "orders",
			"--from", "unconsumed", "--lanes", "3", "--dry-run", "--server", url,
		}); err != nil {
			t.Fatalf("attach: %v", err)
		}
	})
	b := f.sent("POST /v1/topics/orders/children")
	if b["child"] != "orders-to-b" || b["remote"] != "b" || b["remote_topic"] != "orders" || b["from"] != "unconsumed" ||
		b["lanes"] != float64(3) || b["dry_run"] != true {
		t.Fatalf("attach body = %v", b)
	}
	if err := route([]string{"topic", "attach", "orders", "c", "--lanes", "2", "--server", url}); err == nil || !strings.Contains(err.Error(), "--lanes needs --remote") {
		t.Fatalf("--lanes without --remote: %v", err)
	}
}

// Only topic detach --force abandons records; topic rm -f skips the
// prompt and never sends force, and on a refusal prints the counts and
// the detach command.
func TestCLIForceMeansAbandonOnlyOnDetach(t *testing.T) {
	f, url := newFakeAPI(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/topics/orders-to-b":
			_, _ = w.Write([]byte(`{"name":"orders-to-b","parent":"orders","remote":{"name":"b","topic":"orders"}}`))
		case r.Method == http.MethodDelete && r.URL.Query().Get("force") != "true":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(unshipped409))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	err := route([]string{"topic", "rm", "orders-to-b", "-f", "--server", url})
	if err == nil {
		t.Fatal("rm -f of a stub with unshipped records succeeded")
	}
	for _, want := range []string{"120 records not yet on the remote", "3 accepted but not yet committed on narad-1", "narad topic detach orders orders-to-b --force"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("rm -f error %q does not mention %q", err, want)
		}
	}
	for _, c := range f.callList() {
		if strings.Contains(c, "force=") {
			t.Fatalf("rm -f sent force: %v", f.callList())
		}
	}

	err = route([]string{"topic", "detach", "orders", "orders-to-b", "--server", url})
	if err == nil || !strings.Contains(err.Error(), "narad topic detach orders orders-to-b --force") {
		t.Fatalf("detach without --force: %v", err)
	}
	if err := route([]string{"topic", "detach", "orders", "orders-to-b", "--force", "--server", url}); err != nil {
		t.Fatalf("detach --force: %v", err)
	}
	calls := f.callList()
	if last := calls[len(calls)-1]; last != "DELETE /v1/topics/orders/children/orders-to-b?force=true" {
		t.Fatalf("detach --force sent %q", last)
	}
}

func TestCLIPauseResumeSkip(t *testing.T) {
	f, url := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) })
	captureStdout(t, func() {
		for _, args := range [][]string{
			{"topic", "pause", "orders", "orders-to-b", "--reason", "B maintenance"},
			{"topic", "resume", "orders", "orders-to-b", "--accept-target"},
			{"topic", "skip", "orders", "orders-to-b", "--partition", "3", "--offset", "98331"},
		} {
			if err := route(append(args, "--server", url)); err != nil {
				t.Fatalf("%v: %v", args, err)
			}
		}
	})
	if b := f.sent("POST /v1/topics/orders/children/orders-to-b/pause"); b["reason"] != "B maintenance" {
		t.Fatalf("pause body %v", b)
	}
	if b := f.sent("POST /v1/topics/orders/children/orders-to-b/resume"); b["accept_target"] != true {
		t.Fatalf("resume body %v", b)
	}
	if b := f.sent("POST /v1/topics/orders/children/orders-to-b/skip"); b["partition"] != float64(3) || b["offset"] != float64(98331) {
		t.Fatalf("skip body %v", b)
	}
	if err := route([]string{"topic", "skip", "orders", "orders-to-b", "--partition", "3", "--server", url}); err == nil {
		t.Fatal("skip without --offset succeeded")
	}
}

func TestCLIWaitExitStatuses(t *testing.T) {
	var mu sync.Mutex
	state, lag := "running", int64(5)
	_, url := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := json.Marshal(map[string]any{"children": []map[string]any{{
			"name": "orders-to-b", "lag_messages": lag, "lag_complete": true, "state": state, "source_drained": lag == 0,
			"blocked_at": map[string]any{"partition": 2, "offset": 77, "state": "rejected_record"},
		}}})
		_, _ = w.Write(body)
	})
	exited := -1
	cliExit = func(code int) { exited = code }
	t.Cleanup(func() { cliExit = os.Exit })

	if err := route([]string{"topic", "wait", "orders", "orders-to-b", "--lag-zero", "--timeout", "300ms", "--interval", "50ms", "--server", url}); err == nil ||
		!strings.Contains(err.Error(), "timed out") {
		t.Fatalf("lag never reaches 0: %v, want a timeout (exit 1)", err)
	}
	mu.Lock()
	lag = 0
	mu.Unlock()
	out := captureStdout(t, func() {
		if err := route([]string{"topic", "wait", "orders", "orders-to-b", "--lag-zero", "--stable", "120ms", "--interval", "30ms", "--server", url}); err != nil {
			t.Errorf("lag 0: %v", err)
		}
	})
	if !strings.Contains(out, "reached") || exited != -1 {
		t.Fatalf("lag 0: out %q exit %d", out, exited)
	}
	if err := route([]string{"topic", "wait", "orders", "orders-to-b", "--source-drained", "--interval", "30ms", "--server", url}); err != nil {
		t.Fatalf("source drained: %v", err)
	}
	mu.Lock()
	state, lag = "rejected_record", 3
	mu.Unlock()
	out = captureStdout(t, func() {
		_ = route([]string{"topic", "wait", "orders", "orders-to-b", "--lag-zero", "--interval", "30ms", "--server", url})
	})
	if exited != exitStalled || !strings.Contains(out, "rejected_record") || !strings.Contains(out, "partition 2 offset 77") {
		t.Fatalf("stalled: out %q exit %d, want exit 2 with the state and the stuck record", out, exited)
	}
	if err := route([]string{"topic", "wait", "orders", "orders-to-b", "--server", url}); err == nil {
		t.Fatal("wait without a condition succeeded")
	}
}

func TestCLILsShowsWhereAStubSends(t *testing.T) {
	_, url := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"topics":[{"name":"orders-to-b","partitions":0,"role":"child","parent":"orders","remote":{"name":"b","topic":"orders"}}],"next_page_token":""}`))
	})
	out := captureStdout(t, func() {
		if err := route([]string{"topic", "ls", "--server", url}); err != nil {
			t.Errorf("ls: %v", err)
		}
	})
	if !strings.Contains(out, "remote b/orders") {
		t.Fatalf("ls output %q, want the stub's remote", out)
	}
}

// `topic wait` exits 2 at once on every link state the docs say only a
// fix clears (one shared list) and on paused, never on the states that
// clear on their own.
func TestCLIWaitExitsAtOnceOnEveryStateThatNeedsAFix(t *testing.T) {
	var mu sync.Mutex
	state := ""
	_, url := newFakeAPI(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := json.Marshal(map[string]any{"children": []map[string]any{{
			"name": "orders-to-b", "lag_messages": 4, "lag_complete": true, "state": state,
		}}})
		_, _ = w.Write(body)
	})
	exited := -1
	cliExit = func(code int) { exited = code }
	t.Cleanup(func() { cliExit = os.Exit })
	for _, c := range []struct {
		state   string
		stalled bool
	}{
		{"tls_failed", true}, {"target_missing", true}, {"no_batch_produce", true}, {"redirect_refused", true},
		{"auth_failed", true}, {"rejected_record", true}, {"paused", true},
		{"unavailable", false}, {"throttled", false}, {"unknown", false}, {"running", false},
	} {
		mu.Lock()
		state = c.state
		mu.Unlock()
		exited = -1
		var err error
		_ = captureStdout(t, func() {
			err = route([]string{"topic", "wait", "orders", "orders-to-b", "--lag-zero", "--timeout", "200ms", "--interval", "30ms", "--server", url})
		})
		if c.stalled && exited != exitStalled {
			t.Errorf("state %s: exit %d (%v), want %d at once", c.state, exited, err, exitStalled)
		}
		if !c.stalled && (exited != -1 || err == nil || !strings.Contains(err.Error(), "timed out")) {
			t.Errorf("state %s: exit %d (%v), want to keep waiting until the timeout", c.state, exited, err)
		}
	}
}
