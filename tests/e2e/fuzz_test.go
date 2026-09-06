package e2e

// HTTP-layer fuzz targets. Each fuzz process starts ONE secured
// single-node broker (Basic auth and RBAC on, a seeded admin, a fixture
// topic with messages in it) and throws requests at the full router
// stack: metrics, recover, auth, cross-site guard, mux, handlers. The
// invariants:
//
//   - the server never panics (the Recover middleware would turn one
//     into a 500, which the 5xx check catches; a panic outside it would
//     kill the process, which the fuzzer catches);
//   - malformed input is never answered with a 5xx (a 4xx is right; the
//     only 5xx tolerated is a 503 whose body names a genuine
//     unavailability, so a dead control plane cannot hide as noise);
//   - a request never runs past a bound;
//   - the goroutine count returns to its baseline after every request.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	fuzzTopic          = "fz"
	fuzzMaxConsumeWait = 100 * time.Millisecond
	// fuzzRequestBound is how long one request may take before the
	// harness declares it hung: the consume wait ceiling plus the
	// bcrypt work a fresh credential may cost, with generous margin.
	fuzzRequestBound = 10 * time.Second
	// fuzzGoroutineSlack absorbs the transient goroutines the broker's
	// background loops (Raft, dispatcher, fan-out, flushers) spawn.
	fuzzGoroutineSlack = 48
	// fuzzMaxTopics and fuzzMaxUsers bound what the fuzzer may create
	// before the harness deletes its oldest creations: each topic keeps
	// partition logs and goroutines alive, and the goroutine check
	// needs a stable baseline.
	fuzzMaxTopics = 8
	fuzzMaxUsers  = 16
)

// fuzzHarness is the one broker a fuzz process shares across inputs.
type fuzzHarness struct {
	env      *env
	router   http.Handler
	baseline int
	// receipt is a real receipt handle from the fixture topic, for seeds.
	receipt string

	mu     sync.Mutex
	topics []string
	users  []string
}

// newFuzzHarness builds a harness for one fuzz target; the env is torn
// down by f.Cleanup when the target finishes.
func newFuzzHarness(f *testing.F) *fuzzHarness {
	f.Helper()
	e := newTestEnv(f, withSecurity(), withMetrics(), withMaxConsumeWait(fuzzMaxConsumeWait))
	h := &fuzzHarness{env: e, router: e.Server.Config.Handler}
	{

		// Fixture topic with a couple of messages, one of them consumed so
		// a real receipt handle exists for the seeds.
		resp := e.authReq(f, http.MethodPost, "/v1/topics", map[string]any{"name": fuzzTopic, "partitions": 3}, e.adminUser, e.adminPass)
		if resp.StatusCode != http.StatusCreated {
			f.Fatalf("create fixture topic: %d %s", resp.StatusCode, readBody(resp))
		}
		resp.Body.Close()
		// The fixture deadlines are generous: a fuzz worker may be
		// (re)started on a machine already saturated by other workers.
		assigned := false
		for i := 0; i < 10 && !assigned; i++ {
			assigned = e.awaitPartitionAssignments(fuzzTopic, 3)
		}
		if !assigned {
			f.Fatal("fixture topic never got assignments")
		}
		for i := range 4 {
			rec := h.do(f, http.MethodPost, "/v1/topics/"+fuzzTopic+"/produce?key=k"+string(rune('a'+i)), []byte(`{"n":1}`), e.adminUser, e.adminPass)
			if rec.Code != http.StatusAccepted {
				f.Fatalf("produce fixture: %d %s", rec.Code, rec.Body.String())
			}
		}
		deadline := time.Now().Add(30 * time.Second)
		for h.receipt == "" && time.Now().Before(deadline) {
			rec := h.do(f, http.MethodGet, "/v1/topics/"+fuzzTopic+"/consume?wait=100ms", nil, e.adminUser, e.adminPass)
			if rec.Code == http.StatusOK {
				var msg struct {
					ReceiptHandle string `json:"receipt_handle"`
				}
				_ = json.Unmarshal(rec.Body.Bytes(), &msg)
				h.receipt = msg.ReceiptHandle
			}
		}
		if h.receipt == "" {
			f.Fatal("fixture message never became consumable")
		}
		// Let the background loops settle before taking the baseline.
		time.Sleep(200 * time.Millisecond)
		runtime.GC()
		h.baseline = runtime.NumGoroutine()
	}
	return h
}

// do runs one request through the router with Basic credentials.
func (h *fuzzHarness) do(tb testing.TB, method, target string, body []byte, user, pass string) *httptest.ResponseRecorder {
	tb.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	return rec
}

// allowed503 reports whether a 503 body names a genuine unavailability
// rather than a request the server should have rejected as malformed.
func allowed503(body string) bool {
	for _, s := range []string{
		"control plane temporarily unavailable",
		"acked-ahead",
		"not ready",
		"shutting down",
	} {
		if strings.Contains(body, s) {
			return true
		}
	}
	return false
}

// checkStatus enforces the no-5xx invariant.
func (h *fuzzHarness) checkStatus(t *testing.T, code int, body string, describe func() string) {
	t.Helper()
	if code < 500 {
		return
	}
	if code == http.StatusServiceUnavailable && allowed503(body) {
		return
	}
	t.Fatalf("server answered %d to %s\nresponse body: %s", code, describe(), body)
}

// checkGoroutines waits for the goroutine count to fall back to the
// baseline and fails with a dump when it does not.
func (h *fuzzHarness) checkGoroutines(t *testing.T) {
	t.Helper()
	limit := h.baseline + fuzzGoroutineSlack
	if runtime.NumGoroutine() <= limit {
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if runtime.NumGoroutine() <= limit {
			return
		}
	}
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
	t.Fatalf("goroutines did not return to baseline: %d > %d\n%s", runtime.NumGoroutine(), limit, buf.String())
}

// noteCreated remembers a topic or user the fuzzer created and deletes
// the oldest ones once the bound is reached.
func (h *fuzzHarness) noteCreated(t *testing.T, method, path string, code int, body []byte) {
	t.Helper()
	if method != http.MethodPost || code != http.StatusCreated {
		return
	}
	var created struct {
		Name     string `json:"name"`
		Username string `json:"username"`
	}
	if json.Unmarshal(body, &created) != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case path == "/v1/topics" && created.Name != "" && created.Name != fuzzTopic:
		h.topics = append(h.topics, created.Name)
		for len(h.topics) > fuzzMaxTopics {
			name := h.topics[0]
			h.topics = h.topics[1:]
			_ = h.env.Broker.DeleteTopic(context.Background(), name)
		}
	case path == "/v1/users" && created.Username != "" && created.Username != h.env.adminUser:
		h.users = append(h.users, created.Username)
		for len(h.users) > fuzzMaxUsers {
			name := h.users[0]
			h.users = h.users[1:]
			h.do(t, http.MethodDelete, "/v1/users/"+url.PathEscape(name), nil, h.env.adminUser, h.env.adminPass)
		}
	}
}

// fuzzAuthMode selects how a router-level fuzz request authenticates.
func fuzzAuthorization(mode uint8, h *fuzzHarness, user, pass, raw string) (string, bool) {
	basic := func(u, p string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(u+":"+p))
	}
	switch mode % 5 {
	case 0:
		return "", false
	case 1:
		return basic(h.env.adminUser, h.env.adminPass), true
	case 2:
		return basic(user, pass), true
	case 3:
		return basic(h.env.adminUser, pass), true
	default:
		return raw, true
	}
}

// FuzzHTTPRouter drives the router in-process with a request assembled
// from fuzzer-chosen parts. The declared Content-Length may disagree
// with the body by lengthDelta, which exercises the exact-length read
// path in handlers.ReadBody.
func FuzzHTTPRouter(f *testing.F) {
	h := newFuzzHarness(f)

	seed := func(method, target string, mode uint8, ctype, client, hdrName, hdrVal string, body string, delta int8) {
		f.Add(method, target, mode, "user", "pass", ctype, client, hdrName, hdrVal, []byte(body), delta)
	}
	seed(http.MethodGet, "/healthz", 0, "", "", "", "", "", 0)
	seed(http.MethodGet, "/readyz", 0, "", "", "", "", "", 0)
	seed(http.MethodGet, "/metrics", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics?limit=5&page_token=abc", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic, 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"?partition=1", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/schema", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/children", 1, "", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics", 1, "application/json", "", "", "", `{"name":"fz-new","partitions":1}`, 0)
	seed(http.MethodPost, "/v1/topics", 1, "application/json", "", "", "", `{"name":"fz-child","parent":"`+fuzzTopic+`","fanout_delay_ms":10}`, 0)
	seed(http.MethodPost, "/v1/topics", 1, "application/json", "", "", "", `{"name":"fz-schema","schema":{"type":"object"}}`, 0)
	seed(http.MethodPatch, "/v1/topics/"+fuzzTopic, 1, "application/json", "", "", "", `{"partitions":3}`, 0)
	seed(http.MethodPatch, "/v1/topics/"+fuzzTopic, 1, "application/json", "", "", "", `{"retention_ms":60000,"max_in_flight_per_partition":8}`, 0)
	seed(http.MethodPatch, "/v1/topics/"+fuzzTopic, 1, "application/json", "", "", "", `{"schema":{"type":"object"},"schema_base_version":0}`, 0)
	seed(http.MethodDelete, "/v1/topics/fz-new", 1, "", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/children", 1, "application/json", "", "", "", `{"child":"fz-child","delay_ms":5}`, 0)
	seed(http.MethodDelete, "/v1/topics/"+fuzzTopic+"/children/fz-child", 1, "", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/produce?key=k1", 1, "application/octet-stream", "", "", "", `{"n":2}`, 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/produce?key=k1&partition=1", 1, "", "narad-cli", "", "", "payload", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/produce", 1, "application/json", "", "", "", "payload", 3)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/produce", 1, "application/json", "", "", "", "payload", -3)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/consume?wait=50ms", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/consume?partition=0&offset=0", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/consume?partition=x&offset=-1&wait=1x", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/consume?local_only=1", 1, "", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/ack?receipt_handle="+url.QueryEscape(h.receipt), 1, "application/json", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/ack?receipt_handle="+url.QueryEscape(h.receipt)+"&extend=true", 1, "application/json", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/ack?receipt_handle=0:0:1&extend=0", 1, "application/json", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/topics/"+fuzzTopic+"/ack?receipt_handle=%ZZ", 1, "application/json", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/users", 1, "application/json", "", "", "", `{"username":"bob","password":"pw","grants":[{"action":"produce","patterns":["fz*"]}]}`, 0)
	seed(http.MethodGet, "/v1/users", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/users/admin", 1, "", "", "", "", "", 0)
	seed(http.MethodPut, "/v1/users/bob/grants", 1, "application/json", "", "", "", `{"grants":[{"action":"admin"}]}`, 0)
	seed(http.MethodPut, "/v1/users/admin/password", 1, "application/json", "", "", "", `{"current_password":"admin-secret","new_password":"admin-secret"}`, 0)
	seed(http.MethodDelete, "/v1/users/bob", 1, "", "", "", "", "", 0)
	seed(http.MethodPost, "/v1/cluster/members/test-1/decommission", 1, "application/json", "", "", "", "", 0)
	seed(http.MethodDelete, "/v1/cluster/members/test-1/decommission", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/cluster/moves", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/cluster/members", 1, "", "", "", "", "", 0)
	// Auth variants and header games.
	seed(http.MethodGet, "/v1/topics", 2, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics", 3, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics", 4, "", "", "", "Basic !!!", "", 0)
	seed(http.MethodGet, "/v1/topics", 4, "", "", "", "Bearer abc", "", 0)
	seed(http.MethodGet, "/v1/topics", 4, "", "", "", "Basic "+base64.StdEncoding.EncodeToString([]byte("no-colon")), "", 0)
	seed(http.MethodPost, "/v1/topics", 1, "text/plain", "", "", "", `{"name":"x"}`, 0)
	seed(http.MethodPost, "/v1/topics", 1, "application/json; charset=\"", "", "", "", `{"name":"x"}`, 0)
	seed(http.MethodPost, "/v1/topics", 1, "application/json", "", "Transfer-Encoding", "chunked", `{"name":"x"}`, 0)
	seed(http.MethodPost, "/v1/topics", 1, "application/json", "", "Expect", "100-continue", `{"name":"x"}`, 0)
	seed("TRACE", "/v1/topics", 1, "", "", "", "", "", 0)
	seed("", "", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/../../etc/passwd", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/%2e%2e/consume", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+strings.Repeat("a", 300)+"/consume", 1, "", "", "", "", "", 0)
	seed(http.MethodGet, "/v1/topics/"+fuzzTopic+"/consume?"+strings.Repeat("wait=1s&", 200), 1, "", "", "", "", "", 0)

	f.Fuzz(func(t *testing.T, method, target string, mode uint8, user, pass, ctype, client, hdrName, hdrVal string, body []byte, lengthDelta int8) {
		u, err := url.ParseRequestURI(target)
		if err != nil {
			u = &url.URL{Path: target}
		}
		ctx, cancel := context.WithTimeout(context.Background(), fuzzRequestBound)
		defer cancel()
		req := (&http.Request{
			Method:     method,
			URL:        u,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Host:       "fuzz.local",
			RemoteAddr: "127.0.0.1:4242",
			RequestURI: target,
			Body:       io.NopCloser(bytes.NewReader(body)),
		}).WithContext(ctx)
		req.ContentLength = int64(len(body)) + int64(lengthDelta)
		if req.ContentLength < 0 {
			req.ContentLength = -1
		}
		if ctype != "" {
			req.Header.Set("Content-Type", ctype)
		}
		if client != "" {
			req.Header.Set("X-Narad-Client", client)
		}
		if hdrName != "" {
			// Set directly so the fuzzer can use any casing or bytes.
			req.Header[hdrName] = []string{hdrVal}
		}
		if auth, ok := fuzzAuthorization(mode, h, user, pass, hdrVal); ok {
			req.Header.Set("Authorization", auth)
		}

		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.router.ServeHTTP(rec, req)
		}()
		select {
		case <-done:
		case <-time.After(fuzzRequestBound + 5*time.Second):
			t.Fatalf("request hung: %s %q", method, target)
		}

		describe := func() string {
			return fmt.Sprintf("%s %q auth=%q content-type=%q %q=%q content-length=%d body=%q",
				method, target, req.Header.Get("Authorization"), ctype, hdrName, hdrVal, req.ContentLength, body)
		}
		h.checkStatus(t, rec.Code, rec.Body.String(), describe)
		h.noteCreated(t, method, u.Path, rec.Code, rec.Body.Bytes())
		h.checkGoroutines(t)
	})
}

// FuzzHTTPRaw writes arbitrary bytes to a server connection, half-closes,
// and reads everything the server sends back: the HTTP parser, request
// framing (Content-Length versus chunked, pipelining, Expect), and the
// same handlers behind it. Every response that comes back is held to the
// no-5xx rule, and the server must close the connection within a bound.
//
// The connections are in-memory (see memconn_test.go): the same
// net/http server code runs on them, but no loopback socket is spent
// per input, which is what let the TCP version of this target exhaust
// the ephemeral port range and kill its own workers after two minutes.
func FuzzHTTPRaw(f *testing.F) {
	h := newFuzzHarness(f)
	adminAuth := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(h.env.adminUser+":"+h.env.adminPass)) + "\r\n"

	ln := newMemListener()
	srv := &http.Server{Handler: h.router}
	go func() { _ = srv.Serve(ln) }()
	f.Cleanup(func() {
		ln.Close()
		srv.Close()
	})

	f.Add([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n"))
	f.Add([]byte("GET /v1/topics HTTP/1.1\r\nHost: x\r\n" + adminAuth + "\r\n"))
	f.Add([]byte("POST /v1/topics/" + fuzzTopic + "/produce?key=a HTTP/1.1\r\nHost: x\r\n" + adminAuth + "Content-Type: application/octet-stream\r\nContent-Length: 5\r\n\r\nhello"))
	f.Add([]byte("POST /v1/topics/" + fuzzTopic + "/produce HTTP/1.1\r\nHost: x\r\n" + adminAuth + "Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"))
	f.Add([]byte("POST /v1/topics/" + fuzzTopic + "/produce HTTP/1.1\r\nHost: x\r\n" + adminAuth + "Content-Type: application/json\r\nContent-Length: 100\r\n\r\nshort"))
	f.Add([]byte("POST /v1/topics/" + fuzzTopic + "/produce HTTP/1.1\r\nHost: x\r\n" + adminAuth + "Content-Type: application/json\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"))
	f.Add([]byte("POST /v1/topics HTTP/1.1\r\nHost: x\r\n" + adminAuth + "Content-Type: application/json\r\nExpect: 100-continue\r\nContent-Length: 12\r\n\r\n{\"name\":\"q\"}"))
	f.Add([]byte("GET /v1/topics/" + fuzzTopic + "/consume?wait=50ms HTTP/1.1\r\nHost: x\r\n" + adminAuth + "\r\nGET /healthz HTTP/1.1\r\nHost: x\r\n\r\n"))
	f.Add([]byte("GET /v1/topics HTTP/1.0\r\n" + adminAuth + "\r\n"))
	f.Add([]byte("GET /v1/topics HTTP/1.1\r\nHost: x\r\nAuthorization: Basic " + strings.Repeat("A", 9000) + "\r\n\r\n"))
	f.Add([]byte("GET / HTTP/1.1\r\nHost: x\r\nX-Narad-Client: " + strings.Repeat("z", 70000) + "\r\n\r\n"))
	f.Add([]byte("BREW /v1/topics HTTP/1.1\r\nHost: x\r\n\r\n"))
	f.Add([]byte("GET /v1/topics HTTP/9.9\r\n\r\n"))
	f.Add([]byte("\x00\x01\x02garbage"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		conn, err := ln.dial()
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(fuzzRequestBound))
		// A write error means the server already closed on us, which it
		// may legitimately do mid-request on garbage; whatever it sent
		// before closing is still read and checked below.
		_, _ = conn.Write(data)
		_ = conn.(interface{ CloseWrite() error }).CloseWrite()
		raw, err := io.ReadAll(conn)
		if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("server held the connection open past %s for input %q", fuzzRequestBound, data)
		}

		br := bufio.NewReader(bytes.NewReader(raw))
		for {
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				break
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// net/http itself answers an unsupported protocol version or
			// transfer encoding with 505 or 501 before any handler runs;
			// those are the right answers, not server failures.
			if resp.StatusCode == http.StatusHTTPVersionNotSupported || resp.StatusCode == http.StatusNotImplemented {
				continue
			}
			h.checkStatus(t, resp.StatusCode, string(body), func() string { return "raw request " + string(data) })
			if resp.StatusCode == http.StatusCreated {
				h.noteCreated(t, http.MethodPost, "/v1/topics", resp.StatusCode, body)
				h.noteCreated(t, http.MethodPost, "/v1/users", resp.StatusCode, body)
			}
		}
		h.checkGoroutines(t)
	})
}
