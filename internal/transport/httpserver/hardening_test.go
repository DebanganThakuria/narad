package httpserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security"
)

// The server used to run with Go's defaults for header size (1 MiB)
// and header read time (the whole 10 s read timeout).
func TestServerAppliesHeaderLimits(t *testing.T) {
	cfg := config.Default().HTTP
	s := New(cfg, http.NotFoundHandler(), newTestLogger())
	if s.srv.MaxHeaderBytes != cfg.MaxHeaderBytes || cfg.MaxHeaderBytes != 64<<10 {
		t.Fatalf("MaxHeaderBytes = %d, want configured %d (default 64 KiB)", s.srv.MaxHeaderBytes, cfg.MaxHeaderBytes)
	}
	if s.srv.ReadHeaderTimeout != maxReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", s.srv.ReadHeaderTimeout, maxReadHeaderTimeout)
	}
	if got := readHeaderTimeout(time.Second); got != time.Second {
		t.Fatalf("readHeaderTimeout(1s) = %s, want the shorter read timeout", got)
	}
}

// With a connection cap, the (cap+1)th client waits in the accept
// backlog until an earlier connection closes, instead of getting a
// goroutine of its own.
func TestLimitListenerCapsConcurrentConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln = limitListener(ln, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
	go srv.Serve(ln)
	t.Cleanup(func() { _ = srv.Close() })

	// Hold one connection open (accepted, request not yet sent).
	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	done := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + ln.Addr().String() + "/")
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("second connection was served while the cap was held (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	_ = first.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second request after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second connection never served after the first closed")
	}
	if limitListener(ln, 0) != ln {
		t.Fatal("limitListener(0) must return the listener unchanged")
	}
}

// A browser can send POST/PUT/PATCH cross-origin without a preflight
// only with a "simple" content type and no custom headers, attaching
// cached Basic credentials. Those requests are refused; API clients
// pass with an API content type or the client header.
func TestRequireAPIContentTypeBlocksSimpleCrossSiteRequests(t *testing.T) {
	h := RequireAPIContentType()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	do := func(method, contentType string, headers map[string]string) int {
		req := httptest.NewRequest(method, "/v1/topics", strings.NewReader("{}"))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res.Code
	}
	for _, simple := range []string{"", "text/plain", "application/x-www-form-urlencoded", "multipart/form-data; boundary=x", "text/plain;charset=UTF-8"} {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
			if got := do(method, simple, nil); got != http.StatusUnsupportedMediaType {
				t.Fatalf("%s with Content-Type %q: status = %d, want 415", method, simple, got)
			}
		}
	}
	for _, ok := range []string{"application/json", "application/json; charset=utf-8", "application/octet-stream"} {
		if got := do(http.MethodPost, ok, nil); got != http.StatusOK {
			t.Fatalf("POST with Content-Type %q: status = %d, want 200", ok, got)
		}
	}
	if got := do(http.MethodPost, "", map[string]string{ClientHeader: "narad-cli"}); got != http.StatusOK {
		t.Fatalf("bodyless POST with %s: status = %d, want 200", ClientHeader, got)
	}
	// Read methods and DELETE (always preflighted, no body) are untouched.
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodDelete, http.MethodOptions} {
		if got := do(method, "", nil); got != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", method, got)
		}
	}
}

// The guard is wired into the router for the state-changing routes.
func TestRouterRefusesFormEncodedTopicCreate(t *testing.T) {
	router := NewRouter(newTestSet(&fakeBroker{}), newTestLogger(), nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", strings.NewReader(`{"name":"x"}`))
	req.Header.Set("Content-Type", "text/plain")
	res := httptest.NewRecorder()
	router.ServeHTTP(res, req)
	if res.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415: %s", res.Code, res.Body)
	}
}

// consumeBroker parks every consume until released so the test can hold
// requests in flight.
type consumeBroker struct {
	fakeBroker
	release chan struct{}
	started chan struct{}
}

func (b *consumeBroker) Consume(context.Context, string, messaging.ConsumeOpts) (topic.Message, bool, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-b.release
	return topic.Message{}, false, nil
}

func TestConsumeInFlightCapPerIdentity(t *testing.T) {
	br := &consumeBroker{release: make(chan struct{}), started: make(chan struct{}, 8)}
	hash, _ := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	auth := security.New(staticUserStore{users: map[string]user.User{
		"alice": {Username: "alice", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionAdmin}}},
		"bob":   {Username: "bob", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionAdmin}}},
	}}, newTestLogger())
	router := NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, auth, RouterOptions{ConsumeInFlightPerIdentity: 1})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	get := func(userName string) (*http.Response, error) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/topics/orders/consume", nil)
		req.SetBasicAuth(userName, "pw")
		return http.DefaultClient.Do(req)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if resp, err := get("alice"); err == nil {
			resp.Body.Close()
		}
	}()
	<-br.started

	resp, err := get("alice")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second in-flight consume for alice: status = %d, want 429: %s", resp.StatusCode, body)
	}

	// Another identity has its own budget.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if resp, err := get("bob"); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-br.started:
	case <-time.After(2 * time.Second):
		t.Fatal("bob's consume was held by alice's cap")
	}

	close(br.release)
	wg.Wait()
	// Slots are released when requests finish.
	resp, err = get("alice")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatal("alice's slot was not released after her consume finished")
	}
}

func TestInFlightLimiterKeysOnIPWithoutIdentity(t *testing.T) {
	l := newInFlightLimiter(1)
	a := httptest.NewRequest(http.MethodGet, "/", nil)
	a.RemoteAddr = "10.0.0.1:1234"
	b := httptest.NewRequest(http.MethodGet, "/", nil)
	b.RemoteAddr = "10.0.0.1:9999"
	if requestIdentity(a) != requestIdentity(b) {
		t.Fatal("same client IP with different ports must share a budget")
	}
	if !l.acquire(requestIdentity(a)) || l.acquire(requestIdentity(b)) {
		t.Fatal("second request from the same IP must be refused at cap 1")
	}
	l.release(requestIdentity(a))
	if !l.acquire(requestIdentity(b)) {
		t.Fatal("slot not released")
	}
	if newInFlightLimiter(0) != nil {
		t.Fatal("cap 0 must disable the limiter")
	}
}

// /metrics names every topic; with auth on it now needs credentials by
// default, may be opted out, and disappears from the API port when a
// dedicated listener serves it.
func TestMetricsAuthPolicy(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	auth := security.New(staticUserStore{users: map[string]user.User{
		"alice": {Username: "alice", PasswordHash: hash},
	}}, newTestLogger())
	build := func(opts RouterOptions) http.Handler {
		reg := prometheus.NewRegistry()
		m := metrics.New(reg)
		return NewRouterWithOptions(newTestSet(&fakeBroker{}), newTestLogger(), m, reg, auth, opts)
	}
	scrape := func(h http.Handler, withCreds bool) int {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if withCreds {
			req.SetBasicAuth("alice", "pw")
		}
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res.Code
	}

	secure := build(DefaultRouterOptions())
	if got := scrape(secure, false); got != http.StatusUnauthorized {
		t.Fatalf("default /metrics without credentials: status = %d, want 401", got)
	}
	if got := scrape(secure, true); got != http.StatusOK {
		t.Fatalf("default /metrics with credentials: status = %d, want 200", got)
	}
	open := build(RouterOptions{MetricsOnAPI: true, MetricsRequireAuth: false})
	if got := scrape(open, false); got != http.StatusOK {
		t.Fatalf("opted-out /metrics without credentials: status = %d, want 200", got)
	}
	// Probes stay credential-free either way.
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()
	secure.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("/healthz without credentials: status = %d, want 200", res.Code)
	}
	off := build(RouterOptions{MetricsOnAPI: false, MetricsRequireAuth: true})
	if got := scrape(off, true); got != http.StatusNotFound {
		t.Fatalf("/metrics with a dedicated listener configured: status = %d on the API port, want 404", got)
	}
}
