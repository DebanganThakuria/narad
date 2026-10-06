package sink

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// answer builds an Answer the way a lane sees one.
func answer(status int, contentType, body string, chunk int) Answer {
	rec := httptest.NewRecorder()
	if contentType != "" {
		rec.Header().Set("Content-Type", contentType)
	}
	rec.WriteHeader(status)
	_, _ = rec.WriteString(body)
	return Answer{Resp: rec.Result(), Body: []byte(body), Chunk: chunk}
}

func naradErr(status int, msg string, chunk int) Answer {
	return answer(status, "application/json", fmt.Sprintf(`{"error":%q}`+"\n", msg), chunk)
}

type refused struct{}

func (refused) Error() string { return "refused" }

func testClassifier() Classifier {
	return Classifier{
		DestinationRefused: func(err error) (string, bool) {
			if errors.As(err, new(refused)) {
				return "address", true
			}
			return "", false
		},
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// Every row of ch. 6.5, in the order the rows apply.
func TestClassifyEveryRow(t *testing.T) {
	c := testClassifier()
	for _, tc := range []struct {
		name     string
		a        Answer
		action   Action
		state    string
		class    string
		index    int
		shrink   bool
		ambig    bool
		retryAft time.Duration
	}{
		{name: "accepted", a: answer(202, "application/json", `{"accepted":5}`, 5), action: ActCommitted},
		{name: "accepted count differs", a: answer(202, "application/json", `{"accepted":4}`, 5), action: ActRetry, state: topic.RemoteStateUnavailable, ambig: true},
		{name: "message N", a: naradErr(400, "message 3: invalid argument: schema", 5), action: ActResendPrefix, state: topic.RemoteStateRejectedRecord, index: 3},
		{name: "read body", a: naradErr(400, "read body: i/o timeout", 5), action: ActRetry, state: topic.RemoteStateUnavailable, shrink: true},
		{name: "400 without index", a: naradErr(400, "invalid json: bad", 5), action: ActBisect, state: topic.RemoteStateRejectedRecord},
		{name: "409 delay child", a: naradErr(409, "direct produce to a delayed child topic is not allowed", 5), action: ActBlock, state: topic.RemoteStateRejectedRecord},
		{name: "413", a: naradErr(413, "request body too large", 5), action: ActSplitTooLarge, state: topic.RemoteStateRecordTooLarge},
		{name: "413 message N", a: naradErr(413, "message 2: message too large", 5), action: ActSplitTooLarge, state: topic.RemoteStateRecordTooLarge, index: 2},
		{name: "401", a: naradErr(401, "unauthorized", 5), action: ActGate, state: topic.RemoteStateAuthFailed},
		{name: "403", a: naradErr(403, "produce not allowed on this topic", 5), action: ActStall, state: topic.RemoteStateForbidden},
		{name: "404 JSON", a: naradErr(404, "topic not found", 5), action: ActStall, state: topic.RemoteStateTargetMissing},
		{name: "Go 404", a: answer(404, "text/plain; charset=utf-8", "404 page not found\n", 5), action: ActResolveRoute, state: topic.RemoteStateNoBatchProduce},
		{name: "other plain 404", a: answer(404, "text/plain", "not here", 5), action: ActRetry, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEdge},
		{name: "408", a: naradErr(408, "timeout", 5), action: ActRetry, state: topic.RemoteStateUnavailable, ambig: true},
		{name: "500", a: naradErr(500, "produce failed", 5), action: ActRetry, state: topic.RemoteStateUnavailable, ambig: true},
		{name: "503 HTML", a: answer(503, "text/html", "<html>busy</html>", 5), action: ActRetry, state: topic.RemoteStateUnavailable, ambig: true},
		{name: "429", a: func() Answer {
			a := naradErr(429, "slow down", 5)
			a.Resp.Header.Set("Retry-After", "7")
			return a
		}(), action: ActGate, state: topic.RemoteStateThrottled, retryAft: 7 * time.Second},
		{name: "429 capped", a: func() Answer {
			a := naradErr(429, "slow down", 5)
			a.Resp.Header.Set("Retry-After", "600")
			return a
		}(), action: ActGate, state: topic.RemoteStateThrottled, retryAft: MaxRetryAfter},
		{name: "302", a: answer(302, "", "", 5), action: ActStall, state: topic.RemoteStateRedirectRefused},
		{name: "edge 403 HTML", a: answer(403, "text/html", "<h1>Forbidden</h1>", 5), action: ActRetry, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEdge},
		{name: "edge 401 plain", a: answer(401, "text/plain", "no", 5), action: ActRetry, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEdge},
		{name: "edge 400 plain", a: answer(400, "text/plain", "bad request", 5), action: ActRetry, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEdge},
		{name: "compressed invalid json", a: func() Answer {
			a := naradErr(400, "invalid json: invalid character", 5)
			a.Compressed = true
			return a
		}(), action: ActUncompressed, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEncoding},
		{name: "compressed 415", a: func() Answer {
			a := naradErr(415, "unsupported", 5)
			a.Compressed = true
			return a
		}(), action: ActUncompressed, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEncoding},
		{name: "uncompressed 415", a: naradErr(415, "unsupported", 5), action: ActRetry, state: topic.RemoteStateUnavailable, class: topic.RemoteClassEdge},
		{name: "destination refused", a: Answer{Err: fmt.Errorf("dial: %w", refused{}), Chunk: 5}, action: ActStall, state: topic.RemoteStateDestinationRefused},
		{name: "tls", a: Answer{Err: fmt.Errorf("tls: %w", x509.UnknownAuthorityError{}), Chunk: 5}, action: ActGate, state: topic.RemoteStateTLSFailed},
		{name: "timeout", a: Answer{Err: fmt.Errorf("post: %w", context.DeadlineExceeded), Chunk: 5}, action: ActRetry, state: topic.RemoteStateUnavailable, shrink: true, ambig: true},
		{name: "net timeout", a: Answer{Err: timeoutErr{}, Chunk: 5}, action: ActRetry, state: topic.RemoteStateUnavailable, shrink: true, ambig: true},
		{name: "reset", a: Answer{Err: errors.New("connection reset by peer"), Chunk: 5}, action: ActRetry, state: topic.RemoteStateUnavailable, ambig: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := c.Classify(tc.a)
			class := tc.class
			if class == "" {
				class = tc.state
			}
			index := tc.index
			if tc.action != ActResendPrefix && !(tc.action == ActSplitTooLarge && tc.index > 0) {
				index = -1
			}
			if v.Action != tc.action || v.State != tc.state || v.Class != class || v.Index != index ||
				v.Shrink != tc.shrink || v.Ambiguous != tc.ambig || v.RetryAfter != tc.retryAft {
				t.Fatalf("verdict = %+v, want action %d state %q class %q index %d shrink %v ambiguous %v retry %s",
					v, tc.action, tc.state, class, index, tc.shrink, tc.ambig, tc.retryAft)
			}
		})
	}
}

func TestMessageIndexUsesOnlyTheInteger(t *testing.T) {
	for msg, want := range map[string]int{
		"message 0: message required":  0,
		"message 17: schema violation": 17,
		"message -1: x":                -1,
		"message x: y":                 -1,
		"messages required":            -1,
		"message 12":                   -1,
	} {
		if got := messageIndex(msg); got != want {
			t.Errorf("messageIndex(%q) = %d, want %d", msg, got, want)
		}
	}
}

func TestResolveRoute(t *testing.T) {
	listing := answer(200, "application/json", `{"parent":"orders","children":[]}`, 0)
	if v := ResolveRoute(listing.Resp, listing.Body, nil); v.State != topic.RemoteStateNoBatchProduce || v.Action != ActStall {
		t.Fatalf("listing answered 200: %+v, want no_batch_produce", v)
	}
	notListing := answer(200, "text/html", "<html></html>", 0)
	if v := ResolveRoute(notListing.Resp, notListing.Body, nil); v.Class != topic.RemoteClassEdge {
		t.Fatalf("an HTML 200: %+v, want edge", v)
	}
	missing := answer(404, "text/plain", "gone", 0)
	if v := ResolveRoute(missing.Resp, missing.Body, nil); v.Class != topic.RemoteClassEdge {
		t.Fatalf("a failing listing: %+v, want edge", v)
	}
	if v := ResolveRoute(nil, nil, errors.New("down")); v.Class != topic.RemoteClassEdge {
		t.Fatalf("an unreachable listing: %+v, want edge", v)
	}
}

func TestParseListing(t *testing.T) {
	for _, tc := range []struct {
		body    string
		ok      bool
		serves  bool
		id      string
		remotes int
	}{
		{`{"parent":"o","children":[]}`, true, false, "", 0},
		{`{"parent":"o","parent_id":"abc","children":[{"name":"c"},{"name":"r","remote":{"name":"x","topic":"y"}}]}`, true, true, "abc", 1},
		{`{"parent":"o","parent_id":"abc","children":[{"name":"c","remote":null}]}`, true, true, "abc", 0},
		{`{"children":[]}`, false, false, "", 0},
		{`[]`, false, false, "", 0},
	} {
		v, ok := parseListing([]byte(tc.body))
		if ok != tc.ok || v.ServesIDs != tc.serves || v.ParentID != tc.id || v.RemoteChildren != tc.remotes {
			t.Errorf("parseListing(%s) = %+v, %v", tc.body, v, ok)
		}
	}
}

func TestCheckIntervalJitter(t *testing.T) {
	for range 200 {
		d := CheckInterval(60_000)
		if d < 48*time.Second || d > 72*time.Second {
			t.Fatalf("interval %s outside 60s +-20%%", d)
		}
	}
	if d := CheckInterval(0); d < 800*time.Millisecond {
		t.Fatalf("interval %s below the 1s floor's jitter", d)
	}
}

// A plain http.Handler answer read back through the classifier's shape
// helpers, not a hand-typed status: the Go 404 comes from a real mux.
func TestClassifyGoNotFoundFromARealMux(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/topics/{t}/children", func(w http.ResponseWriter, _ *http.Request) {})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce/batch", nil))
	res := rec.Result()
	body := rec.Body.Bytes()
	if v := testClassifier().Classify(Answer{Resp: res, Body: body, Chunk: 1}); v.Action != ActResolveRoute {
		t.Fatalf("Go's own 404: %+v, want resolve through the listing", v)
	}
}
