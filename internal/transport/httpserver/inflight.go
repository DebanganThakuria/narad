package httpserver

import (
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/security"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// inFlightLimiter caps concurrent requests per caller identity. Each
// GET /consume?wait= pins a goroutine, a connection, and on non-owner
// nodes a forwarded RPC stream slot for up to max_consume_wait; with no
// per-caller bound one authenticated client could open tens of
// thousands of long-polls on a node. The identity is the authenticated
// username, or the client IP when security is off.
type inFlightLimiter struct {
	max int
	// what names the requests it counts in its 429 ("consume").
	what   string
	mu     sync.Mutex
	counts map[string]int
	// reading counts, per identity, the requests a gate admitted that
	// have not raised their hold yet: they are reading or decoding their
	// bodies (see gate).
	reading map[string]int
}

// newInFlightLimiter returns a limiter allowing max concurrent consume
// requests per identity; max <= 0 disables limiting (wrap returns the
// handler).
func newInFlightLimiter(max int) *inFlightLimiter {
	return newInFlightLimiterFor(max, "consume")
}

// newInFlightLimiterFor is newInFlightLimiter for the requests what
// names.
func newInFlightLimiterFor(max int, what string) *inFlightLimiter {
	if max <= 0 {
		return nil
	}
	return &inFlightLimiter{max: max, what: what, counts: make(map[string]int), reading: make(map[string]int)}
}

// wrap gates next: a request beyond the identity's cap is answered 429
// without reaching it. weight, when not nil, is how much of the cap a
// request takes (a batch consume of N records takes N); it is clamped
// to [1, max], so a request the cap could never hold is still served
// when it is the identity's only one. nil counts every request as 1.
func (l *inFlightLimiter) wrap(next http.Handler, weight func(*http.Request) int) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := requestIdentity(r)
		n := 1
		if weight != nil {
			n = min(max(weight(r), 1), l.max)
		}
		if !l.acquireN(key, n) {
			writeTooManyInFlightFor(w, l.what, l.max)
			return
		}
		defer l.releaseN(key, n)
		next.ServeHTTP(w, r)
	})
}

// gate is wrap for a handler that learns a request's weight only from
// its body (a batch produce weighs its message count). The handler takes
// the gate before it reads the body and raises the hold to the weight
// once it knows it (see httpmessaging.InFlightGate). Nil when limiting
// is off.
//
// A request that has not raised its hold yet counts in reading, and is
// admitted only while the identity's weight in flight plus its requests
// still reading stay within the cap, so the cap bounds the bodies being
// read and decoded as well as the weight admitted. A raise is held to
// the weight in flight alone, clamped as wrap clamps it: two requests
// that both read their bodies are not refused together for each other's
// reading slot, and a lone request larger than the cap is still served.
func (l *inFlightLimiter) gate() httpmessaging.InFlightGate {
	if l == nil {
		return nil
	}
	return func(w http.ResponseWriter, r *http.Request) (httpmessaging.InFlightHold, bool) {
		key := requestIdentity(r)
		if !l.beginRead(key) {
			writeTooManyInFlightFor(w, l.what, l.max)
			return nil, false
		}
		return &inFlightHold{l: l, key: key}, true
	}
}

// inFlightHold is one gated request: a reading slot until Raise
// succeeds, n of the identity's budget after.
type inFlightHold struct {
	l   *inFlightLimiter
	key string
	n   int
}

func (h *inFlightHold) Raise(w http.ResponseWriter, n int) bool {
	n = min(max(n, 1), h.l.max)
	if !h.l.raise(h.key, n) {
		writeTooManyInFlightFor(w, h.l.what, h.l.max)
		return false
	}
	h.n = n
	return true
}

func (h *inFlightHold) Release() {
	if h.n == 0 {
		h.l.endRead(h.key)
		return
	}
	h.l.releaseN(h.key, h.n)
}

// beginRead admits one more request of key to read its body.
func (l *inFlightLimiter) beginRead(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key]+l.reading[key] >= l.max {
		return false
	}
	l.reading[key]++
	return true
}

// raise turns one of key's reading slots into n of its budget.
func (l *inFlightLimiter) raise(key string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key]+n > l.max {
		return false
	}
	l.counts[key] += n
	l.dropReadLocked(key)
	return true
}

// endRead gives back a reading slot that was never raised.
func (l *inFlightLimiter) endRead(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropReadLocked(key)
}

func (l *inFlightLimiter) dropReadLocked(key string) {
	if l.reading[key] <= 1 {
		delete(l.reading, key)
		return
	}
	l.reading[key]--
}

func (l *inFlightLimiter) acquire(key string) bool { return l.acquireN(key, 1) }

func (l *inFlightLimiter) release(key string) { l.releaseN(key, 1) }

func (l *inFlightLimiter) acquireN(key string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key]+n > l.max {
		return false
	}
	l.counts[key] += n
	return true
}

func (l *inFlightLimiter) releaseN(key string, n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] <= n {
		delete(l.counts, key)
		return
	}
	l.counts[key] -= n
}

// requestIdentity keys the cap: the authenticated user when there is
// one, else the client IP.
func requestIdentity(r *http.Request) string {
	if id, ok := security.IdentityFrom(r.Context()); ok && id.Username != "" {
		return "user:" + id.Username
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

func writeTooManyInFlight(w http.ResponseWriter, max int) {
	writeTooManyInFlightFor(w, "consume", max)
}

func writeTooManyInFlightFor(w http.ResponseWriter, what string, max int) {
	msg := "too many in-flight " + what + " requests for this identity (limit " + strconv.Itoa(max) + " per node)"
	body := make([]byte, 0, len(msg)+14)
	body = append(body, `{"error":`...)
	body = topic.AppendJSONQuoted(body, msg)
	body = append(body, "}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(body)
}
