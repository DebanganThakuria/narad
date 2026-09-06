package httpserver

import (
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/debanganthakuria/narad/internal/security"
)

// inFlightLimiter caps concurrent requests per caller identity. Each
// GET /consume?wait= pins a goroutine, a connection, and on non-owner
// nodes a forwarded RPC stream slot for up to max_consume_wait; with no
// per-caller bound one authenticated client could open tens of
// thousands of long-polls on a node. The identity is the authenticated
// username, or the client IP when security is off.
type inFlightLimiter struct {
	max    int
	mu     sync.Mutex
	counts map[string]int
}

// newInFlightLimiter returns a limiter allowing max concurrent requests
// per identity; max <= 0 disables limiting (wrap returns the handler).
func newInFlightLimiter(max int) *inFlightLimiter {
	if max <= 0 {
		return nil
	}
	return &inFlightLimiter{max: max, counts: make(map[string]int)}
}

// wrap gates next: a request beyond the identity's cap is answered 429
// without reaching it.
func (l *inFlightLimiter) wrap(next http.Handler) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := requestIdentity(r)
		if !l.acquire(key) {
			writeTooManyInFlight(w, l.max)
			return
		}
		defer l.release(key)
		next.ServeHTTP(w, r)
	})
}

func (l *inFlightLimiter) acquire(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] >= l.max {
		return false
	}
	l.counts[key]++
	return true
}

func (l *inFlightLimiter) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] <= 1 {
		delete(l.counts, key)
		return
	}
	l.counts[key]--
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
	msg := "too many in-flight consume requests for this identity (limit " + strconv.Itoa(max) + " per node)"
	body := make([]byte, 0, len(msg)+14)
	body = append(body, `{"error":`...)
	body = strconv.AppendQuote(body, msg)
	body = append(body, "}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(body)
}
