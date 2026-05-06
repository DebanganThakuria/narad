package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"
)

// HeaderRequestID is set on every response carrying the request's
// correlation ID.
const HeaderRequestID = "X-Request-ID"

// ctxKey is the unexported key type for storing per-request values in
// context.Context, per the standard convention.
type ctxKey int

const (
	ctxRequestID ctxKey = iota
)

// RequestID generates (or accepts) a correlation ID and propagates it
// via both the response header and the request context.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderRequestID)
			if id == "" {
				id = newRequestID()
			}
			w.Header().Set(HeaderRequestID, id)
			ctx := context.WithValue(r.Context(), ctxRequestID, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequestIDFrom returns the correlation ID from ctx, or "" if absent.
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxRequestID).(string); ok {
		return v
	}
	return ""
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read failing is essentially impossible on a healthy
		// system; fall back to a timestamp-derived ID rather than
		// panicking the request.
		return "req-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(b[:])
}
