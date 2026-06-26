package httpserver

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// HeaderRequestID is set on every response carrying the request's
// correlation ID.
const HeaderRequestID = "X-Request-ID"

type contextKey int

const requestIDKey contextKey = iota

var (
	requestIDPrefix = "req-" + sanitizeRequestIDPart(requestIDPodID())
	requestIDSeq    atomic.Uint64
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
			ctx := context.WithValue(r.Context(), requestIDKey, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// requestIDFrom returns the correlation ID from ctx, or "" if absent.
func requestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

func newRequestID() string {
	seq := requestIDSeq.Add(1)
	now := time.Now().UnixNano()

	id := make([]byte, 0, len(requestIDPrefix)+1+13+1+13)
	id = append(id, requestIDPrefix...)
	id = append(id, '-')
	id = strconv.AppendInt(id, now, 36)
	id = append(id, '-')
	id = strconv.AppendUint(id, seq, 36)
	return string(id)
}

func requestIDPodID() string {
	for _, key := range [...]string{"NARAD_NODE_ID", "POD_NAME", "HOSTNAME"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return "unknown"
}

func sanitizeRequestIDPart(s string) string {
	if s == "" {
		return "unknown"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c)
		case c >= '0' && c <= '9':
			out = append(out, c)
		case c == '-' || c == '_' || c == '.':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "unknown"
	}
	return string(out)
}
