package httpserver

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/security"
)

// authExempt lists the paths served without credentials: Kubernetes
// probes must never need them, and Prometheus scrapes are assumed to
// come from inside the trust boundary.
var authExempt = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/metrics": true,
}

// Auth returns the Basic-authentication middleware. A nil authenticator
// disables authentication entirely (dev mode, tests).
func Auth(auth *security.Authenticator, log *slog.Logger) Middleware {
	if auth == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if authExempt[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}
			username, password, ok := r.BasicAuth()
			if !ok {
				unauthorized(w)
				return
			}
			rec, err := auth.Verify(r.Context(), username, password)
			switch {
			case err == nil:
				// The ServeMux records the matched route pattern on the
				// request it receives. We must give it a context-augmented
				// copy to carry the identity, so copy the resolved pattern
				// back onto the caller's request — otherwise outer
				// middleware (metrics route labelling) reads an empty
				// pattern and buckets every authenticated request under
				// "unmatched".
				authed := r.WithContext(security.WithIdentity(r.Context(), rec))
				next.ServeHTTP(w, authed)
				r.Pattern = authed.Pattern
			case errors.Is(err, security.ErrThrottled):
				writeAuthError(w, http.StatusTooManyRequests, "too many failed authentication attempts")
			case errors.Is(err, security.ErrUnauthorized):
				unauthorized(w)
			default:
				log.Error("authentication store failure", "err", err)
				writeAuthError(w, http.StatusInternalServerError, "authentication unavailable")
			}
		})
	}
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="narad"`)
	writeAuthError(w, http.StatusUnauthorized, "authentication required")
}

// writeAuthError mirrors the handlers package's `{"error": msg}` body
// shape so clients see one error format regardless of which layer
// rejected the request.
func writeAuthError(w http.ResponseWriter, status int, msg string) {
	body := make([]byte, 0, len(msg)+14)
	body = append(body, `{"error":`...)
	body = strconv.AppendQuote(body, msg)
	body = append(body, "}\n"...)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
