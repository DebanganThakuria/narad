package httpserver

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// AccessLog emits one structured log line per request. Status and
// payload size are captured via the recorder wrapper so the line is
// accurate even when handlers don't call WriteHeader explicitly.
func AccessLog(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if log == nil {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			rec := &recorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			duration := time.Since(start)
			level := accessLogLevel(r, rec.status)
			if !log.Enabled(r.Context(), level) {
				return
			}

			log.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", duration),
				slog.String("remote", r.RemoteAddr),
				slog.String("request_id", requestIDFrom(r.Context())),
			)
		})
	}
}

func accessLogLevel(r *http.Request, status int) slog.Level {
	switch {
	case status >= http.StatusInternalServerError:
		return slog.LevelError
	case status >= http.StatusBadRequest:
		return slog.LevelWarn
	case isDataPlaneRequest(r):
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

func isDataPlaneRequest(r *http.Request) bool {
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/produce"):
		return true
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/consume"):
		return true
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/ack"):
		return true
	default:
		return false
	}
}
