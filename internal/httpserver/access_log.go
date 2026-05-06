package httpserver

import (
	"log/slog"
	"net/http"
	"time"
)

// AccessLog emits one structured log line per request. Status and
// payload size are captured via the recorder wrapper so the line is
// accurate even when handlers don't call WriteHeader explicitly.
func AccessLog(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &recorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			log.LogAttrs(r.Context(), slog.LevelInfo, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", time.Since(start)),
				slog.String("remote", r.RemoteAddr),
				slog.String("request_id", RequestIDFrom(r.Context())),
			)
		})
	}
}
