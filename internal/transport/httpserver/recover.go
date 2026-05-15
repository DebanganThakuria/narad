package httpserver

import (
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Recover catches panics from downstream handlers, logs the stack with
// request context, and writes a 500.
func Recover(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.LogAttrs(r.Context(), slog.LevelError, "http panic",
						slog.Any("panic", rec),
						slog.String("path", r.URL.Path),
						slog.String("request_id", requestIDFrom(r.Context())),
						slog.String("stack", string(debug.Stack())),
					)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"internal server panic"}`))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
