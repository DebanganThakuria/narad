package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/debanganthakuria/narad/internal/httpserver/handlers"
)

// NewRouter wires HTTP routes to handler methods and applies the standard
// middleware stack.
//
// Routes (Go 1.22+ method+path patterns):
//
//	POST /topics
//	GET  /topics
//	POST /topics/{topic}/produce
//	GET  /topics/{topic}/consume
//	POST /topics/{topic}/ack
//	GET  /healthz
//	GET  /readyz
func NewRouter(h *handlers.Set, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /topics", h.CreateTopic)
	mux.HandleFunc("GET /topics", h.ListTopics)
	mux.HandleFunc("POST /topics/{topic}/produce", h.Produce)
	mux.HandleFunc("GET /topics/{topic}/consume", h.Consume)
	mux.HandleFunc("POST /topics/{topic}/ack", h.Ack)

	mux.HandleFunc("GET /healthz", h.Healthz)
	mux.HandleFunc("GET /readyz", h.Readyz)

	stack := Chain(
		Recover(log),
		RequestID(),
		AccessLog(log),
	)
	return stack(mux)
}
