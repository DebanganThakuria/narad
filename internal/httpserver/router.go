package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/debanganthakuria/narad/internal/httpserver/handlers"
)

// NewRouter wires HTTP routes to handler methods. All API routes live
// under /v1; /healthz and /readyz are unprefixed (Kubernetes
// convention).
func NewRouter(h *handlers.Set, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/topics", h.CreateTopic)
	mux.HandleFunc("GET /v1/topics", h.ListTopics)
	mux.HandleFunc("GET /v1/topics/{topic}", h.GetTopic)
	mux.HandleFunc("PATCH /v1/topics/{topic}", h.AlterTopic)
	mux.HandleFunc("DELETE /v1/topics/{topic}", h.DeleteTopic)
	mux.HandleFunc("POST /v1/topics/{topic}/produce", h.Produce)
	mux.HandleFunc("GET /v1/topics/{topic}/consume", h.Consume)
	mux.HandleFunc("POST /v1/topics/{topic}/ack", h.Ack)

	mux.HandleFunc("GET /healthz", h.Healthz)
	mux.HandleFunc("GET /readyz", h.Readyz)

	stack := Chain(
		Recover(log),
		RequestID(),
		AccessLog(log),
	)
	return stack(mux)
}
