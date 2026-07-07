// Package handlers carries the shared *Set and helper methods used
// by every HTTP handler in the per-domain subpackages
// (handlers/topics, handlers/messaging, handlers/health). The
// subpackages contain only the per-endpoint request types and
// handler functions; this package owns the dependencies and the
// JSON / error-mapping plumbing.
//
// Handler subpackage functions take a *Set and return an
// http.HandlerFunc:
//
//	func Create(s *handlers.Set) http.HandlerFunc { ... }
//
// The router wires them up at startup so the subpackages don't need
// to register routes themselves.
package handlers

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

const (
	// MaxJSONBodyBytes caps JSON request bodies (topic CRUD).
	MaxJSONBodyBytes int64 = 1 << 20

	// MaxMessageBodyBytes caps produce payloads.
	MaxMessageBodyBytes int64 = 1 << 20

	// DefaultMaxConsumeWait is the hard ceiling applied to a long-poll
	// consume wait when Deps.MaxConsumeWait is left unset (<= 0). It stops
	// a client from pinning a server goroutine for an arbitrary duration
	// if the configured cap is ever missing from the wiring.
	DefaultMaxConsumeWait = 30 * time.Second
)

// Router forwards requests to the partition-owning pod in a multi-node cluster.
// Nil in single-node mode — handlers skip all routing checks when it is nil.
type Router interface {
	RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool
	RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) (bool, *int)
	RouteConsumeRemote(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) (bool, bool)
	RouteAck(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, handle consumer.Handle) bool
	RouteExtendAck(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, handle consumer.Handle) bool
	RouteNack(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, handle consumer.Handle) bool
	RouteCreateTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool
	RouteAlterTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, body []byte) bool
	RouteDeleteTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) bool
	BroadcastDeleteTopic(ctx context.Context, topicName string) error
	RouteGetTopic(ctx context.Context, r *http.Request, topicName string, details topic.Details) (topic.Details, error)
	RouteAttachChild(ctx context.Context, w http.ResponseWriter, r *http.Request, parent, child string) bool
	RouteDetachChild(ctx context.Context, w http.ResponseWriter, r *http.Request, parent, child string) bool
	// CollectFanoutCursors merges remote owners' fan-out cursor stats
	// with the local ones; ok=false means some owners were unreachable
	// and the merged lag is a lower bound.
	CollectFanoutCursors(ctx context.Context, parent string, local []topic.FanoutCursorStat) ([]topic.FanoutCursorStat, bool)
	RouteCreateUser(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool
	RouteUpdateUser(ctx context.Context, w http.ResponseWriter, r *http.Request, username string, body []byte) bool
	RouteDeleteUser(ctx context.Context, w http.ResponseWriter, r *http.Request, username string) bool
}

// Deps is the bag of collaborators every handler needs.
type Deps struct {
	Broker    broker.Broker
	Logs      *runtime.Logs
	Metastore *metastore.Store
	Logger    *slog.Logger

	// MaxConsumeWait caps a long-poll consume wait; DefaultMaxConsumeWait
	// applies when it is unset.
	MaxConsumeWait time.Duration

	// ShutdownCtx is cancelled when graceful shutdown begins; healthz
	// flips unhealthy on it.
	ShutdownCtx context.Context

	// Router is optional. When set, requests are forwarded to the partition
	// owner instead of being handled locally on non-owner pods.
	Router Router
}

// Set is shared by every handler subpackage. The Deps field is
// exported so subpackages can reach the broker and logger; the
// methods on Set are the encoding / error-mapping primitives.
type Set struct {
	Deps Deps
}

// New panics on missing required deps — handlers are constructed
// once at startup, so failing here surfaces wiring bugs immediately.
func New(d Deps) *Set {
	if d.Broker == nil {
		panic("handlers: Broker is required")
	}
	if d.Logs == nil {
		d.Logs = runtime.NewLogs("", storage.DefaultOptions(), nil, nil)
	}
	if d.Logger == nil {
		panic("handlers: Logger is required")
	}
	if d.ShutdownCtx == nil {
		d.ShutdownCtx = context.Background()
	}
	return &Set{Deps: d}
}
