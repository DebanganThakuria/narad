// Package handlers carries the shared *Set and helper methods used
// by every HTTP handler in the per-domain subpackages
// (handlers/topics, handlers/messaging, handlers/health). The
// subpackages contain only the per-endpoint request types and
// handler functions; this file owns the dependencies and the
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

const (
	MaxJSONBodyBytes    int64 = 1 << 20
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
	RouteCreateTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool
	RouteAlterTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, body []byte) bool
	RouteDeleteTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) bool
	BroadcastDeleteTopic(ctx context.Context, topicName string) error
	RouteGetTopic(ctx context.Context, r *http.Request, topicName string, details topic.Details) (topic.Details, error)
}

// Deps is the bag of collaborators every handler needs.
type Deps struct {
	Broker         broker.Broker
	Logs           *runtime.Logs
	Metastore      *metastore.Store
	Logger         *slog.Logger
	MaxConsumeWait time.Duration
	ShutdownCtx    context.Context
	// Router is optional. When set, requests are forwarded to the partition
	// owner instead of being handled locally on non-owner pods.
	Router Router
}

// Set is shared by every handler subpackage. The Deps field is
// exported so subpackages can reach the broker and logger; the
// helper methods below are the encoding / error-mapping primitives.
type Set struct {
	Deps Deps
}

type jsonAppender interface {
	AppendJSON([]byte) []byte
}

var responseBufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
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

// WriteJSON encodes v as the JSON response body with the given
// status. Nil values produce a header-only response.
func (s *Set) WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	if v == nil {
		w.WriteHeader(status)
		return
	}

	buf := responseBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer responseBufferPool.Put(buf)

	if appender, ok := v.(jsonAppender); ok {
		buf.Write(appender.AppendJSON(buf.AvailableBuffer()))
		buf.WriteByte('\n')
	} else if err := json.NewEncoder(buf).Encode(v); err != nil {
		s.Deps.Logger.Error("encode response", "err", err)
		w.WriteHeader(status)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(status)
	if _, err := w.Write(buf.Bytes()); err != nil {
		s.Deps.Logger.Error("write response", "err", err)
	}
}

// WriteError writes a `{"error": msg}` body with the given status.
func (s *Set) WriteError(w http.ResponseWriter, status int, msg string) {
	s.logServerError(status, msg)
	s.writeError(w, status, msg)
}

func (s *Set) writeError(w http.ResponseWriter, status int, msg string) {
	body := make([]byte, 0, len(msg)+14)
	body = append(body, `{"error":`...)
	body = strconv.AppendQuote(body, msg)
	body = append(body, "}\n"...)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		s.Deps.Logger.Error("write error response", "err", err)
	}
}

func (s *Set) logServerError(status int, msg string, attrs ...slog.Attr) {
	if status < http.StatusInternalServerError {
		return
	}
	logAttrs := make([]slog.Attr, 0, len(attrs)+2)
	logAttrs = append(logAttrs,
		slog.Int("status", status),
		slog.String("error", msg),
	)
	logAttrs = append(logAttrs, attrs...)
	s.Deps.Logger.LogAttrs(context.Background(), slog.LevelError, "http server error", logAttrs...)
}

func (s *Set) ReadBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.WriteError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		}
		s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
		return nil, false
	}
	return body, true
}

// DecodeJSON reads a JSON body in strict mode (unknown fields
// rejected). Responds to the client on failure and returns false.
func (s *Set) DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxJSONBodyBytes))
	return s.decodeJSON(w, dec, dst)
}

func (s *Set) DecodeJSONBytes(w http.ResponseWriter, body []byte, dst any) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	return s.decodeJSON(w, dec, dst)
}

func (s *Set) decodeJSON(w http.ResponseWriter, dec *json.Decoder, dst any) bool {
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			s.WriteError(w, http.StatusBadRequest, "invalid json: multiple JSON values")
			return false
		}
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}

// Validator is implemented by request types that carry their own
// post-decode invariants.
type Validator interface {
	Validate() error
}

// DecodeAndValidate is DecodeJSON followed by dst.Validate(). Bad
// validation responds with 400 and returns false.
func (s *Set) DecodeAndValidate(w http.ResponseWriter, r *http.Request, dst Validator) bool {
	if !s.DecodeJSON(w, r, dst) {
		return false
	}
	if err := dst.Validate(); err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

// WriteBrokerError maps a broker error to the right HTTP status. op
// is a short verb ("produce", "ack", …) used in log messages and
// the generic 5xx body.
//
// Receipt-handle errors are mapped to discrete codes so clients can
// distinguish "you sent garbage or used the wrong topic" (400) from
// "you took too long / it was already redelivered" (410). Out-of-order
// ack rejected by ackedAhead-cap maps to 503 — the head is genuinely
// stuck and the client should back off.
func (s *Set) WriteBrokerError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, errs.ErrTopicNotFound):
		s.WriteError(w, http.StatusNotFound, "topic not found")
	case errors.Is(err, errs.ErrTopicAlreadyExists):
		s.WriteError(w, http.StatusConflict, "topic already exists")
	case errors.Is(err, errs.ErrHandleMalformed):
		s.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrHandleStale):
		s.WriteError(w, http.StatusGone, err.Error())
	case errors.Is(err, errs.ErrAckedAheadFull):
		s.WriteError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, errs.ErrInvalidArgument),
		errors.Is(err, errs.ErrPartitionRequired):
		s.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrNotPartitionOwner):
		s.WriteError(w, http.StatusMisdirectedRequest, err.Error())
	default:
		status := http.StatusInternalServerError
		msg := op + " failed"
		s.logServerError(status, msg, slog.String("op", op), slog.Any("err", err))
		s.writeError(w, status, msg)
	}
}
