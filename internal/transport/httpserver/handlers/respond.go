package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/debanganthakuria/narad/internal/errs"
)

// jsonAppender lets response types serialize themselves without going
// through encoding/json reflection on the hot path.
type jsonAppender interface {
	AppendJSON([]byte) []byte
}

var responseBufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
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

// logServerError logs 5xx responses only: 4xx errors are the client's
// fault and would just be noise.
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
	case errors.Is(err, errs.ErrFanoutRoleConflict),
		errors.Is(err, errs.ErrFanoutChildLimit),
		errors.Is(err, errs.ErrFanoutSchemaMismatch),
		errors.Is(err, errs.ErrFanoutSchemaManaged),
		errors.Is(err, errs.ErrFanoutDelayTooLong),
		errors.Is(err, errs.ErrDelayedChildProduce),
		errors.Is(err, errs.ErrAlreadyExists):
		s.WriteError(w, http.StatusConflict, err.Error())
	case errors.Is(err, errs.ErrNotFound):
		s.WriteError(w, http.StatusNotFound, err.Error())
	default:
		status := http.StatusInternalServerError
		msg := op + " failed"
		s.logServerError(status, msg, slog.String("op", op), slog.Any("err", err))
		s.writeError(w, status, msg)
	}
}
