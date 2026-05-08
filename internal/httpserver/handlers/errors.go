package handlers

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker"
)

// writeBrokerError maps a broker error to the right HTTP status. op
// is a short verb ("produce", "ack", …) used in log messages and the
// generic 5xx body.
func (s *Set) writeBrokerError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, broker.ErrTopicNotFound):
		s.writeError(w, http.StatusNotFound, "topic not found")
	case errors.Is(err, broker.ErrTopicAlreadyExists):
		s.writeError(w, http.StatusConflict, "topic already exists")
	case errors.Is(err, broker.ErrInvalidArgument),
		errors.Is(err, broker.ErrPartitionRequired):
		s.writeError(w, http.StatusBadRequest, err.Error())
	default:
		s.deps.Logger.Error(op, "err", err)
		s.writeError(w, http.StatusInternalServerError, op+" failed")
	}
}

type Validator interface {
	Validate() error
}

func (s *Set) decodeAndValidate(w http.ResponseWriter, r *http.Request, dst Validator) bool {
	if !s.decodeJSON(w, r, dst) {
		return false
	}
	if err := dst.Validate(); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}
