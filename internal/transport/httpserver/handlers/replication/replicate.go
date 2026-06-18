package replication

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type replicateRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Payload   []byte `json:"payload"`
	LeaderID  string `json:"leader_id"`
}

func (req replicateRequest) Validate() error {
	if req.Topic == "" {
		return errors.New("topic required")
	}
	if req.Partition < 0 {
		return errors.New("partition must be >= 0")
	}
	if req.Offset < 0 {
		return errors.New("offset must be >= 0")
	}
	if !json.Valid(req.Payload) {
		return errors.New("payload must be valid JSON")
	}
	return nil
}

func Replicate(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req replicateRequest
		if !s.DecodeAndValidate(w, r, &req) {
			return
		}

		log, err := s.Deps.Logs.Get(req.Topic, req.Partition)
		if err != nil {
			s.Deps.Logger.Error("replicate open log", "topic", req.Topic, "partition", req.Partition, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replicate failed")
			return
		}
		if next := log.NextOffset(); next != req.Offset {
			s.Deps.Logger.Error("replicate offset mismatch", "topic", req.Topic, "partition", req.Partition, "got", next, "want", req.Offset)
			s.WriteError(w, http.StatusConflict, "replicate offset mismatch")
			return
		}

		offset, err := log.Append(req.Payload)
		if err != nil {
			s.Deps.Logger.Error("replicate append", "topic", req.Topic, "partition", req.Partition, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replicate failed")
			return
		}
		if offset != req.Offset {
			s.Deps.Logger.Error("replicate offset mismatch", "topic", req.Topic, "partition", req.Partition, "got", offset, "want", req.Offset)
			s.WriteError(w, http.StatusConflict, "replicate offset mismatch")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func ReadReplica(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.URL.Query().Get("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		partitionIdx, err := strconv.Atoi(r.URL.Query().Get("partition"))
		if err != nil || partitionIdx < 0 {
			s.WriteError(w, http.StatusBadRequest, "invalid partition")
			return
		}
		offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil || offset < 0 {
			s.WriteError(w, http.StatusBadRequest, "invalid offset")
			return
		}
		committedOnly := r.URL.Query().Get("committed") == "true"

		log, err := s.Deps.Logs.Get(topicName, partitionIdx)
		if err != nil {
			s.Deps.Logger.Error("replica read open log", "topic", topicName, "partition", partitionIdx, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replica read failed")
			return
		}
		if committedOnly && offset >= log.HighWatermark() {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		payload, err := log.Read(offset)
		if err != nil {
			if errors.Is(err, storage.ErrOffsetNotFound) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			s.Deps.Logger.Error("replica read", "topic", topicName, "partition", partitionIdx, "offset", offset, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replica read failed")
			return
		}

		s.WriteJSON(w, http.StatusOK, map[string]any{"payload": payload})
	}
}
