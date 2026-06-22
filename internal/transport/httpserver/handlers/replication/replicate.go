package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type replicateRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Payload   []byte `json:"payload"`
	LeaderID  string `json:"leader_id"`
}

type replicateBatchRequest struct {
	Topic      string
	Partition  int
	BaseOffset int64
	Payloads   [][]byte
	LeaderID   string
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

func (req replicateBatchRequest) Validate() error {
	if req.Topic == "" {
		return errors.New("topic required")
	}
	if req.Partition < 0 {
		return errors.New("partition must be >= 0")
	}
	if req.BaseOffset < 0 {
		return errors.New("offset must be >= 0")
	}
	if len(req.Payloads) == 0 {
		return errors.New("batch must contain at least one payload")
	}
	for _, payload := range req.Payloads {
		if !json.Valid(payload) {
			return errors.New("payload must be valid JSON")
		}
	}
	return nil
}

func Replicate(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if replicationwire.IsBatchContentType(r.Header.Get("Content-Type")) {
			replicateBatch(s, w, r)
			return
		}

		req, ok := decodeReplicateRequest(s, w, r)
		if !ok {
			return
		}

		log, err := s.Deps.Logs.Get(req.Topic, req.Partition)
		if err != nil {
			s.Deps.Logger.Error("replicate open log", "topic", req.Topic, "partition", req.Partition, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replicate failed")
			return
		}
		if next := log.NextOffset(); next != req.Offset {
			if next > req.Offset && acceptDuplicateReplica(w, s, log, req) {
				return
			}
			writeOffsetMismatch(w, s, req, next)
			return
		}

		offset, err := log.Append(req.Payload)
		if err != nil {
			s.Deps.Logger.Error("replicate append", "topic", req.Topic, "partition", req.Partition, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replicate failed")
			return
		}
		if offset != req.Offset {
			writeOffsetMismatch(w, s, req, offset)
			return
		}
		if err := log.AdvanceHighWatermark(req.Offset + 1); err != nil {
			s.Deps.Logger.Error("replicate commit boundary", "topic", req.Topic, "partition", req.Partition, "offset", req.Offset, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replicate failed")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func replicateBatch(s *handlers.Set, w http.ResponseWriter, r *http.Request) {
	req, ok := decodeBatchReplicateRequest(s, w, r)
	if !ok {
		return
	}

	log, err := s.Deps.Logs.Get(req.Topic, req.Partition)
	if err != nil {
		s.Deps.Logger.Error("replicate batch open log", "topic", req.Topic, "partition", req.Partition, "err", err)
		s.WriteError(w, http.StatusInternalServerError, "replicate failed")
		return
	}

	next := log.NextOffset()
	endOffset := req.BaseOffset + int64(len(req.Payloads))
	if next < req.BaseOffset {
		writeBatchOffsetMismatch(w, s, req, next)
		return
	}

	appendStart := next
	if next > req.BaseOffset {
		verifiedUntil, accepted := acceptDuplicateBatchPrefix(log, req, min(next, endOffset))
		if !accepted {
			writeBatchOffsetMismatch(w, s, req, next)
			return
		}
		appendStart = verifiedUntil
	}
	if appendStart < endOffset {
		startIdx := int(appendStart - req.BaseOffset)
		first, last, err := log.AppendBatch(req.Payloads[startIdx:])
		if err != nil {
			s.Deps.Logger.Error("replicate batch append", "topic", req.Topic, "partition", req.Partition, "err", err)
			s.WriteError(w, http.StatusInternalServerError, "replicate failed")
			return
		}
		if first != appendStart || last != endOffset-1 {
			writeBatchOffsetMismatch(w, s, req, first)
			return
		}
	}
	if err := log.AdvanceHighWatermark(endOffset); err != nil {
		s.Deps.Logger.Error("replicate batch commit boundary", "topic", req.Topic, "partition", req.Partition, "offset", endOffset, "err", err)
		s.WriteError(w, http.StatusInternalServerError, "replicate failed")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type replicaLog interface {
	Read(int64) ([]byte, error)
	AdvanceHighWatermark(int64) error
}

func acceptDuplicateReplica(w http.ResponseWriter, s *handlers.Set, log replicaLog, req replicateRequest) bool {
	existing, err := log.Read(req.Offset)
	if err != nil || !bytes.Equal(existing, req.Payload) {
		return false
	}
	if err := log.AdvanceHighWatermark(req.Offset + 1); err != nil {
		s.Deps.Logger.Error("replicate duplicate commit boundary", "topic", req.Topic, "partition", req.Partition, "offset", req.Offset, "err", err)
		s.WriteError(w, http.StatusInternalServerError, "replicate failed")
		return true
	}
	w.WriteHeader(http.StatusNoContent)
	return true
}

func writeOffsetMismatch(w http.ResponseWriter, s *handlers.Set, req replicateRequest, next int64) {
	w.Header().Set(replicationwire.HeaderReplicaNextOffset, strconv.FormatInt(next, 10))
	s.Deps.Logger.Error("replicate offset mismatch", "topic", req.Topic, "partition", req.Partition, "got", next, "want", req.Offset)
	s.WriteError(w, http.StatusConflict, "replicate offset mismatch")
}

func writeBatchOffsetMismatch(w http.ResponseWriter, s *handlers.Set, req replicateBatchRequest, next int64) {
	w.Header().Set(replicationwire.HeaderReplicaNextOffset, strconv.FormatInt(next, 10))
	s.Deps.Logger.Error("replicate batch offset mismatch", "topic", req.Topic, "partition", req.Partition, "got", next, "want", req.BaseOffset)
	s.WriteError(w, http.StatusConflict, "replicate offset mismatch")
}

func decodeReplicateRequest(s *handlers.Set, w http.ResponseWriter, r *http.Request) (replicateRequest, bool) {
	if replicationwire.IsRawContentType(r.Header.Get("Content-Type")) {
		return decodeRawReplicateRequest(s, w, r)
	}

	var req replicateRequest
	if !s.DecodeAndValidate(w, r, &req) {
		return replicateRequest{}, false
	}
	return req, true
}

func decodeRawReplicateRequest(s *handlers.Set, w http.ResponseWriter, r *http.Request) (replicateRequest, bool) {
	partition, err := strconv.Atoi(r.Header.Get(replicationwire.HeaderPartition))
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
		return replicateRequest{}, false
	}
	offset, err := strconv.ParseInt(r.Header.Get(replicationwire.HeaderOffset), 10, 64)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid offset: "+err.Error())
		return replicateRequest{}, false
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, handlers.MaxJSONBodyBytes))
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
		return replicateRequest{}, false
	}
	req := replicateRequest{
		Topic:     r.Header.Get(replicationwire.HeaderTopic),
		Partition: partition,
		Offset:    offset,
		Payload:   payload,
		LeaderID:  r.Header.Get(replicationwire.HeaderLeaderID),
	}
	if err := req.Validate(); err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return replicateRequest{}, false
	}
	return req, true
}

func decodeBatchReplicateRequest(s *handlers.Set, w http.ResponseWriter, r *http.Request) (replicateBatchRequest, bool) {
	partition, err := strconv.Atoi(r.Header.Get(replicationwire.HeaderPartition))
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
		return replicateBatchRequest{}, false
	}
	offset, err := strconv.ParseInt(r.Header.Get(replicationwire.HeaderOffset), 10, 64)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid offset: "+err.Error())
		return replicateBatchRequest{}, false
	}
	body := http.MaxBytesReader(w, r.Body, handlers.MaxJSONBodyBytes)
	payloads, err := replicationwire.DecodeBatchPayload(body, 0)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid batch: "+err.Error())
		return replicateBatchRequest{}, false
	}
	req := replicateBatchRequest{
		Topic:      r.Header.Get(replicationwire.HeaderTopic),
		Partition:  partition,
		BaseOffset: offset,
		Payloads:   payloads,
		LeaderID:   r.Header.Get(replicationwire.HeaderLeaderID),
	}
	if err := req.Validate(); err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return replicateBatchRequest{}, false
	}
	return req, true
}

func acceptDuplicateBatchPrefix(log replicaLog, req replicateBatchRequest, exclusiveEnd int64) (int64, bool) {
	for offset := req.BaseOffset; offset < exclusiveEnd; offset++ {
		existing, err := log.Read(offset)
		idx := int(offset - req.BaseOffset)
		if err != nil || idx < 0 || idx >= len(req.Payloads) || !bytes.Equal(existing, req.Payloads[idx]) {
			return offset, false
		}
	}
	return exclusiveEnd, true
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
