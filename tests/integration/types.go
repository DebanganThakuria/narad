package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

type config struct {
	mode               string
	nodes              []string
	topics             int
	messages           int
	partitions         int
	produceConcurrency int
	consumeConcurrency int
	timeout            time.Duration
	assignmentTimeout  time.Duration
	visibilityTimeout  time.Duration
	runID              string
	cleanup            bool
	username           string
	password           string
}

type roundRobinClient struct {
	nodes    []string
	client   *http.Client
	next     atomic.Uint64
	username string
	password string
}

type topicRecord struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

type listTopicsResponse struct {
	Topics []topicRecord `json:"topics"`
}

type topicDetailsResponse struct {
	topicRecord
	PartitionStats []partitionStats `json:"partition_stats"`
}

type partitionStats struct {
	Index int `json:"index"`
}

type messageRecord struct {
	ID       string `json:"id"`
	Topic    string `json:"topic"`
	Sequence int    `json:"sequence"`
	Key      string `json:"key"`
	RunID    string `json:"run_id"`
}

type consumeResponse struct {
	Topic         string        `json:"topic"`
	Partition     int           `json:"partition"`
	Offset        int64         `json:"offset"`
	Payload       messageRecord `json:"payload"`
	ReceiptHandle string        `json:"receipt_handle"`
}

type messageJob struct {
	Topic string
	Key   string
	Body  messageRecord
}

type runStats struct {
	produced   atomic.Int64
	consumed   atomic.Int64
	acked      atomic.Int64
	duplicates atomic.Int64
}

const messageSchema = `{
  "type": "object",
  "properties": {
    "id":       { "type": "string" },
    "topic":    { "type": "string" },
    "sequence": { "type": "integer" },
    "key":      { "type": "string" },
    "run_id":   { "type": "string" }
  },
  "required": ["id", "topic", "sequence", "key", "run_id"],
  "additionalProperties": false
}`
