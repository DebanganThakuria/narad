package main

import "encoding/json"

const testerMessageSchema = `{
  "type": "object",
  "properties": {
    "id":                  { "type": "string" },
    "run_id":              { "type": "string" },
    "topic":               { "type": "string" },
    "sequence":            { "type": "integer" },
    "key":                 { "type": "string" },
    "produced_at_unix_ms": { "type": "integer" },
    "payload":             { "type": "string" }
  },
  "required": ["id", "run_id", "topic", "sequence", "key", "produced_at_unix_ms", "payload"],
  "additionalProperties": false
}`

type createTopicRequest struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

type produceRequest struct {
	Key     string        `json:"key"`
	Message testerMessage `json:"message"`
}

type produceResponse struct {
	Status           string `json:"status"`
	MessageID        string `json:"message_id"`
	Topic            string `json:"topic"`
	Partition        int    `json:"partition"`
	AcceptedAtUnixMs int64  `json:"accepted_at_unix_ms"`
	Offset           int64  `json:"-"`
}

type consumeResponse struct {
	Topic         string        `json:"topic"`
	Partition     int           `json:"partition"`
	Offset        int64         `json:"offset"`
	Payload       testerMessage `json:"payload"`
	ReceiptHandle string        `json:"receipt_handle"`
}

type ackRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
}

type testerMessage struct {
	ID               string `json:"id"`
	RunID            string `json:"run_id"`
	Topic            string `json:"topic"`
	Sequence         int64  `json:"sequence"`
	Key              string `json:"key"`
	ProducedAtUnixMs int64  `json:"produced_at_unix_ms"`
	Payload          string `json:"payload"`
}

type messageJob struct {
	ID       string
	Topic    string
	Sequence int64
	Key      string
}
