package topic

import (
	"encoding/json"
)

// Message is a single record returned to consumers. Payload is the
// caller-supplied JSON value, kept verbatim so it round-trips without
// being mangled into base64 (as a plain []byte field would be).
type Message struct {
	Topic     string          `json:"topic"`
	Partition int             `json:"partition"`
	Offset    int64           `json:"offset"`
	Key       string          `json:"key,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp int64           `json:"timestamp"`
}
