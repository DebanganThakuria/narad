package topic

import (
	"encoding/json"
)

// Message is a single record returned to consumers. Payload is the
// caller-supplied JSON value, kept verbatim so it round-trips without
// being mangled into base64 (as a plain []byte field would be).
//
// ReceiptHandle is an opaque, HMAC-signed token the consumer must
// echo back on Ack. It encodes (topic, partition, offset, expiresAt,
// nonce) so the broker can reject acks for offsets the client never
// reserved or whose visibility window has elapsed and been re-issued
// to another consumer. Empty for replay reads — replay does not
// reserve and therefore has nothing to ack.
type Message struct {
	Topic         string          `json:"topic"`
	Partition     int             `json:"partition"`
	Offset        int64           `json:"offset"`
	Key           string          `json:"key,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	Timestamp     int64           `json:"timestamp"`
	ReceiptHandle string          `json:"receipt_handle,omitempty"`
}
