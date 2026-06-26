package topic

import (
	"encoding/json"
	"strconv"
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

func (m Message) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"topic":`...)
	dst = strconv.AppendQuote(dst, m.Topic)
	dst = append(dst, `,"partition":`...)
	dst = strconv.AppendInt(dst, int64(m.Partition), 10)
	dst = append(dst, `,"offset":`...)
	dst = strconv.AppendInt(dst, m.Offset, 10)
	if m.Key != "" {
		dst = append(dst, `,"key":`...)
		dst = strconv.AppendQuote(dst, m.Key)
	}
	dst = append(dst, `,"payload":`...)
	if len(m.Payload) == 0 {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, m.Payload...)
	}
	dst = append(dst, `,"timestamp":`...)
	dst = strconv.AppendInt(dst, m.Timestamp, 10)
	if m.ReceiptHandle != "" {
		dst = append(dst, `,"receipt_handle":`...)
		dst = strconv.AppendQuote(dst, m.ReceiptHandle)
	}
	dst = append(dst, '}')
	return dst
}
