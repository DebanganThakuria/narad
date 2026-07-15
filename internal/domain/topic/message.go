package topic

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// Message is a single record returned to consumers. Produce is
// octet-stream permissive, so Payload holds whatever bytes the caller
// sent; the response encoding depends on what they are. A valid JSON
// value is kept verbatim so it round-trips without being mangled into
// base64 (as a plain []byte field would be). Non-JSON UTF-8 text is
// returned as a JSON string. Anything else is binary, which a JSON
// string cannot carry losslessly: it is returned base64-encoded with
// "payload_encoding":"base64" alongside so consumers know to decode.
//
// ReceiptHandle is the token the consumer must echo back on Ack. It
// encodes partition, offset, and reservation nonce as
// partition:offset:nonce. The ack request path supplies the topic.
// Empty for replay reads — replay does not reserve and therefore has
// nothing to ack.
type Message struct {
	Topic         string          `json:"topic"`
	Partition     int             `json:"partition"`
	Offset        int64           `json:"offset"`
	Key           string          `json:"key,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	Timestamp     int64           `json:"timestamp"`
	ReceiptHandle string          `json:"receipt_handle,omitempty"`
}

// AppendJSON appends m's JSON encoding to dst and returns the extended
// slice. It is THE encoding of a Message (MarshalJSON delegates here),
// written append-style — no reflection or intermediate allocations —
// because it sits on the consume hot path. Key and ReceiptHandle get
// omitempty handling; an empty Payload encodes as null.
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
	switch {
	case len(m.Payload) == 0:
		dst = append(dst, "null"...)
	case json.Valid(m.Payload):
		dst = append(dst, m.Payload...)
	case utf8.Valid(m.Payload):
		// Non-JSON text: deliver it as a plain JSON string. The quoted
		// form decodes back to exactly the bytes produced.
		dst = appendJSONString(dst, m.Payload)
	default:
		// Binary: a JSON string cannot carry arbitrary bytes losslessly
		// (invalid UTF-8 would be silently replaced), so base64-wrap and
		// flag it for the consumer.
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, m.Payload)
		dst = append(dst, `","payload_encoding":"base64"`...)
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

// appendJSONString appends s as a JSON string literal. encoding/json
// does the escaping: strconv.AppendQuote would emit Go-syntax \x
// escapes that JSON forbids. This branch only runs for non-JSON text
// payloads, so the allocation is off the hot path.
func appendJSONString(dst, s []byte) []byte {
	quoted, err := json.Marshal(string(s))
	if err != nil {
		// Unreachable for a string input; keep the response well-formed
		// regardless.
		return append(dst, `""`...)
	}
	return append(dst, quoted...)
}

// MarshalJSON delegates to AppendJSON so every encoder — including the
// node-RPC layer's json.Marshal when a consume is answered for a peer —
// applies the same payload tolerance. Without this, a non-JSON payload
// inside the RawMessage makes json.Marshal fail and a forwarded consume
// turns into an opaque 500 while the reservation is already held.
func (m Message) MarshalJSON() ([]byte, error) {
	return m.AppendJSON(nil), nil
}
