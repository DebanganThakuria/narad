// Package sink is the data path of a remote child: it turns a slab of
// parent records into batch-produce requests for a topic on another
// Narad cluster, reads the target's answers, and holds the per-remote
// pacing (the gate, lanes, the chunk caps and the held-record budget).
// The fan-out engine (cluster/fanout_remote.go) drives it; nothing here
// reads the metastore or knows about cursors.
//
// The target is a well-behaved client's view of another cluster: every
// request goes to POST /v1/topics/{t}/produce/batch with the remote's
// cached client and header, never to single produce (a key would travel
// in the query string), and nothing from the target's answers reaches a
// log or an API answer except a status code, a class and a message
// index.
package sink

import (
	"encoding/base64"
	"encoding/json"
	"unicode/utf8"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// batchPrefix and batchSuffix frame a batch-produce body.
const (
	batchPrefix = `{"messages":[`
	batchSuffix = `]}`
)

// RawPayload reports whether p can travel as it is: exactly one valid
// JSON value, valid UTF-8, with no whitespace before or after it. The
// target stores such a value's bytes exactly as written, so it arrives
// byte for byte. Anything else goes as base64.
func RawPayload(p []byte) bool {
	if len(p) == 0 || isJSONSpace(p[0]) || isJSONSpace(p[len(p)-1]) {
		return false
	}
	return utf8.Valid(p) && json.Valid(p)
}

func isJSONSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// AppendMessage appends rec as one batch message: the key as a JSON
// string when it is valid UTF-8, else as base64 with key_encoding; no
// key for a keyless record; the payload raw when RawPayload allows it
// and base64Payload is false, else as base64 with payload_encoding. The
// body is built by appending bytes, never through json.Marshal of a raw
// value, which would compact and HTML-escape it. No partition is sent:
// the target's partitioner decides.
func AppendMessage(dst []byte, rec topic.KeyedRecord, base64Payload bool) []byte {
	dst = append(dst, '{')
	if rec.Key != "" {
		if utf8.ValidString(rec.Key) {
			dst = append(dst, `"key":`...)
			dst = topic.AppendJSONQuoted(dst, rec.Key)
		} else {
			dst = append(dst, `"key":"`...)
			dst = base64.StdEncoding.AppendEncode(dst, []byte(rec.Key))
			dst = append(dst, `","key_encoding":"base64"`...)
		}
		dst = append(dst, ',')
	}
	if !base64Payload && RawPayload(rec.Payload) {
		dst = append(dst, `"payload":`...)
		dst = append(dst, rec.Payload...)
	} else {
		dst = append(dst, `"payload":"`...)
		dst = base64.StdEncoding.AppendEncode(dst, rec.Payload)
		dst = append(dst, `","payload_encoding":"base64"`...)
	}
	return append(dst, '}')
}

// SentRaw reports whether AppendMessage sends rec's payload raw unless
// told otherwise: a rejected raw record is retried once as base64.
func SentRaw(rec topic.KeyedRecord) bool { return RawPayload(rec.Payload) }

// AppendBatch appends a whole batch body for recs. base64At, when not
// nil, forces base64 for the records it reports true for.
func AppendBatch(dst []byte, recs []topic.KeyedRecord, base64At func(i int) bool) []byte {
	dst = append(dst, batchPrefix...)
	for i, rec := range recs {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = AppendMessage(dst, rec, base64At != nil && base64At(i))
	}
	return append(dst, batchSuffix...)
}

// ChunkBuilder cuts one chunk off the front of a lane's records: at most
// maxMessages messages and at most maxBytes of body, except that a first
// record larger than maxBytes goes alone in its own chunk.
//
// Every chunk gets a buffer of its own. The HTTP transport may still be
// writing a request body after the response arrived (a target that
// answers 413 before reading), so a reused buffer could change under it.
type ChunkBuilder struct {
	sizeHint int
}

// Build encodes a chunk of recs and returns its body and how many
// records it holds (at least one when recs is not empty).
func (b *ChunkBuilder) Build(recs []topic.KeyedRecord, maxMessages, maxBytes int, base64At func(i int) bool) ([]byte, int) {
	buf := make([]byte, 0, max(b.sizeHint, 4096))
	buf = append(buf, batchPrefix...)
	n := 0
	for i := range recs {
		if n == maxMessages {
			break
		}
		mark := len(buf)
		if n > 0 {
			buf = append(buf, ',')
		}
		buf = AppendMessage(buf, recs[i], base64At != nil && base64At(i))
		if n > 0 && len(buf)+len(batchSuffix) > maxBytes {
			buf = buf[:mark]
			break
		}
		n++
	}
	buf = append(buf, batchSuffix...)
	b.sizeHint = min(len(buf)+len(buf)/8, maxBytes+len(batchSuffix))
	return buf, n
}
