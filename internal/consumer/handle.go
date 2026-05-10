package consumer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
)

// Receipt-handle errors. Surfaced through the broker so the HTTP layer
// can map them to specific status codes (400 / 401 / 410).
var (
	ErrHandleMalformed     = errors.New("consumer: receipt handle is malformed")
	ErrHandleHMACMismatch  = errors.New("consumer: receipt handle HMAC mismatch")
	ErrHandleTopicMismatch = errors.New("consumer: receipt handle topic does not match request path")
)

// Handle is the decoded, validated content of a receipt token. The
// nonce identifies a single ReserveNext invocation and is used by the
// in-flight set to detect a stale handle whose offset has been
// re-reserved by another consumer.
type Handle struct {
	Topic           string
	Partition       int32
	Offset          int64
	ExpiresAtUnixMs int64
	Nonce           int64
}

// EncodeHandle serializes (topic, partition, offset, exp, nonce) and
// signs it with HMAC-SHA256 keyed by secret. Output is two
// base64-url-no-pad strings joined by '.', conventional for opaque
// tokens.
//
// Wire format of the signed payload (length-prefixed binary, big-
// endian):
//
//	[topicLen u16][topic bytes][partition i32][offset i64][exp i64][nonce i64]
//
// Length-prefixed (rather than pipe-delimited) because topic names
// can in principle contain delimiters; this keeps the parser
// unambiguous.
func EncodeHandle(secret []byte, topic string, partition int32, offset, expiresAtUnixMs, nonce int64) string {
	payload := encodePayload(topic, partition, offset, expiresAtUnixMs, nonce)
	mac := hmacSum(secret, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac)
}

// DecodeHandle parses and verifies a receipt handle. It does NOT check
// that the encoded topic matches any caller-supplied topic — that's
// the caller's job (call MatchTopic). It returns ErrHandleMalformed
// for any structural issue and ErrHandleHMACMismatch if the signature
// doesn't verify.
func DecodeHandle(secret []byte, raw string) (Handle, error) {
	dot := -1
	for i := 0; i < len(raw); i++ {
		if raw[i] == '.' {
			dot = i
			break
		}
	}
	if dot <= 0 || dot == len(raw)-1 {
		return Handle{}, ErrHandleMalformed
	}

	payload, err := base64.RawURLEncoding.DecodeString(raw[:dot])
	if err != nil {
		return Handle{}, fmt.Errorf("%w: payload b64: %v", ErrHandleMalformed, err)
	}
	mac, err := base64.RawURLEncoding.DecodeString(raw[dot+1:])
	if err != nil {
		return Handle{}, fmt.Errorf("%w: mac b64: %v", ErrHandleMalformed, err)
	}

	expected := hmacSum(secret, payload)
	if !hmac.Equal(mac, expected) {
		return Handle{}, ErrHandleHMACMismatch
	}

	h, err := decodePayload(payload)
	if err != nil {
		return Handle{}, fmt.Errorf("%w: %v", ErrHandleMalformed, err)
	}
	return h, nil
}

// MatchTopic returns ErrHandleTopicMismatch if the handle's encoded
// topic does not match the supplied path-topic. Cheap defense against
// using a handle for one topic against another's ack endpoint.
func (h Handle) MatchTopic(topic string) error {
	if h.Topic != topic {
		return ErrHandleTopicMismatch
	}
	return nil
}

func encodePayload(topic string, partition int32, offset, exp, nonce int64) []byte {
	tb := []byte(topic)
	out := make([]byte, 0, 2+len(tb)+4+8+8+8)
	var u16 [2]byte
	binary.BigEndian.PutUint16(u16[:], uint16(len(tb)))
	out = append(out, u16[:]...)
	out = append(out, tb...)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(partition))
	out = append(out, u32[:]...)
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], uint64(offset))
	out = append(out, u64[:]...)
	binary.BigEndian.PutUint64(u64[:], uint64(exp))
	out = append(out, u64[:]...)
	binary.BigEndian.PutUint64(u64[:], uint64(nonce))
	out = append(out, u64[:]...)
	return out
}

func decodePayload(b []byte) (Handle, error) {
	if len(b) < 2 {
		return Handle{}, errors.New("payload too short for topic length")
	}
	tlen := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) != 2+tlen+4+8+8+8 {
		return Handle{}, errors.New("payload length mismatch")
	}
	topic := string(b[2 : 2+tlen])
	off := 2 + tlen
	partition := int32(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	offset := int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	exp := int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	nonce := int64(binary.BigEndian.Uint64(b[off : off+8]))
	return Handle{
		Topic:           topic,
		Partition:       partition,
		Offset:          offset,
		ExpiresAtUnixMs: exp,
		Nonce:           nonce,
	}, nil
}

func hmacSum(secret, payload []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return mac.Sum(nil)
}
