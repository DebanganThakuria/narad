package sink

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/remote"
)

// Capabilities is what a target's batch produce takes beyond the
// original contract: up to 1,000 messages (Q13) and a zstd body (Q14).
type Capabilities struct {
	MaxMessages int
	Zstd        bool
}

// DefaultCapabilities assumes neither.
func DefaultCapabilities() Capabilities { return Capabilities{MaxMessages: DefaultMaxChunkMessages} }

// errProbeInconclusive reports a probe answer that proves nothing about
// the capability (the target was unreachable, refused the grant, ...).
var errProbeInconclusive = errors.New("sink: capability probe inconclusive")

// ProbeCapabilities asks the target, without writing anything, what its
// batch produce takes. 101 messages with no payload get 400 "too many
// messages" from a target capped at 100, and 400 "message 0: message
// required" from one that takes 1,000. With wantZstd, a zstd-compressed
// empty batch gets 400 "messages required" from a target that decodes
// it, and 400 "invalid json" or 415 from one that does not. Run once
// per remote per node and after each credential change.
func ProbeCapabilities(ctx context.Context, e *remote.Entry, topicName string, wantZstd bool) (Capabilities, error) {
	caps := DefaultCapabilities()
	path, err := remote.TopicPath(topicName, "produce", "batch")
	if err != nil {
		return caps, err
	}
	body := make([]byte, 0, len(batchPrefix)+3*(DefaultMaxChunkMessages+1)+len(batchSuffix))
	body = append(body, batchPrefix...)
	for i := range DefaultMaxChunkMessages + 1 {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, "{}"...)
	}
	body = append(body, batchSuffix...)
	msg, status, err := probe(ctx, e, path, body, "")
	switch {
	case err != nil:
		return caps, err
	case status == http.StatusBadRequest && msg == "message 0: message required":
		caps.MaxMessages = ProbedMaxChunkMessages
	case status == http.StatusBadRequest && strings.HasPrefix(msg, "too many messages"):
	default:
		return caps, fmt.Errorf("%w: status %d", errProbeInconclusive, status)
	}
	if !wantZstd {
		return caps, nil
	}
	compressed, err := CompressZstd(nil, []byte(batchPrefix+batchSuffix))
	if err != nil {
		return caps, err
	}
	msg, status, err = probe(ctx, e, path, compressed, "zstd")
	switch {
	case err != nil:
		return caps, err
	case status == http.StatusBadRequest && msg == "messages required":
		caps.Zstd = true
	case status == http.StatusUnsupportedMediaType, status == http.StatusBadRequest && strings.HasPrefix(msg, "invalid json"):
	default:
		return caps, fmt.Errorf("%w: status %d", errProbeInconclusive, status)
	}
	return caps, nil
}

func probe(ctx context.Context, e *remote.Entry, path string, body []byte, encoding string) (string, int, error) {
	resp, err := e.Do(ctx, remote.Outbound{Method: http.MethodPost, Path: path, Body: body, ContentType: "application/json", ContentEncoding: encoding})
	if err != nil {
		return "", 0, err
	}
	answer, err := remote.ReadBody(resp, remote.MaxProduceAnswerBytes)
	if err != nil {
		return "", resp.StatusCode, err
	}
	msg, _ := remote.NaradError(resp, answer)
	return msg, resp.StatusCode, nil
}

// MinCompressionSaving is the share of a chunk compression must save to
// be worth sending compressed: already compressed or encrypted payloads
// gain nothing (ch. 10.8).
const MinCompressionSaving = 0.10

var zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic("sink: zstd encoder: " + err.Error())
	}
	return enc
})

// CompressZstd appends src compressed with zstd's fastest level to dst.
// Safe for concurrent use.
func CompressZstd(dst, src []byte) ([]byte, error) {
	return zstdEncoder().EncodeAll(src, dst), nil
}

// MaybeCompress returns body compressed with zstd when that saves at
// least MinCompressionSaving, and whether it did.
func MaybeCompress(body []byte) ([]byte, bool) {
	out, err := CompressZstd(make([]byte, 0, len(body)/2), body)
	if err != nil || float64(len(out)) > float64(len(body))*(1-MinCompressionSaving) {
		return body, false
	}
	return out, true
}
