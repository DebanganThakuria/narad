package messaging

import (
	"bytes"
	"encoding/json"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// A batch of binary records, whose keys and payloads go out base64, is
// built in the one buffer appendMessages sizes for it: each record with
// its widest numbers and both encoding flags fits the estimate, so the
// buffer never grows. Sized on the raw lengths it grew twice near the
// scan's reserve, allocating about three times the body and copying
// several MiB each time. Encoding a binary record also allocates a
// little on its own (json.Valid builds its error), so the check is on
// the bytes allocated: about the body once, not the body and the
// buffers it outgrew.
func TestShipAppendMessagesBinaryBatchAllocatesOnce(t *testing.T) {
	key := strings.Repeat("\xff", 32)
	payload := bytes.Repeat([]byte{0xff, 0xfe}, 20<<10)
	msgs := make([]topic.Message, MaxConsumeBatch)
	for i := range msgs {
		msgs[i] = topic.Message{
			Topic: "orders", Partition: math.MaxInt32, Offset: math.MaxInt64 - int64(i),
			Key: key, Payload: payload, Timestamp: math.MaxInt64,
			ReceiptHandle: consumer.EncodeHandle(consumer.Handle{Partition: math.MaxInt32, Offset: math.MaxInt64, Nonce: math.MaxInt64}),
		}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	body, sent := appendMessages(nil, msgs)
	runtime.ReadMemStats(&after)
	if sent != len(msgs) {
		t.Fatalf("sent %d of %d records, want all within the reply bound", sent, len(msgs))
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > uint64(len(body))*5/4 {
		t.Fatalf("a %d-byte batch of %d binary records allocated %d bytes (%.2fx), want about the body once",
			len(body), len(msgs), allocated, float64(allocated)/float64(len(body)))
	}
	var reply struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(body, &reply); err != nil || len(reply.Messages) != len(msgs) {
		t.Fatalf("body decodes to %d records (%v), want %d", len(reply.Messages), err, len(msgs))
	}
}
