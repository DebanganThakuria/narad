package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// zzWP20ReplyBound is the largest body a local batch consume may build
// with more than one record in it.
const zzWP20ReplyBound = 8 << 20

// zzWP20Record is record i of a partition whose records all carry
// payload: its offset is i and its receipt handle is unique.
func zzWP20Record(i int, payload []byte) topic.Message {
	return topic.Message{
		Topic: "orders", Partition: 0, Offset: int64(i), Payload: payload, Timestamp: 1790000000,
		ReceiptHandle: consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: int64(i), Nonce: int64(i) + 1}),
	}
}

// zzWP20BoundBroker is a local partition of total records carrying
// payload. ConsumeBatch reserves them in order as the engine does,
// stopping at max or once the records taken carry opts.MaxBytes of key
// and payload. With burst set, ConsumeWait hands over the next record
// and the partition then holds burst records, as a burst landing wakes
// a parked consume. Nacks are recorded.
type zzWP20BoundBroker struct {
	*fakeBroker
	payload []byte
	total   int
	burst   int

	mu     sync.Mutex
	next   int
	opts   []brokermsg.ConsumeOpts
	nacked []int64
}

func newZZWP20BoundBroker(total int, payload []byte) *zzWP20BoundBroker {
	b := &zzWP20BoundBroker{payload: payload, total: total}
	b.fakeBroker = &fakeBroker{nackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		b.mu.Lock()
		b.nacked = append(b.nacked, h.Offset)
		b.mu.Unlock()
		return nil
	}}
	return b
}

func (b *zzWP20BoundBroker) ConsumeBatch(_ context.Context, _ string, opts brokermsg.ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opts = append(b.opts, opts)
	got, size := 0, 0
	for got < max && (opts.MaxBytes <= 0 || size < opts.MaxBytes) && b.next < b.total {
		m := zzWP20Record(b.next, b.payload)
		b.next++
		dst = append(dst, m)
		got++
		size += len(m.Key) + len(m.Payload)
	}
	if got == 0 {
		return dst, &brokermsg.ConsumeWaiter{}, nil
	}
	return dst, nil, nil
}

func (b *zzWP20BoundBroker) ConsumeWait(context.Context, *brokermsg.ConsumeWaiter, time.Duration, <-chan struct{}) (topic.Message, bool, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.burst == 0 {
		return topic.Message{}, false, false, nil
	}
	m := zzWP20Record(b.next, b.payload)
	b.next++
	b.total, b.burst = b.burst, 0
	return m, true, false, nil
}

// reserved is how many records ConsumeBatch handed out, and nacks the
// offsets given back, in order.
func (b *zzWP20BoundBroker) reserved() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.next
}

func (b *zzWP20BoundBroker) nacks() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.nacked...)
}

func zzWP20Batch(t *testing.T, res *httptest.ResponseRecorder) []topic.Message {
	t.Helper()
	if res.Code != http.StatusOK {
		t.Fatalf("status %d (%.200s), want 200", res.Code, res.Body)
	}
	var reply struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &reply); err != nil {
		t.Fatalf("reply: %v", err)
	}
	return reply.Messages
}

// zzWP20CheckBound checks one batch response against what the broker
// reserved: the records that went out are the first ones reserved, in
// order and intact; every other reserved record was given back once,
// at once; and the body stays within the bound unless it carries a
// single record.
func zzWP20CheckBound(t *testing.T, br *zzWP20BoundBroker, first int64, res *httptest.ResponseRecorder, payload []byte) []topic.Message {
	t.Helper()
	msgs := zzWP20Batch(t, res)
	if len(msgs) == 0 {
		t.Fatal("the batch carried no records")
	}
	if len(msgs) > 1 && res.Body.Len() > zzWP20ReplyBound {
		t.Fatalf("a batch of %d records is %d bytes, want at most %d", len(msgs), res.Body.Len(), zzWP20ReplyBound)
	}
	for i, m := range msgs {
		if m.Offset != first+int64(i) {
			t.Fatalf("record %d has offset %d, want %d: records go out in the order reserved", i, m.Offset, first+int64(i))
		}
		var text string
		got := []byte(m.Payload)
		if json.Unmarshal(m.Payload, &text) == nil {
			got = []byte(text)
		}
		if string(got) != string(payload) {
			t.Fatalf("record %d's payload came back altered", i)
		}
	}
	nacks := br.nacks()
	left := br.reserved() - int(first) - len(msgs)
	if len(nacks) != left {
		t.Fatalf("%d records reserved, %d sent, %d given back; want every record left out given back", br.reserved()-int(first), len(msgs), len(nacks))
	}
	for i, off := range nacks {
		if want := first + int64(len(msgs)+i); off != want {
			t.Fatalf("gave back offset %d, want %d", off, want)
		}
	}
	return msgs
}

// TestZZWP20LocalBatchConsumeEncodedBound checks that a batch consume
// served from this node's own partitions stops before the record that
// would take its body past the bound, and gives the records it leaves
// out back at once. Nothing bounded it: 100 records of 1 MiB, each up to
// six times that once escaped, all went into one buffer. Text that is
// not JSON goes out as a JSON string, where a NUL or one of < > & takes
// six bytes, so 64 KiB of NULs is 384 KiB in the body.
func TestZZWP20LocalBatchConsumeEncodedBound(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"zero-filled payload", make([]byte, 64<<10)},
		{"markup payload", []byte(strings.Repeat("<a>&", 16<<10))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := newZZWP20BoundBroker(100, tc.payload)
			res := zzWP12Consume(newTestSet(br, nil), "?max=100")
			msgs := zzWP20CheckBound(t, br, 0, res, tc.payload)
			if len(msgs) < 2 || len(msgs) == br.reserved() {
				t.Fatalf("sent %d of %d reserved records, want the bound to cut the batch", len(msgs), br.reserved())
			}
			if br.opts[0].MaxBytes <= 0 {
				t.Fatalf("the scan reserved without a byte bound (%+v)", br.opts[0])
			}
		})
	}
}

// TestZZWP20LocalBatchConsumeBoundAfterWait checks the bound covers the
// top-up after a wait: the waited record goes out first, and what the
// top-up adds is cut and given back the same way.
func TestZZWP20LocalBatchConsumeBoundAfterWait(t *testing.T) {
	payload := make([]byte, 64<<10)
	br := newZZWP20BoundBroker(0, payload)
	br.burst = 101
	res := zzWP12Consume(newTestSet(br, nil), "?max=100&wait=1s")
	msgs := zzWP20CheckBound(t, br, 0, res, payload)
	if len(msgs) < 2 || len(msgs) == br.reserved() {
		t.Fatalf("sent %d of %d reserved records, want the waited one topped up and the top-up cut", len(msgs), br.reserved())
	}
}

// TestZZWP20LocalBatchConsumeFirstRecordAlwaysGoes checks a record whose
// encoding alone passes the bound still goes out, alone, as a single
// consume would send it.
func TestZZWP20LocalBatchConsumeFirstRecordAlwaysGoes(t *testing.T) {
	payload := make([]byte, 2<<20) // 12 MiB escaped
	br := newZZWP20BoundBroker(3, payload)
	res := zzWP12Consume(newTestSet(br, nil), "?max=3")
	msgs := zzWP20CheckBound(t, br, 0, res, payload)
	if len(msgs) != 1 {
		t.Fatalf("sent %d records, want the first alone", len(msgs))
	}
}

// TestZZWP20LocalBatchConsumeJSONUnaffected checks that JSON records,
// which go out as they are, fill a batch as before: the bounds only cut
// what escaping inflates.
func TestZZWP20LocalBatchConsumeJSONUnaffected(t *testing.T) {
	payload := []byte(`"` + strings.Repeat("x", 30<<10) + `"`)
	br := newZZWP20BoundBroker(100, payload)
	res := zzWP12Consume(newTestSet(br, nil), "?max=100")
	if msgs := zzWP20CheckBound(t, br, 0, res, []byte(strings.Repeat("x", 30<<10))); len(msgs) != 100 {
		t.Fatalf("sent %d of 100 JSON records of 30 KiB, want all", len(msgs))
	}
}

// BenchmarkZZWP20LocalBatchConsume is a local batch consume through the
// handler, with a broker that always has max small JSON records.
func BenchmarkZZWP20LocalBatchConsume(b *testing.B) {
	for _, max := range []int{10, 100} {
		b.Run(fmt.Sprintf("max=%d", max), func(b *testing.B) {
			msgs := make([]topic.Message, max)
			for i := range msgs {
				msgs[i] = zzWP18ConsumeMsg
				msgs[i].Offset = int64(i)
			}
			br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) { return msgs, nil }}
			h := Consume(newTestSet(br, nil))
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/topics/orders/consume?max=%d", max), nil)
			req.SetPathValue("topic", "orders")
			w := &zzWP18DiscardWriter{h: http.Header{}}
			b.ReportAllocs()
			for b.Loop() {
				br.calls = br.calls[:0]
				clear(w.h)
				w.status, w.n = 0, 0
				h(w, req)
				if w.status != http.StatusOK {
					b.Fatalf("status %d", w.status)
				}
			}
		})
	}
}
