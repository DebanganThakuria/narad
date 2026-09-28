package node

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func zzWP12AckBatch() AckBatchRequest {
	return AckBatchRequest{Items: []AckBatchItem{
		{Topic: "orders", Partition: 0, Offset: 10, Nonce: 100, Mode: AckModeAck},
		{Topic: "orders", Partition: 2, Offset: 11, Nonce: 101, Mode: AckModeExtend},
		{Topic: "payments", Partition: 1, Offset: 12, Nonce: 102, Mode: AckModeNack},
		{Topic: "orders", Partition: 3, Offset: 1 << 40, Nonce: -5, Mode: AckModeAck},
	}}
}

func TestZZWP12AckBatchRequestRoundTrip(t *testing.T) {
	for _, req := range []AckBatchRequest{{}, zzWP12AckBatch()} {
		payload, err := EncodeAckBatchRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		if op, _ := OperationOf(payload); op != OpAckBatch {
			t.Fatalf("operation = %d, want OpAckBatch", op)
		}
		got, err := DecodeAckBatchRequest(payload)
		if err != nil {
			t.Fatalf("DecodeAckBatchRequest() error = %v", err)
		}
		if len(req.Items) == 0 && len(got.Items) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, req) {
			t.Fatalf("round trip = %+v, want %+v", got, req)
		}
	}
}

// The wire carries the op a node too old for OpAckBatch refuses by
// number, so the value must never move.
func TestZZWP12AckBatchOpIsAppended(t *testing.T) {
	if OpAckBatch != 32 {
		t.Fatalf("OpAckBatch = %d, want 32 (after OpTokenNotify)", OpAckBatch)
	}
}

func TestZZWP12AckBatchRequestRejectsMalformed(t *testing.T) {
	payload, err := EncodeAckBatchRequest(zzWP12AckBatch())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(payload); i++ {
		if _, err := DecodeAckBatchRequest(payload[:i]); err == nil {
			t.Fatalf("truncated to %d bytes: decoded without error", i)
		}
	}
	if _, err := DecodeAckBatchRequest(append(bytes.Clone(payload), 0)); err == nil {
		t.Fatal("trailing byte: decoded without error")
	}
	badMode := bytes.Clone(payload)
	badMode[5] = 9 // first item's mode byte, after the op and the count
	if _, err := DecodeAckBatchRequest(badMode); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("invalid mode: error = %v", err)
	}
	// A count the payload cannot hold is refused before anything is
	// allocated for it.
	huge := []byte{byte(OpAckBatch), 0x00, 0x00, 0x03, 0xff}
	if _, err := DecodeAckBatchRequest(huge); err == nil {
		t.Fatal("count past the payload: decoded without error")
	}
	if _, err := DecodeAckBatchRequest([]byte{byte(OpAck), 0, 0, 0, 0}); err == nil {
		t.Fatal("wrong op: decoded without error")
	}
	if _, err := EncodeAckBatchRequest(AckBatchRequest{Items: make([]AckBatchItem, MaxAckBatch+1)}); err == nil {
		t.Fatal("oversize batch: encoded without error")
	}
	if _, err := EncodeAckBatchRequest(AckBatchRequest{Items: []AckBatchItem{{Topic: "t", Mode: 7}}}); err == nil {
		t.Fatal("invalid mode: encoded without error")
	}
}

// A batch for one topic decodes the topic once: the items slice and one
// string, however many records it carries.
func TestZZWP12AckBatchRequestSharesTopic(t *testing.T) {
	if zzWP2Race {
		t.Skip("allocation counts differ under the race detector")
	}
	req := AckBatchRequest{Items: make([]AckBatchItem, 32)}
	for i := range req.Items {
		req.Items[i] = AckBatchItem{Topic: "orders", Partition: i % 3, Offset: int64(i), Nonce: int64(i * 7)}
	}
	payload, err := EncodeAckBatchRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() { _, _ = DecodeAckBatchRequest(payload) }); got != 2 {
		t.Fatalf("decode allocs = %v, want 2 (the items and one topic)", got)
	}
}

func TestZZWP12AckBatchReplyRoundTrip(t *testing.T) {
	results := []AckResult{
		{Status: 204},
		{Status: 410, Error: "receipt handle is stale"},
		{Status: 421, Error: "not the partition owner"},
		{Status: 204},
	}
	body, err := AppendAckBatchReply(nil, results)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeAckBatchReply(body, nil)
	if err != nil {
		t.Fatalf("DecodeAckBatchReply() error = %v", err)
	}
	if !reflect.DeepEqual(got, results) {
		t.Fatalf("round trip = %+v, want %+v", got, results)
	}
	for i := 0; i < len(body); i++ {
		if _, err := DecodeAckBatchReply(body[:i], nil); err == nil {
			t.Fatalf("truncated to %d bytes: decoded without error", i)
		}
	}
	if _, err := DecodeAckBatchReply(append(bytes.Clone(body), 1), nil); err == nil {
		t.Fatal("trailing byte: decoded without error")
	}
	if _, err := AppendAckBatchReply(nil, []AckResult{{Status: 70000}}); err == nil {
		t.Fatal("status past uint16: encoded without error")
	}
}

func FuzzZZWP12DecodeAckBatchRequest(f *testing.F) {
	seed, err := EncodeAckBatchRequest(zzWP12AckBatch())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{byte(OpAckBatch)})
	f.Add([]byte{byte(OpAckBatch), 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		req, err := DecodeAckBatchRequest(payload)
		if err != nil {
			return
		}
		size := 0
		for _, item := range req.Items {
			size += len(item.Topic)
		}
		if size > len(payload) && len(req.Items) > 0 {
			// Shared topic strings may be counted more than once; bound
			// by the distinct bytes instead.
			distinct := map[string]bool{}
			size = 0
			for _, item := range req.Items {
				if !distinct[item.Topic] {
					distinct[item.Topic] = true
					size += len(item.Topic)
				}
			}
			if size > len(payload) {
				t.Fatalf("decoded %d topic bytes from a %d-byte payload", size, len(payload))
			}
		}
		again, err := EncodeAckBatchRequest(req)
		if err != nil {
			t.Fatalf("re-encode of a decoded batch failed: %v", err)
		}
		if !bytes.Equal(again, payload) {
			t.Fatalf("re-encoding differs:\n got %x\nwant %x", again, payload)
		}
	})
}

func FuzzZZWP12DecodeAckBatchReply(f *testing.F) {
	seed, err := AppendAckBatchReply(nil, []AckResult{{Status: 204}, {Status: 410, Error: "stale"}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, body []byte) {
		results, err := DecodeAckBatchReply(body, nil)
		if err != nil {
			return
		}
		again, err := AppendAckBatchReply(nil, results)
		if err != nil {
			t.Fatalf("re-encode of a decoded reply failed: %v", err)
		}
		if !bytes.Equal(again, body) {
			t.Fatalf("re-encoding differs:\n got %x\nwant %x", again, body)
		}
	})
}
