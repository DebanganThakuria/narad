package sink

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestRawPayload(t *testing.T) {
	for _, tc := range []struct {
		payload string
		raw     bool
	}{
		{`{"a":1}`, true},
		{`[1,2]`, true},
		{`"text"`, true},
		{`42`, true},
		{`null`, true},
		{`{"a": 1}`, true}, // inner whitespace is part of the value
		{` {"a":1}`, false},
		{`{"a":1} `, false},
		{"{\"a\":1}\n", false},
		{`{"a":1}{"b":2}`, false},
		{`not json`, false},
		{"", false},
		{"\"\xff\"", false}, // invalid UTF-8 inside a string goes base64
	} {
		if got := RawPayload([]byte(tc.payload)); got != tc.raw {
			t.Errorf("RawPayload(%q) = %v, want %v", tc.payload, got, tc.raw)
		}
	}
}

// decoded is one message as the target's batch decoder reads it; the
// round trip through the target's real decoder lives in the handler's
// tests (handlers/messaging), this checks the wire shape.
type decoded struct {
	Key             *string         `json:"key"`
	KeyEncoding     string          `json:"key_encoding"`
	Payload         json.RawMessage `json:"payload"`
	PayloadEncoding string          `json:"payload_encoding"`
	Partition       *int            `json:"partition"`
}

func decodeBatch(t *testing.T, body []byte) []decoded {
	t.Helper()
	var b struct {
		Messages []decoded `json:"messages"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		t.Fatalf("batch body does not decode strictly: %v\n%s", err, body)
	}
	return b.Messages
}

func TestAppendMessageShapes(t *testing.T) {
	recs := []topic.KeyedRecord{
		{Key: "k1", Payload: []byte(`{"x":"<&>"}`)},
		{Key: "\xff\x00", Payload: []byte{0, 1, 2, 255}},
		{Payload: []byte(` {"padded":true} `)},
	}
	msgs := decodeBatch(t, AppendBatch(nil, recs, nil))
	if len(msgs) != 3 {
		t.Fatalf("decoded %d messages, want 3", len(msgs))
	}
	if *msgs[0].Key != "k1" || msgs[0].KeyEncoding != "" || string(msgs[0].Payload) != `{"x":"<&>"}` || msgs[0].PayloadEncoding != "" {
		t.Fatalf("message 0 = %+v, want the key as a string and the payload raw, byte for byte", msgs[0])
	}
	key, _ := base64.StdEncoding.DecodeString(*msgs[1].Key)
	var p string
	_ = json.Unmarshal(msgs[1].Payload, &p)
	payload, _ := base64.StdEncoding.DecodeString(p)
	if msgs[1].KeyEncoding != "base64" || string(key) != "\xff\x00" || msgs[1].PayloadEncoding != "base64" || !bytes.Equal(payload, []byte{0, 1, 2, 255}) {
		t.Fatalf("message 1 = %+v, want base64 key and payload", msgs[1])
	}
	if msgs[2].Key != nil || msgs[2].PayloadEncoding != "base64" {
		t.Fatalf("message 2 = %+v, want no key and a base64 payload (surrounding whitespace)", msgs[2])
	}
	for i, m := range msgs {
		if m.Partition != nil {
			t.Fatalf("message %d names a partition; the target's partitioner decides", i)
		}
	}
}

func TestAppendMessageForcedBase64(t *testing.T) {
	msgs := decodeBatch(t, AppendBatch(nil, []topic.KeyedRecord{{Key: "k", Payload: []byte(`{"a":1}`)}}, func(int) bool { return true }))
	if msgs[0].PayloadEncoding != "base64" {
		t.Fatalf("forced base64 went raw: %+v", msgs[0])
	}
}

func records(n, size int) []topic.KeyedRecord {
	out := make([]topic.KeyedRecord, n)
	for i := range out {
		out[i] = topic.KeyedRecord{Key: "k", Offset: int64(i), Payload: []byte(`"` + strings.Repeat("x", size) + `"`)}
	}
	return out
}

func TestChunkBuilderCaps(t *testing.T) {
	var b ChunkBuilder
	body, n := b.Build(records(150, 10), DefaultMaxChunkMessages, MaxChunkBytes, nil)
	if n != 100 || len(decodeBatch(t, body)) != 100 {
		t.Fatalf("n = %d, want the 100-message cap", n)
	}
	body, n = b.Build(records(1500, 10), ProbedMaxChunkMessages, MaxChunkBytes, nil)
	if n != 1000 || len(decodeBatch(t, body)) != 1000 {
		t.Fatalf("n = %d, want the probed 1,000-message cap", n)
	}
	// 20 KiB records: the byte cap binds before the message cap.
	body, n = b.Build(records(100, 20<<10), DefaultMaxChunkMessages, MaxChunkBytes, nil)
	if len(body) > MaxChunkBytes || n >= 100 || n != len(decodeBatch(t, body)) {
		t.Fatalf("n = %d, body = %d bytes, want a chunk under %d bytes", n, len(body), MaxChunkBytes)
	}
	// A record larger than the cap goes alone.
	big := records(3, MaxChunkBytes+1)
	body, n = b.Build(big, DefaultMaxChunkMessages, MaxChunkBytes, nil)
	if n != 1 || len(decodeBatch(t, body)) != 1 {
		t.Fatalf("an oversize record shared its chunk: n = %d", n)
	}
	// Two chunks never share a buffer: the transport may still be
	// writing the first when the second is built.
	first, _ := b.Build(records(2, 10), 2, MaxChunkBytes, nil)
	second, _ := b.Build(records(2, 10), 2, MaxChunkBytes, nil)
	if &first[0] == &second[0] {
		t.Fatal("two chunks share one buffer")
	}
}

func TestLanesKeepKeysTogetherAndOrder(t *testing.T) {
	var recs []topic.KeyedRecord
	for i := range 400 {
		key := ""
		if i%3 != 0 {
			key = "key-" + string(rune('a'+i%7))
		}
		recs = append(recs, topic.KeyedRecord{Key: key, Offset: int64(1000 + i)})
	}
	lanes := SplitLanes(recs, 4)
	laneOfKey := map[string]int{}
	total := 0
	for l, lane := range lanes {
		for i, rec := range lane {
			total++
			if i > 0 && rec.Offset <= lane[i-1].Offset {
				t.Fatalf("lane %d out of slab order at %d", l, i)
			}
			if rec.Key == "" {
				if want := int(rec.Offset % 4); l != want {
					t.Fatalf("keyless offset %d on lane %d, want %d", rec.Offset, l, want)
				}
				continue
			}
			if prev, ok := laneOfKey[rec.Key]; ok && prev != l {
				t.Fatalf("key %q on lanes %d and %d", rec.Key, prev, l)
			}
			laneOfKey[rec.Key] = l
		}
	}
	if total != len(recs) {
		t.Fatalf("lanes hold %d records, want %d", total, len(recs))
	}
	if one := SplitLanes(recs, 1); len(one) != 1 || len(one[0]) != len(recs) {
		t.Fatal("one lane must hold the slab as it is")
	}
}

func TestChunkCapAdapts(t *testing.T) {
	c := NewChunkCap()
	for range 10 {
		c.Shrink()
	}
	if c.Bytes() != MinChunkBytes {
		t.Fatalf("cap = %d after many timeouts, want the %d floor", c.Bytes(), MinChunkBytes)
	}
	for range growAfterSuccesses - 1 {
		c.Accepted()
	}
	if c.Bytes() != MinChunkBytes {
		t.Fatal("the cap grew before 20 accepted chunks in a row")
	}
	c.Accepted()
	if c.Bytes() != 2*MinChunkBytes {
		t.Fatalf("cap = %d after 20 accepted chunks, want it doubled", c.Bytes())
	}
	c.Shrink()
	for range 20 * 10 {
		c.Accepted()
	}
	if c.Bytes() != MaxChunkBytes {
		t.Fatalf("cap = %d, want it back at %d", c.Bytes(), MaxChunkBytes)
	}
}

func TestBackoffFullJitterWithinBounds(t *testing.T) {
	b := LaneBackoff()
	ceiling := b.Min
	for range 20 {
		d := b.Next()
		if d <= 0 || d > ceiling {
			t.Fatalf("wait %s outside (0, %s]", d, ceiling)
		}
		ceiling = min(ceiling*2, b.Max)
	}
	b.Reset()
	if d := b.Next(); d > b.Min {
		t.Fatalf("after Reset the wait %s exceeds %s", d, b.Min)
	}
}
