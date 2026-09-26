package ingress

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"unsafe"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// zzWP5Payload is the distinct payload of record i.
func zzWP5Payload(i int) []byte {
	return fmt.Appendf(nil, `{"n":%d,"pad":"%s"}`, i, bytes.Repeat([]byte("p"), 200+i%7))
}

// zzWP5AcceptConcurrently accepts records 0..n-1 from 16 goroutines (so
// the WAL batches them) with topic topic-(i%3), key k<i>, partition
// i%12 and payload zzWP5Payload(i). WAL order is therefore not i order.
func zzWP5AcceptConcurrently(t *testing.T, m *Manager, n int) {
	t.Helper()
	const workers = 16
	errs := make(chan error, workers)
	for w := range workers {
		go func() {
			for i := w; i < n; i += workers {
				if _, err := m.AcceptProduce(context.Background(), fmt.Sprintf("topic-%d", i%3), fmt.Sprintf("k%d", i), i%12, zzWP5Payload(i)); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range workers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// A dispatcher pass allocated seven objects per record: the frame
// header, the frame payload, a copy of the topic bytes, the topic
// string, a copy of the key bytes, the key string and a copy of the
// payload, plus a 64 KiB read buffer per pass. The payload now aliases
// the frame (which replay allocates fresh per record), topics are
// interned per pass and the header and read buffer are reused, leaving
// the frame and the key.
func TestZZWP5ReplayProduceAllocsPerRecord(t *testing.T) {
	m, err := OpenManager(t.TempDir(), wal.Options{SegmentBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	const n = 256
	zzWP5AcceptConcurrently(t, m, n)
	allocs := testing.AllocsPerRun(20, func() {
		count := 0
		if err := m.ReplayProduceFromCursor(wal.Cursor{}, func(ProduceRecord, wal.Cursor) error {
			count++
			return nil
		}); err != nil || count != n {
			t.Fatalf("replay: %v count=%d", err, count)
		}
	})
	if perRecord := allocs / n; perRecord > 2.25 {
		t.Fatalf("replay allocates %.2f objects per record (%.0f per pass of %d), want at most the frame and the key", perRecord, allocs, n)
	}
}

// Payloads that alias their frames must stay intact after the pass that
// read them, and after later passes over the same records: the
// dispatcher keeps a window's records until they commit. Topic strings
// must be shared within a pass.
func TestZZWP5ReplayProduceRetainedRecordsStayIntact(t *testing.T) {
	m, err := OpenManager(t.TempDir(), wal.Options{SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	const n = 200
	zzWP5AcceptConcurrently(t, m, n)
	var passes [][]ProduceRecord
	for range 3 {
		var kept []ProduceRecord
		if err := m.ReplayProduceFromCursor(wal.Cursor{}, func(r ProduceRecord, _ wal.Cursor) error {
			kept = append(kept, r)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		passes = append(passes, kept)
	}
	for p, kept := range passes {
		if len(kept) != n {
			t.Fatalf("pass %d replayed %d records, want %d", p, len(kept), n)
		}
		names := map[string]*byte{}
		seen := map[int]bool{}
		for idx, r := range kept {
			var i int
			if _, err := fmt.Sscanf(r.Key, "k%d", &i); err != nil || seen[i] {
				t.Fatalf("pass %d record %d: key %q", p, idx, r.Key)
			}
			seen[i] = true
			if !bytes.Equal(r.Payload, zzWP5Payload(i)) || r.Topic != fmt.Sprintf("topic-%d", i%3) || r.TargetPartition != i%12 || r.WAL.Seq != uint64(idx) {
				t.Fatalf("pass %d record %d = %+v (payload %q)", p, idx, r, r.Payload)
			}
			if cap(r.Payload) != len(r.Payload) {
				t.Fatalf("pass %d record %d: payload cap %d > len %d, an append could write into the frame", p, idx, cap(r.Payload), len(r.Payload))
			}
			if first, ok := names[r.Topic]; ok && first != unsafe.StringData(r.Topic) {
				t.Fatalf("pass %d record %d: topic %q not interned", p, idx, r.Topic)
			}
			names[r.Topic] = unsafe.StringData(r.Topic)
		}
	}
	// Records of different passes never share a frame.
	if &passes[0][0].Payload[0] == &passes[1][0].Payload[0] {
		t.Fatal("two passes returned the same payload backing array")
	}
}

// DecodeProduceRecord is exported for callers that do not own their
// buffer: its record must not alias the input.
func TestZZWP5DecodeProduceRecordCopies(t *testing.T) {
	data, err := EncodeProduceRecord(ProduceRecord{Topic: "orders", Key: "k", TargetPartition: 1, Payload: []byte("payload"), CreatedAtUnixMs: 7})
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeProduceRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] = 'X'
	}
	if record.Topic != "orders" || record.Key != "k" || string(record.Payload) != "payload" {
		t.Fatalf("decoded record changed with its input: %+v", record)
	}
}

// The interner is bounded: past maxInternedTopics names it still returns
// the right string, it just stops remembering new ones.
func TestZZWP5TopicInternerBounded(t *testing.T) {
	var topics topicInterner
	for i := range maxInternedTopics + 10 {
		name := fmt.Sprintf("t%d", i)
		if got := topics.intern([]byte(name)); got != name {
			t.Fatalf("intern(%q) = %q", name, got)
		}
	}
	if len(topics.names) != maxInternedTopics {
		t.Fatalf("interner holds %d names, want the cap %d", len(topics.names), maxInternedTopics)
	}
	var nilInterner *topicInterner
	if got := nilInterner.intern([]byte("x")); got != "x" {
		t.Fatalf("nil interner = %q", got)
	}
}
