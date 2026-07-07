package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKeyedRecordRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		payload []byte
	}{
		{"empty key", "", []byte(`{"a":1}`)},
		{"simple key", "order-42", []byte(`{"a":1}`)},
		{"long key", strings.Repeat("k", 4096), []byte("p")},
		{"binary payload", "k", []byte{0x00, 0xCA, 0xFE, 0x01, 0xFF}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc := EncodeKeyedRecord(tc.key, tc.payload)
			key, payload, err := DecodeKeyedRecord(enc)
			if err != nil {
				t.Fatalf("DecodeKeyedRecord() error = %v", err)
			}
			if key != tc.key || !bytes.Equal(payload, tc.payload) {
				t.Fatalf("round trip = (%q, %q), want (%q, %q)", key, payload, tc.key, tc.payload)
			}
		})
	}
}

func TestKeyedRecordDecodeRejectsCorrupt(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"unknown version", []byte{0x7B, 'x'}},
		{"truncated varint", []byte{keyedRecordVersion}},
		{"key length overruns", append([]byte{keyedRecordVersion}, 0xFF, 0x01)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := DecodeKeyedRecord(tc.in); !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("DecodeKeyedRecord(%v) error = %v, want %v", tc.in, err, ErrCorruptRecord)
			}
		})
	}
}

// A log written before the keyed envelope existed has no keyed.from
// marker: opening it must stamp the marker at the recovered tail so
// old records read as bare payloads while new appends decode.
func TestReadKeyedHonorsKeyedFromMarker(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p00000")
	opts := Options{FlushInterval: time.Millisecond}

	legacy := [][]byte{[]byte(`{"legacy":1}`), []byte(`{"legacy":2}`)}
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	for _, p := range legacy {
		if _, err := l.Append(p); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := l.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Simulate the pre-envelope on-disk state: the marker this open
	// wrote never existed back then.
	if err := os.Remove(filepath.Join(dir, keyedFromFileName)); err != nil {
		t.Fatalf("remove marker: %v", err)
	}

	l, err = NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog(reopen): %v", err)
	}
	defer l.Close()
	if got := l.KeyedFromOffset(); got != 2 {
		t.Fatalf("KeyedFromOffset() = %d, want 2 (recovered tail)", got)
	}

	if _, err := l.Append(EncodeKeyedRecord("k1", []byte(`{"new":3}`))); err != nil {
		t.Fatalf("Append keyed: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := l.AdvanceHighWatermark(3); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}

	for i, want := range legacy {
		key, payload, err := l.ReadKeyed(int64(i))
		if err != nil {
			t.Fatalf("ReadKeyed(%d): %v", i, err)
		}
		if key != "" || !bytes.Equal(payload, want) {
			t.Fatalf("ReadKeyed(%d) = (%q, %q), want bare legacy payload %q", i, key, payload, want)
		}
	}
	key, payload, err := l.ReadKeyed(2)
	if err != nil {
		t.Fatalf("ReadKeyed(2): %v", err)
	}
	if key != "k1" || string(payload) != `{"new":3}` {
		t.Fatalf("ReadKeyed(2) = (%q, %q), want decoded envelope", key, payload)
	}

	// The marker must survive a reopen (not be re-stamped at the new tail).
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l, err = NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog(reopen 2): %v", err)
	}
	defer l.Close()
	if got := l.KeyedFromOffset(); got != 2 {
		t.Fatalf("KeyedFromOffset() after reopen = %d, want 2", got)
	}
}

// A fresh partition is fully keyed from offset zero.
func TestNewLogStampsKeyedFromZero(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p00000"), Options{FlushInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()
	if got := l.KeyedFromOffset(); got != 0 {
		t.Fatalf("KeyedFromOffset() = %d, want 0", got)
	}
}
