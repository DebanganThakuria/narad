package storage

import (
	"bytes"
	"errors"
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

// ReadKeyed round-trips key and payload through a real log, across a
// reopen.
func TestReadKeyedThroughLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p00000")
	opts := Options{FlushInterval: time.Millisecond}

	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	if _, err := l.Append(EncodeKeyedRecord("k1", []byte(`{"v":1}`))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Append(EncodeKeyedRecord("", []byte(`{"v":2}`))); err != nil {
		t.Fatalf("Append: %v", err)
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

	l, err = NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog(reopen): %v", err)
	}
	defer l.Close()

	key, payload, err := l.ReadKeyed(0)
	if err != nil || key != "k1" || string(payload) != `{"v":1}` {
		t.Fatalf("ReadKeyed(0) = (%q, %q, %v), want (k1, {\"v\":1}, nil)", key, payload, err)
	}
	key, payload, err = l.ReadKeyed(1)
	if err != nil || key != "" || string(payload) != `{"v":2}` {
		t.Fatalf("ReadKeyed(1) = (%q, %q, %v), want empty key", key, payload, err)
	}
}
