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
			const ts = int64(1_720_000_000_123)
			enc := EncodeKeyedRecord(tc.key, ts, tc.payload)
			key, committedAt, payload, err := DecodeKeyedRecord(enc)
			if err != nil {
				t.Fatalf("DecodeKeyedRecord() error = %v", err)
			}
			if key != tc.key || committedAt != ts || !bytes.Equal(payload, tc.payload) {
				t.Fatalf("round trip = (%q, %d, %q), want (%q, %d, %q)", key, committedAt, payload, tc.key, ts, tc.payload)
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
		{"timestamp truncated", append([]byte{keyedRecordVersion, 0x01}, 'k', 0x00)},
		{"retired v1 layout", append([]byte{0x01, 0x02}, 'k', '1', 'p', 'a', 'y', 'l', 'o', 'a', 'd', '!')},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := DecodeKeyedRecord(tc.in); !errors.Is(err, ErrCorruptRecord) {
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
	if _, err := l.Append(EncodeKeyedRecord("k1", 111, []byte(`{"v":1}`))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Append(EncodeKeyedRecord("", 222, []byte(`{"v":2}`))); err != nil {
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

	key, ts, payload, err := l.ReadKeyed(0)
	if err != nil || key != "k1" || ts != 111 || string(payload) != `{"v":1}` {
		t.Fatalf("ReadKeyed(0) = (%q, %d, %q, %v), want (k1, 111, {\"v\":1}, nil)", key, ts, payload, err)
	}
	key, ts, payload, err = l.ReadKeyed(1)
	if err != nil || key != "" || ts != 222 || string(payload) != `{"v":2}` {
		t.Fatalf("ReadKeyed(1) = (%q, %d, %q, %v), want empty key + ts 222", key, ts, payload, err)
	}
}
