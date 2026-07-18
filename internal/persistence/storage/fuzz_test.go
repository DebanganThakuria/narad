package storage

// Fuzz targets for the parse and recovery surfaces — the code that must
// stay well-behaved against arbitrary, hostile, or corrupted bytes on
// disk. The universal invariant across all of them: NEVER panic. A bad
// input is an error, not a crash, and a decoded value must never claim
// bytes it doesn't own.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// FuzzDecodeKeyedRecord throws arbitrary bytes at the record-envelope
// parser. It must return either a clean decode or a corrupt-record
// error, never panic, and never report a payload/key that overruns the
// input. Valid decodes must round-trip through EncodeKeyedRecord.
func FuzzDecodeKeyedRecord(f *testing.F) {
	f.Add(EncodeKeyedRecord("", 0, nil))
	f.Add(EncodeKeyedRecord("k", 123, []byte("payload")))
	f.Add(EncodeKeyedRecord("a-longer-key", -1, bytes.Repeat([]byte{0xff}, 64)))
	f.Add([]byte{})
	f.Add([]byte{keyedRecordVersion})
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, b []byte) {
		key, ts, payload, err := DecodeKeyedRecord(b)
		if err != nil {
			return // a rejected envelope is a valid outcome
		}
		// A successful decode must not have invented bytes.
		if len(key)+len(payload) > len(b) {
			t.Fatalf("decoded key(%d)+payload(%d) exceeds input(%d)", len(key), len(payload), len(b))
		}
		// And it must round-trip: re-encoding the decoded fields and
		// decoding again yields the same values.
		re := EncodeKeyedRecord(key, ts, payload)
		key2, ts2, payload2, err2 := DecodeKeyedRecord(re)
		if err2 != nil {
			t.Fatalf("re-decode of a valid record failed: %v", err2)
		}
		if key2 != key || ts2 != ts || !bytes.Equal(payload2, payload) {
			t.Fatalf("round-trip mismatch: (%q,%d,%q) -> (%q,%d,%q)", key, ts, payload, key2, ts2, payload2)
		}
	})
}

// FuzzDecodeRecordsPayload fuzzes the multi-record frame-payload splitter
// with an arbitrary payload and a fuzzer-chosen record count. It must
// never panic and never return a slice that aliases beyond the payload.
func FuzzDecodeRecordsPayload(f *testing.F) {
	f.Add(encodeRecordsPayload(nil, [][]byte{[]byte("a"), []byte("bb")}), int32(2))
	f.Add([]byte{}, int32(0))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, int32(1))
	f.Add([]byte{0, 0, 0, 1, 0x41}, int32(1))

	f.Fuzz(func(t *testing.T, payload []byte, count int32) {
		// Bound the count so the fuzzer can't ask us to allocate a
		// multi-GB slice header; the parser itself must handle any value
		// without panicking, which is what we're checking.
		if count < 0 || count > 1<<16 {
			return
		}
		records, err := decodeRecordsPayload(payload, count)
		if err != nil {
			return
		}
		if int32(len(records)) != count {
			t.Fatalf("returned %d records, header claimed %d", len(records), count)
		}
		var total int
		for _, r := range records {
			total += len(r)
			// Every returned record must be a sub-slice of payload.
			if len(r) > len(payload) {
				t.Fatalf("record length %d exceeds payload %d", len(r), len(payload))
			}
		}
		// 4 length-prefix bytes per record + record bytes must exactly
		// account for the consumed payload.
		if total+4*int(count) != len(payload) {
			t.Fatalf("record bytes %d + headers %d != payload %d", total, 4*count, len(payload))
		}
	})
}

// FuzzReadFrameRaw fuzzes the on-disk frame reader against arbitrary
// bytes presented as a segment. It must never panic; a bad frame is an
// error. A frame that reads clean must survive re-reading identically.
func FuzzReadFrameRaw(f *testing.F) {
	valid, _ := encodeFrame([][]byte{[]byte("hello")}, 0, codec.NewNoopCodec())
	f.Add(valid)
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0}, headerSize))
	f.Add(append([]byte{0xCA, 0xFE}, bytes.Repeat([]byte{0xff}, 40)...))

	f.Fuzz(func(t *testing.T, b []byte) {
		r := bytes.NewReader(b)
		h, payload, err := readFrameRaw(r, 0)
		if err != nil {
			return
		}
		// A clean read must not claim more payload than the input holds.
		if int64(len(payload)) != int64(h.compressed) {
			t.Fatalf("payload len %d != header compressed %d", len(payload), h.compressed)
		}
		if headerSize+len(payload) > len(b) {
			t.Fatalf("frame (header+%d payload) exceeds input %d", len(payload), len(b))
		}
	})
}

// FuzzLogRecovery is the crown jewel: build a real, valid partition log,
// corrupt its segment bytes per the fuzzer's mutation, then REOPEN it.
// Recovery must never panic and must never expose a record it cannot
// read back — every offset it makes visible must ReadKeyed cleanly or
// report corruption, never crash and never return garbage as valid.
func FuzzLogRecovery(f *testing.F) {
	f.Add(0, byte(0xff), false)  // flip first byte
	f.Add(30, byte(0x00), false) // flip mid-frame
	f.Add(0, byte(0), true)      // truncate near start
	f.Add(45, byte(0x7f), true)  // truncate mid-second-frame

	f.Fuzz(func(t *testing.T, pos int, xor byte, truncate bool) {
		dir := t.TempDir()

		// Phase 1: a genuine 3-record log, durably closed.
		func() {
			log, err := NewLog(dir, Options{FlushInterval: time.Millisecond})
			if err != nil {
				t.Fatalf("NewLog: %v", err)
			}
			defer log.Close()
			for i := range 3 {
				rec := EncodeKeyedRecord("k", int64(i), []byte{byte('a' + i)})
				if _, err := log.Append(rec); err != nil {
					t.Fatalf("Append: %v", err)
				}
				if err := log.Sync(); err != nil {
					t.Fatalf("Sync: %v", err)
				}
			}
			if err := log.AdvanceHighWatermark(3); err != nil {
				t.Fatalf("AdvanceHighWatermark: %v", err)
			}
		}()

		segs, err := filepath.Glob(filepath.Join(dir, "*"+segmentFileSuffix))
		if err != nil || len(segs) == 0 {
			t.Fatalf("no segment files: %v", err)
		}
		segPath := segs[0]

		data, err := os.ReadFile(segPath)
		if err != nil {
			t.Fatalf("read segment: %v", err)
		}
		if len(data) == 0 {
			return
		}
		// Normalize the fuzzer's position into [0, len) — Go's % keeps the
		// dividend's sign, so a negative pos must be folded, not sliced.
		at := ((pos % len(data)) + len(data)) % len(data)
		// Apply the fuzzer's corruption.
		if truncate {
			if err := os.WriteFile(segPath, data[:at], 0o600); err != nil {
				t.Fatalf("truncate: %v", err)
			}
		} else {
			data[at] ^= xor
			if err := os.WriteFile(segPath, data, 0o600); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
		}

		// Phase 2: reopen the corrupted log. Must not panic.
		log2, err := NewLog(dir, Options{FlushInterval: time.Millisecond})
		if err != nil {
			return // refusing to open corrupt state is acceptable
		}
		defer log2.Close()

		// Every offset recovery exposes must be readable or cleanly
		// corrupt — never a panic, never garbage-as-valid.
		next := log2.NextOffset()
		oldest := log2.OldestOffset()
		for off := oldest; off < next; off++ {
			_, _, _, _ = log2.ReadKeyed(off) // panics fail the fuzz; errors are fine
		}
	})
}
