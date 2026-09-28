package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// The consumer state files keep v3.0.1's bytes: a rollback reads what
// this tree writes, and the offset committer writes them itself through
// these encoders. The digests below are of v3.0.1's encoder output for
// the same inputs; a change here is a format change and needs a
// rollback story.
func TestZZWP23ConsumerStateBytesAreV301s(t *testing.T) {
	if got := EncodeConsumerOffset(0x0102030405060708); got != [8]byte{1, 2, 3, 4, 5, 6, 7, 8} {
		t.Fatalf("EncodeConsumerOffset = %x, want big-endian 0102030405060708", got)
	}
	if got := EncodeConsumerOffset(-1); got != [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff} {
		t.Fatalf("EncodeConsumerOffset(-1) = %x", got)
	}
	for _, tc := range []struct {
		seq       uint64
		committed int64
		offsets   []int64
		digest    string
	}{
		{0x1122334455667788, 41, []int64{50, 43, 50, 40, 1 << 40}, "32c24536e3fca9441cf69b7547b3790fb91aa7ea86179436d68703126e04e990"},
		{1, -1, nil, "684e6015cb304562e3c1c5a431c79ac77a01820e3909859c8df18ebef0eac5de"},
	} {
		slot := EncodeConsumerAhead(tc.seq, tc.committed, tc.offsets)
		sum := sha256.Sum256(slot)
		if got := hex.EncodeToString(sum[:]); got != tc.digest {
			t.Errorf("EncodeConsumerAhead(%#x, %d, %v) digest %s, want %s", tc.seq, tc.committed, tc.offsets, got, tc.digest)
		}
	}
	if ConsumerAheadSlotSize != 4096 || ConsumerOffsetFileName != "consumer.offset" || ConsumerAheadFileName != "consumer.ahead" || ConsumerStateFileMode != 0o600 {
		t.Fatal("consumer state file names, slot size or mode changed")
	}
}

// DecodeConsumerAheadSlot is the validation ReadConsumerAhead applies
// to each slot.
func TestZZWP23DecodeConsumerAheadSlotMatchesTheReader(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConsumerAhead(dir, 0, 7, 3, []int64{5, 9}); err != nil {
		t.Fatal(err)
	}
	if err := WriteConsumerAhead(dir, 1, 8, 4, []int64{6}); err != nil {
		t.Fatal(err)
	}
	buf, err := os.ReadFile(filepath.Join(dir, ConsumerAheadFileName))
	if err != nil {
		t.Fatal(err)
	}
	newest, ok, err := ReadConsumerAhead(dir)
	if err != nil || !ok {
		t.Fatalf("ReadConsumerAhead: ok %v err %v", ok, err)
	}
	for slot, want := range []struct {
		seq       uint64
		committed int64
		n         int
	}{{7, 3, 2}, {8, 4, 1}} {
		rec, ok := DecodeConsumerAheadSlot(buf[slot*ConsumerAheadSlotSize : (slot+1)*ConsumerAheadSlotSize])
		if !ok || rec.Seq != want.seq || rec.Committed != want.committed || len(rec.Offsets) != want.n {
			t.Fatalf("slot %d decodes %+v ok %v", slot, rec, ok)
		}
		if slot == newest.Slot && rec.Seq != newest.Seq {
			t.Fatalf("slot %d decodes seq %d, the reader's newest from it has %d", slot, rec.Seq, newest.Seq)
		}
	}
	torn := append([]byte(nil), buf[:ConsumerAheadSlotSize]...)
	torn[20] ^= 0xff // inside the committed field
	if _, ok := DecodeConsumerAheadSlot(torn); ok {
		t.Fatal("a torn slot decoded")
	}
}
