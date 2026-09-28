package runtime

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"slices"
)

// v3.0.1's recovery of one partition's consumer state, kept here so a
// rollback is tested against the release's reader and not against
// whatever this tree's readers become. The logic is v3.0.1's, from
// git show v3.0.1:internal/persistence/storage/consumer_ahead.go
// (decodeConsumerAheadSlot, ReadConsumerAhead),
// internal/persistence/storage/consumer_offset.go (ReadConsumerOffset),
// cmd/narad/serve_wiring.go (a read error recovers nothing) and
// internal/consumer/inflight.go:351-370 with shard.go's
// seedAheadLocked (the larger frontier wins, then the frontier
// collapses over the acked-ahead run right above it).

const (
	zzV301SlotSize = 4096
	zzV301HdrLen   = 4 + 1 + 3 + 8 + 8 + 4 + 4 + 4
	zzV301CRCOff   = zzV301HdrLen - 4
)

var (
	zzV301Magic = [4]byte{'N', 'A', 'A', 'H'}
	zzV301CRC   = crc32.MakeTable(crc32.Castagnoli)
)

type zzV301Ahead struct {
	seq       uint64
	committed int64
	offsets   []int64
}

func zzV301DecodeSlot(slot []byte) (zzV301Ahead, bool) {
	if len(slot) < zzV301HdrLen || [4]byte(slot[0:4]) != zzV301Magic || slot[4] != 1 {
		return zzV301Ahead{}, false
	}
	count := binary.LittleEndian.Uint32(slot[24:28])
	plen := binary.LittleEndian.Uint32(slot[28:32])
	if int(plen) > len(slot)-zzV301HdrLen || count > plen {
		return zzV301Ahead{}, false
	}
	payload := slot[zzV301HdrLen : zzV301HdrLen+int(plen)]
	crc := crc32.Checksum(slot[:zzV301CRCOff], zzV301CRC)
	crc = crc32.Update(crc, zzV301CRC, payload)
	if crc != binary.LittleEndian.Uint32(slot[zzV301CRCOff:zzV301HdrLen]) {
		return zzV301Ahead{}, false
	}
	rec := zzV301Ahead{
		seq:       binary.LittleEndian.Uint64(slot[8:16]),
		committed: int64(binary.LittleEndian.Uint64(slot[16:24])),
		offsets:   make([]int64, 0, count),
	}
	if rec.committed < -1 {
		return zzV301Ahead{}, false
	}
	prev := rec.committed
	for i := uint32(0); i < count; i++ {
		delta, n := binary.Uvarint(payload)
		if n <= 0 || delta == 0 || delta > uint64(math.MaxInt64-prev) {
			return zzV301Ahead{}, false
		}
		payload = payload[n:]
		prev += int64(delta)
		rec.offsets = append(rec.offsets, prev)
	}
	return rec, true
}

func zzV301ReadAhead(im zzWP23Image) (zzV301Ahead, bool) {
	if !im.present {
		return zzV301Ahead{}, false
	}
	buf := im.data
	var best zzV301Ahead
	found := false
	for start := 0; start+zzV301HdrLen <= len(buf) && start < 2*zzV301SlotSize; start += zzV301SlotSize {
		end := min(start+zzV301SlotSize, len(buf))
		rec, ok := zzV301DecodeSlot(buf[start:end])
		if ok && (!found || rec.seq > best.seq) {
			best, found = rec, true
		}
	}
	return best, found
}

func zzV301ReadOffset(im zzWP23Image) (int64, bool) {
	if !im.present || len(im.data) != 8 {
		// Missing and empty read as none; any other length is an error,
		// which the serve wiring recovers as none.
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(im.data)), true
}

// zzWP23RecoverV301 recovers one partition from its two files' images
// exactly as v3.0.1 does.
func zzWP23RecoverV301(ahead, offset zzWP23Image) zzWP23Recovered {
	committed := int64(-1)
	if off, ok := zzV301ReadOffset(offset); ok {
		committed = off
	}
	set := map[int64]bool{}
	if rec, ok := zzV301ReadAhead(ahead); ok {
		committed = max(committed, rec.committed)
		for _, off := range rec.offsets {
			if off > committed {
				set[off] = true
			}
		}
	}
	for set[committed+1] {
		delete(set, committed+1)
		committed++
	}
	out := zzWP23Recovered{frontier: committed}
	for off := range set {
		out.ahead = append(out.ahead, off)
	}
	slices.Sort(out.ahead)
	return out
}
