package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// consumer.ahead persists the offsets acked out of order above the
// committed frontier, so a restart does not redeliver messages that
// were already acked. It sits next to consumer.offset, which stays the
// 8-byte frontier file: an older binary ignores this one.
//
// The file holds two fixed-size slots written alternately in place:
// one WriteAt and one data sync per write, no rename and no directory
// mutation, the same cost the frontier write was cut down to. A torn
// write can only damage the slot being written; the reader validates
// both and takes the one with the highest sequence. A record that does
// not fit keeps the lowest offsets and drops the rest: a dropped entry
// costs one duplicate on recovery, never a wrong frontier, because
// every offset in the file was acked and the reader ignores anything at
// or below the recovered frontier.
//
// Slot layout (little endian):
//
//	magic "NAAH" | version u8 | pad [3] | seq u64 | committed i64 |
//	count u32 | payload length u32 | crc32c u32 | payload
//
// The payload is count uvarints, each the delta from the previous
// offset (the first from committed), so a dense set costs a byte per
// entry. The CRC covers the header before the CRC field and the payload.
const (
	consumerAheadFileName = "consumer.ahead"
	consumerAheadSlotSize = 4096
	consumerAheadVersion  = 1
	consumerAheadHdrLen   = 4 + 1 + 3 + 8 + 8 + 4 + 4 + 4
	consumerAheadCRCOff   = consumerAheadHdrLen - 4
	consumerAheadPayload  = consumerAheadSlotSize - consumerAheadHdrLen
)

var (
	consumerAheadMagic = [4]byte{'N', 'A', 'A', 'H'}
	consumerAheadCRC   = crc32.MakeTable(crc32.Castagnoli)
)

// ConsumerAhead is one recovered consumer.ahead record.
type ConsumerAhead struct {
	// Seq orders records across writes; the reader keeps the highest.
	Seq uint64
	// Committed is the frontier the shard had reached when the record
	// was written. Every offset in Offsets is above it.
	Committed int64
	// Offsets are the acked-ahead offsets, ascending, all > Committed.
	Offsets []int64
	// Slot is the slot the record was read from (0 or 1), so a writer
	// resuming after a restart alternates away from the newest record.
	Slot int
}

// EncodeConsumerAhead builds one slot for the given record. Offsets are
// sorted and deduplicated; entries at or below committed are dropped;
// when the payload would overflow the slot the lowest offsets are kept.
// The returned slice is exactly consumerAheadSlotSize bytes.
func EncodeConsumerAhead(seq uint64, committed int64, offsets []int64) []byte {
	sorted := slices.Clone(offsets)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)

	slot := make([]byte, consumerAheadSlotSize)
	payload := slot[consumerAheadHdrLen:consumerAheadHdrLen]
	var tmp [binary.MaxVarintLen64]byte
	prev := committed
	count := uint32(0)
	for _, off := range sorted {
		if off <= committed {
			continue
		}
		n := binary.PutUvarint(tmp[:], uint64(off-prev))
		if len(payload)+n > consumerAheadPayload {
			break
		}
		payload = append(payload, tmp[:n]...)
		prev = off
		count++
	}

	copy(slot[0:4], consumerAheadMagic[:])
	slot[4] = consumerAheadVersion
	binary.LittleEndian.PutUint64(slot[8:16], seq)
	binary.LittleEndian.PutUint64(slot[16:24], uint64(committed))
	binary.LittleEndian.PutUint32(slot[24:28], count)
	binary.LittleEndian.PutUint32(slot[28:32], uint32(len(payload)))
	crc := crc32.Checksum(slot[:consumerAheadCRCOff], consumerAheadCRC)
	crc = crc32.Update(crc, consumerAheadCRC, payload)
	binary.LittleEndian.PutUint32(slot[consumerAheadCRCOff:consumerAheadHdrLen], crc)
	return slot
}

// decodeConsumerAheadSlot validates one slot. ok=false for anything
// that is not a complete, checksummed record of a known version.
func decodeConsumerAheadSlot(slot []byte) (ConsumerAhead, bool) {
	if len(slot) < consumerAheadHdrLen || [4]byte(slot[0:4]) != consumerAheadMagic || slot[4] != consumerAheadVersion {
		return ConsumerAhead{}, false
	}
	count := binary.LittleEndian.Uint32(slot[24:28])
	plen := binary.LittleEndian.Uint32(slot[28:32])
	if int(plen) > len(slot)-consumerAheadHdrLen || count > plen {
		return ConsumerAhead{}, false
	}
	payload := slot[consumerAheadHdrLen : consumerAheadHdrLen+int(plen)]
	crc := crc32.Checksum(slot[:consumerAheadCRCOff], consumerAheadCRC)
	crc = crc32.Update(crc, consumerAheadCRC, payload)
	if crc != binary.LittleEndian.Uint32(slot[consumerAheadCRCOff:consumerAheadHdrLen]) {
		return ConsumerAhead{}, false
	}
	rec := ConsumerAhead{
		Seq:       binary.LittleEndian.Uint64(slot[8:16]),
		Committed: int64(binary.LittleEndian.Uint64(slot[16:24])),
		Offsets:   make([]int64, 0, count),
	}
	if rec.Committed < -1 {
		return ConsumerAhead{}, false
	}
	prev := rec.Committed
	for i := uint32(0); i < count; i++ {
		delta, n := binary.Uvarint(payload)
		if n <= 0 || delta == 0 || delta > uint64(math.MaxInt64-prev) {
			return ConsumerAhead{}, false
		}
		payload = payload[n:]
		prev += int64(delta)
		rec.Offsets = append(rec.Offsets, prev)
	}
	return rec, true
}

// ReadConsumerAhead returns the newest valid acked-ahead record in
// partitionDir. ok=false (with a nil error) when the file is missing,
// empty, or holds no valid slot: recovery then starts from the frontier
// alone, which is always correct.
func ReadConsumerAhead(partitionDir string) (ConsumerAhead, bool, error) {
	buf, err := os.ReadFile(filepath.Join(partitionDir, consumerAheadFileName))
	if errors.Is(err, os.ErrNotExist) {
		return ConsumerAhead{}, false, nil
	}
	if err != nil {
		return ConsumerAhead{}, false, err
	}
	var best ConsumerAhead
	found := false
	for start := 0; start+consumerAheadHdrLen <= len(buf) && start < 2*consumerAheadSlotSize; start += consumerAheadSlotSize {
		end := min(start+consumerAheadSlotSize, len(buf))
		rec, ok := decodeConsumerAheadSlot(buf[start:end])
		if ok && (!found || rec.Seq > best.Seq) {
			rec.Slot = start / consumerAheadSlotSize
			best, found = rec, true
		}
	}
	return best, found, nil
}

// WriteConsumerAhead durably persists one acked-ahead record into slot
// (slot % 2) of consumerAheadFileName under partitionDir. Callers
// alternate the slot on every write and give each write a higher seq
// than the last, so the previous record survives a torn write. A
// missing partition directory maps to ErrPartitionDirMissing so a
// commit racing a topic delete never resurrects the directory.
func WriteConsumerAhead(partitionDir string, slot int, seq uint64, committed int64, offsets []int64) error {
	info, err := os.Stat(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("consumer ahead partition path is not a directory: %s", partitionDir)
	}
	path := filepath.Join(partitionDir, consumerAheadFileName)
	created := false
	f, err := syncfile.OpenFile(path, os.O_WRONLY, dataFileMode)
	if errors.Is(err, os.ErrNotExist) {
		f, err = syncfile.OpenFile(path, os.O_WRONLY|os.O_CREATE, dataFileMode)
		created = true
	}
	if errors.Is(err, os.ErrNotExist) {
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	rec := EncodeConsumerAhead(seq, committed, offsets)
	if _, err := syncfile.WriteAt(f, rec, int64(slot%2)*consumerAheadSlotSize); err != nil {
		_ = f.Close()
		return err
	}
	if err := syncfile.SyncData(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if created {
		if err := syncDir(partitionDir); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrPartitionDirMissing
			}
			return err
		}
	}
	return nil
}

// consumerAheadFileSize is a test hook: the bytes both slots occupy.
func consumerAheadFileSize(partitionDir string) (int64, error) {
	f, err := os.Open(filepath.Join(partitionDir, consumerAheadFileName))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Seek(0, io.SeekEnd)
}
