package storage

// Fan-out cursor persistence. A cursor tracks how far one child topic
// has been fanned out from one parent partition; it lives in the PARENT
// partition's directory (the cursor runs on the parent partition's
// owner, next to the data it tails) as fanout-<child>.offset. The file
// carries the link's attach epoch so a cursor from an earlier
// attachment is never resumed after a detach/re-attach — re-attach
// starts fresh at the parent's tail, matching the no-backfill contract.
//
// A cursor advances once per committed slab, so the file is one
// fixed-size record rewritten in place (one write and one data sync,
// like consumer.offset) instead of a temp file, a data sync and a
// rename per slab. The record stays JSON, so every binary parses it (an
// older one ignores the crc field and the padding):
//
//	{"crc":"<8 hex>","epoch":"<epoch>","next_offset":<n>}<spaces>\n
//
// padded to fanoutCursorRecordSize bytes. The crc is CRC-32C over every
// byte after the crc field, padding included. It is what makes the
// overwrite safe: a reader that catches the writer mid-copy (a move
// listing sidecars, the cursor stats handler), or a crash that tore the
// sector, would otherwise parse a splice of two offsets (98332 and 99100
// reading as 99132) as a valid cursor ahead of the true position, a
// silent skip for whoever resumes from it. Readers re-read a record that
// fails the check. The crc comes first so any splice of two records
// still carries one. The record fits in one sector, the single-sector
// overwrite argument the watermark and consumer offset files already
// rely on. A record that cannot be padded (an epoch far longer than the
// 16 hex characters an attach mints) is written unpadded and always
// replaced atomically, as is any file in another format.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

const (
	fanoutCursorRecordSize = 256
	fanoutCursorCRCPrefix  = `{"crc":"`
	// fanoutCursorCRCEnd is the offset of the crc value's closing
	// quote; the checksummed body starts after that quote and a comma.
	fanoutCursorCRCEnd  = len(fanoutCursorCRCPrefix) + 8
	fanoutCursorBodyOff = fanoutCursorCRCEnd + 2

	// A record that fails validation is re-read this many times, this
	// far apart, before it is reported corrupt: an in-place write is
	// copied in well under a millisecond.
	fanoutCursorReadAttempts = 5
	fanoutCursorReadRetry    = time.Millisecond
)

var (
	fanoutCursorCRC     = crc32.MakeTable(crc32.Castagnoli)
	fanoutCursorPadding = bytes.Repeat([]byte{' '}, fanoutCursorRecordSize)
)

// FanoutCursor is the persisted fan-out position of one
// (child, parentPartition) pair.
type FanoutCursor struct {
	// Epoch is the attach epoch of the parent→child link this cursor
	// belongs to (topic.Topic.AttachEpoch).
	Epoch string `json:"epoch"`
	// NextOffset is the next parent-log offset to fan out. It advances
	// only after the records below it are durably committed to the
	// child (commit-before-advance).
	NextOffset int64 `json:"next_offset"`
}

func fanoutCursorFileName(child string) string {
	return "fanout-" + child + ".offset"
}

// encodeFanoutCursor renders the checksummed cursor record: exactly
// fanoutCursorRecordSize bytes when it fits, longer otherwise.
func encodeFanoutCursor(c FanoutCursor) ([]byte, error) {
	epoch, err := json.Marshal(c.Epoch)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, fanoutCursorRecordSize)
	buf = append(buf, fanoutCursorCRCPrefix...)
	buf = append(buf, `00000000","epoch":`...)
	buf = append(buf, epoch...)
	buf = append(buf, `,"next_offset":`...)
	buf = strconv.AppendInt(buf, c.NextOffset, 10)
	buf = append(buf, '}')
	if pad := fanoutCursorRecordSize - 1 - len(buf); pad > 0 {
		buf = append(buf, fanoutCursorPadding[:pad]...)
	}
	buf = append(buf, '\n')
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(buf[fanoutCursorBodyOff:], fanoutCursorCRC))
	hex.Encode(buf[len(fanoutCursorCRCPrefix):fanoutCursorCRCEnd], sum[:])
	return buf, nil
}

// decodeFanoutCursor parses one cursor record. A checksummed record
// must match its crc; one without is the plain JSON older binaries
// wrote, always through a rename and so never torn.
func decodeFanoutCursor(buf []byte) (FanoutCursor, error) {
	if bytes.HasPrefix(buf, []byte(fanoutCursorCRCPrefix)) {
		if len(buf) < fanoutCursorBodyOff || buf[fanoutCursorCRCEnd] != '"' || buf[fanoutCursorCRCEnd+1] != ',' {
			return FanoutCursor{}, errors.New("malformed checksum field")
		}
		var sum [4]byte
		if _, err := hex.Decode(sum[:], buf[len(fanoutCursorCRCPrefix):fanoutCursorCRCEnd]); err != nil {
			return FanoutCursor{}, fmt.Errorf("malformed checksum field: %w", err)
		}
		if binary.BigEndian.Uint32(sum[:]) != crc32.Checksum(buf[fanoutCursorBodyOff:], fanoutCursorCRC) {
			return FanoutCursor{}, errors.New("checksum mismatch (torn record)")
		}
	}
	var c FanoutCursor
	if err := json.Unmarshal(buf, &c); err != nil {
		return FanoutCursor{}, err
	}
	return c, nil
}

// readFanoutCursorFile reads and validates the cursor file name in dir,
// returning the record's verbatim bytes as well. A record that fails
// validation is re-read a few times before it is reported corrupt: an
// in-place advance can be caught half copied, and the next read sees it
// whole. ok=false (with nil error) when the file does not exist.
func readFanoutCursorFile(dir, name string) (FanoutCursor, []byte, bool, error) {
	path := filepath.Join(dir, name)
	var err error
	for attempt := range fanoutCursorReadAttempts {
		if attempt > 0 {
			time.Sleep(fanoutCursorReadRetry)
		}
		var buf []byte
		buf, err = os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return FanoutCursor{}, nil, false, nil
		}
		if err != nil {
			return FanoutCursor{}, nil, false, err
		}
		var c FanoutCursor
		if c, err = decodeFanoutCursor(buf); err == nil {
			return c, buf, true, nil
		}
	}
	return FanoutCursor{}, nil, false, fmt.Errorf("storage: fan-out cursor %s corrupt: %w", name, err)
}

// ReadFanoutCursor loads the persisted cursor for child from a parent
// partition directory. ok=false (with nil error) when none exists.
func ReadFanoutCursor(partitionDir, child string) (FanoutCursor, bool, error) {
	c, _, ok, err := readFanoutCursorFile(partitionDir, fanoutCursorFileName(child))
	return c, ok, err
}

// WriteFanoutCursorIfPartitionDirExists atomically replaces the cursor
// (temp file, data sync, rename, directory sync), failing with
// ErrPartitionDirMissing instead of resurrecting a parent partition
// directory that a concurrent topic delete removed. For a cursor's
// first anchor; the per-slab advance is AdvanceFanoutCursor.
func WriteFanoutCursorIfPartitionDirExists(partitionDir, child string, c FanoutCursor) error {
	info, err := os.Stat(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("storage: fan-out cursor partition path is not a directory: %s", partitionDir)
	}
	rec, err := encodeFanoutCursor(c)
	if err != nil {
		return err
	}
	return writeFanoutCursorAtomic(partitionDir, fanoutCursorFileName(child), rec)
}

// AdvanceFanoutCursor durably records the cursor position after a
// committed slab by overwriting the fixed-size record in place. A
// missing file, or one in another format (an older binary's, or a
// record too long to pad), is replaced atomically instead. Like
// WriteFanoutCursorIfPartitionDirExists it fails with
// ErrPartitionDirMissing rather than recreate a deleted partition
// directory.
func AdvanceFanoutCursor(partitionDir, child string, c FanoutCursor) error {
	rec, err := encodeFanoutCursor(c)
	if err != nil {
		return err
	}
	name := fanoutCursorFileName(child)
	if len(rec) == fanoutCursorRecordSize {
		done, err := overwriteFanoutCursor(filepath.Join(partitionDir, name), rec)
		if done || err != nil {
			return err
		}
	}
	return writeFanoutCursorAtomic(partitionDir, name, rec)
}

// overwriteFanoutCursor writes rec over an existing fixed-size record at
// path: one write at offset 0 and one data sync, no truncate, so the
// file never shrinks or empties. done=false (nil error) when there is no
// such record to overwrite: the file is missing or has another length.
func overwriteFanoutCursor(path string, rec []byte) (bool, error) {
	f, err := syncfile.OpenFile(path, os.O_WRONLY, dataFileMode)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return false, err
	}
	if info.Size() != fanoutCursorRecordSize {
		return false, f.Close()
	}
	if _, err := syncfile.WriteAt(f, rec, 0); err != nil {
		_ = f.Close()
		return false, err
	}
	if err := syncfile.SyncData(f); err != nil {
		_ = f.Close()
		return false, err
	}
	return true, f.Close()
}

// writeFanoutCursorAtomic replaces dir/name with rec through a temp file
// and a rename, then syncs the directory so a newly created cursor
// survives a crash (a lost cursor re-anchors).
func writeFanoutCursorAtomic(dir, name string, rec []byte) error {
	if err := writeFileAtomic(dir, name, rec); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrPartitionDirMissing
		}
		return err
	}
	return nil
}

// WriteFanoutCursorCreating persists the cursor, creating the partition
// directory if it does not exist yet. For the first anchor of a link the
// owner just confirmed with the leader: a parent partition that has
// never been produced to has no directory, and refusing to create one
// would leave the cursor unable to anchor (it stops, the reconciler
// respawns it, and the child never catches up). Every other persist
// keeps using WriteFanoutCursorIfPartitionDirExists so a concurrent
// topic delete is never resurrected.
func WriteFanoutCursorCreating(partitionDir, child string, c FanoutCursor) error {
	if err := os.MkdirAll(partitionDir, dataDirMode); err != nil {
		return err
	}
	return WriteFanoutCursorIfPartitionDirExists(partitionDir, child, c)
}

// RemoveFanoutCursor deletes the persisted cursor for child. Called
// when the parent→child link is gone (detach or delete) so a later
// re-attach cannot resume — and thereby replay — a dead cursor.
// Removing a cursor that does not exist is a no-op.
func RemoveFanoutCursor(partitionDir, child string) error {
	err := os.Remove(filepath.Join(partitionDir, fanoutCursorFileName(child)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ListFanoutCursorChildren returns the child topics that have a
// persisted cursor in the partition directory.
func ListFanoutCursorChildren(partitionDir string) ([]string, error) {
	entries, err := os.ReadDir(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var children []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "fanout-") || !strings.HasSuffix(name, ".offset") {
			continue
		}
		children = append(children, strings.TrimSuffix(strings.TrimPrefix(name, "fanout-"), ".offset"))
	}
	return children, nil
}

// SidecarFile is one small per-partition state file carried verbatim
// alongside the segments when a partition moves: the fan-out cursor
// files. Name is the file's base name inside the partition directory.
type SidecarFile struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

// IsFanoutCursorFileName reports whether name is a fan-out cursor file's
// base name (fanout-<child>.offset) with no path component, which is the
// only kind of sidecar a move installs into a partition directory.
func IsFanoutCursorFileName(name string) bool {
	if name == "" || name != filepath.Base(name) || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	if !strings.HasPrefix(name, "fanout-") || !strings.HasSuffix(name, ".offset") {
		return false
	}
	return len(name) > len("fanout-")+len(".offset")
}

// ListFanoutCursorFiles reads every fan-out cursor file in the partition
// directory verbatim, so a move can carry them to the new owner. Each
// record is validated first (re-read if caught mid-advance); one that
// stays corrupt fails the listing. A missing directory yields no files.
func ListFanoutCursorFiles(partitionDir string) ([]SidecarFile, error) {
	entries, err := os.ReadDir(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []SidecarFile
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !IsFanoutCursorFileName(name) {
			continue
		}
		// Validated, not just read: a record caught mid-advance must
		// never ship, since an older destination installs whatever
		// parses as JSON.
		_, data, ok, err := readFanoutCursorFile(partitionDir, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // removed under us: its link dissolved
		}
		files = append(files, SidecarFile{Name: name, Data: data})
	}
	return files, nil
}

// InstallFanoutCursorFile atomically writes one transferred cursor file
// into the partition directory. It refuses any name that is not a plain
// fan-out cursor file name, so a transfer can never plant an arbitrary
// path, and validates the payload parses as a cursor (checksum included
// when the record carries one).
func InstallFanoutCursorFile(partitionDir string, f SidecarFile) error {
	if !IsFanoutCursorFileName(f.Name) {
		return fmt.Errorf("storage: refusing to install sidecar %q: not a fan-out cursor file name", f.Name)
	}
	if _, err := decodeFanoutCursor(f.Data); err != nil {
		return fmt.Errorf("storage: refusing to install sidecar %q: corrupt cursor: %w", f.Name, err)
	}
	return writeFileAtomic(partitionDir, f.Name, f.Data)
}

// FanoutCursorFileAtMost returns f with its cursor's next offset lowered
// to limit when it points past it, re-encoded; changed reports whether
// it was. A move installs the source's cursor files into a copy promoted
// at a high watermark: a cursor past that boundary would skip the parent
// records the new owner later writes below it, and the child would never
// get them. Lowered, it fans some records out again at most. A cursor at
// or below limit is returned verbatim.
func FanoutCursorFileAtMost(f SidecarFile, limit int64) (SidecarFile, bool, error) {
	c, err := decodeFanoutCursor(f.Data)
	if err != nil {
		return f, false, fmt.Errorf("storage: sidecar %q: corrupt cursor: %w", f.Name, err)
	}
	if c.NextOffset <= limit {
		return f, false, nil
	}
	c.NextOffset = limit
	data, err := encodeFanoutCursor(c)
	if err != nil {
		return f, false, fmt.Errorf("storage: sidecar %q: %w", f.Name, err)
	}
	return SidecarFile{Name: f.Name, Data: data}, true, nil
}
