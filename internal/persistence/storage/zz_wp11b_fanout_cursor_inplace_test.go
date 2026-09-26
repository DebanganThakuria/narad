package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// Old binaries decode the cursor with a plain json.Unmarshal into
// {epoch, next_offset}: the new record must stay readable that way, so
// a cursor file moved to (or left for) an older binary still resumes.
func TestZZWP11BFanoutCursorRecordReadableByPlainJSON(t *testing.T) {
	dir := t.TempDir()
	want := FanoutCursor{Epoch: "0123456789abcdef", NextOffset: 1 << 40}
	if err := AdvanceFanoutCursor(dir, "child", want); err != nil {
		t.Fatal(err)
	}
	want.NextOffset++
	if err := AdvanceFanoutCursor(dir, "child", want); err != nil {
		t.Fatal(err)
	}
	buf, err := os.ReadFile(filepath.Join(dir, fanoutCursorFileName("child")))
	if err != nil {
		t.Fatal(err)
	}
	if len(buf) != fanoutCursorRecordSize {
		t.Fatalf("cursor record is %d bytes, want the fixed %d", len(buf), fanoutCursorRecordSize)
	}
	var legacy struct {
		Epoch      string `json:"epoch"`
		NextOffset int64  `json:"next_offset"`
	}
	if err := json.Unmarshal(buf, &legacy); err != nil {
		t.Fatalf("plain json.Unmarshal of %q: %v", buf, err)
	}
	if legacy.Epoch != want.Epoch || legacy.NextOffset != want.NextOffset {
		t.Fatalf("plain decode = %+v, want %+v", legacy, want)
	}
	files, err := ListFanoutCursorFiles(dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("ListFanoutCursorFiles = (%v, %v), want one file", files, err)
	}
	if err := InstallFanoutCursorFile(t.TempDir(), files[0]); err != nil {
		t.Fatalf("InstallFanoutCursorFile: %v", err)
	}
}

// A cursor file written by an older binary (variable-length JSON, no
// checksum) still reads, and the first advance migrates it to the
// fixed record through an atomic replace; later advances go in place.
func TestZZWP11BFanoutCursorLegacyFileMigrates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, fanoutCursorFileName("child"))
	if err := os.WriteFile(path, []byte(`{"epoch":"e1","next_offset":7}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadFanoutCursor(dir, "child")
	if err != nil || !ok || got != (FanoutCursor{Epoch: "e1", NextOffset: 7}) {
		t.Fatalf("legacy read = (%+v, %v, %v)", got, ok, err)
	}
	if err := AdvanceFanoutCursor(dir, "child", FanoutCursor{Epoch: "e1", NextOffset: 9}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != fanoutCursorRecordSize {
		t.Fatalf("advanced legacy file is %d bytes, want the fixed %d", info.Size(), fanoutCursorRecordSize)
	}
	got, ok, err = ReadFanoutCursor(dir, "child")
	if err != nil || !ok || got != (FanoutCursor{Epoch: "e1", NextOffset: 9}) {
		t.Fatalf("read after migration = (%+v, %v, %v)", got, ok, err)
	}
	// An epoch too long for the fixed record still round-trips through
	// the atomic path.
	long := FanoutCursor{Epoch: string(bytes.Repeat([]byte{'e'}, 2*fanoutCursorRecordSize)), NextOffset: 11}
	if err := AdvanceFanoutCursor(dir, "child", long); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadFanoutCursor(dir, "child"); err != nil || !ok || got != long {
		t.Fatalf("long-epoch read = (%v, %v)", ok, err)
	}
	if err := AdvanceFanoutCursor(dir, "child", FanoutCursor{Epoch: "e2", NextOffset: 12}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadFanoutCursor(dir, "child"); err != nil || !ok || got != (FanoutCursor{Epoch: "e2", NextOffset: 12}) {
		t.Fatalf("read after shrinking back = (%+v, %v, %v)", got, ok, err)
	}
}

// AdvanceFanoutCursor never resurrects a deleted partition directory.
func TestZZWP11BFanoutCursorAdvanceRefusesMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	if err := AdvanceFanoutCursor(missing, "child", FanoutCursor{Epoch: "e"}); !errors.Is(err, ErrPartitionDirMissing) {
		t.Fatalf("advance into missing dir = %v, want %v", err, ErrPartitionDirMissing)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing dir was recreated: %v", err)
	}
}

// Readers running beside an in-place writer (the cursor stats handler,
// a partition move listing sidecars) only ever see cursors the writer
// actually wrote, never an error or a mix of two records.
func TestZZWP11BFanoutCursorConcurrentReadersSeeWholeRecords(t *testing.T) {
	dir := t.TempDir()
	const epoch = "0123456789abcdef"
	const step = 997
	if err := WriteFanoutCursorIfPartitionDirExists(dir, "child", FanoutCursor{Epoch: epoch, NextOffset: 0}); err != nil {
		t.Fatal(err)
	}
	// Every written offset is 0 or 1 mod step; a spliced one almost
	// never is, and the checksum rejects it regardless.
	var written atomic.Int64
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for next := int64(1); ; next += step {
			select {
			case <-stop:
				return
			default:
			}
			written.Store(next)
			if err := AdvanceFanoutCursor(dir, "child", FanoutCursor{Epoch: epoch, NextOffset: next}); err != nil {
				t.Errorf("advance: %v", err)
				return
			}
		}
	}()
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 300 {
				got, ok, err := ReadFanoutCursor(dir, "child")
				if err != nil || !ok {
					t.Errorf("concurrent read: ok=%v err=%v", ok, err)
					return
				}
				if got.Epoch != epoch || (got.NextOffset != 0 && got.NextOffset%step != 1) || got.NextOffset > written.Load() {
					t.Errorf("concurrent read returned %+v, not a written cursor", got)
					return
				}
				files, err := ListFanoutCursorFiles(dir)
				if err != nil || len(files) != 1 {
					t.Errorf("concurrent listing = (%d files, %v)", len(files), err)
					return
				}
			}
		})
	}
	readers.Wait()
	close(stop)
	<-writerDone
}
