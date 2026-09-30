package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// zzShipCopyDir copies every regular file of src into a fresh directory:
// the files as a crash at this instant would leave them, since every
// committed byte has been synced.
func zzShipCopyDir(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "crash.log")
	if err := os.MkdirAll(dst, dataDirMode); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, dataFileMode); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// The hwm file is empty while a log that exposed records is open, and
// stays empty on a crash image until the log is opened and closed again.
// The closed-partition readers must read that as "no boundary" (ok=false,
// a zero HighWatermark in StatPartitionDir), never as an error, and an
// open of the image must bootstrap the boundary from the record tail.
// Close then writes the exact boundary back.
func TestZZShipHighWatermarkFileEmptyWhileOpenAndOnACrashImage(t *testing.T) {
	dir := testLogPath(t)
	l, err := NewLog(dir, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := range n {
		appendCommit(t, l, fmt.Sprintf("rec-%d", i))
	}
	if hwm := l.HighWatermark(); hwm != n {
		t.Fatalf("open log HighWatermark = %d, want %d", hwm, n)
	}

	for _, d := range []string{dir, zzShipCopyDir(t, dir)} {
		if info, err := os.Stat(hwmFilePath(d)); err != nil || info.Size() != 0 {
			t.Fatalf("%s: hwm file (%v, err %v), want present and empty", d, info, err)
		}
		if hwm, ok, err := ReadPersistedHighWatermark(d); err != nil || ok || hwm != 0 {
			t.Fatalf("%s: ReadPersistedHighWatermark = (%d, %v, %v), want (0, false, nil)", d, hwm, ok, err)
		}
		st, err := StatPartitionDir(d)
		if err != nil {
			t.Fatalf("%s: StatPartitionDir: %v", d, err)
		}
		if st.Segments != 1 || st.HighWatermark != 0 {
			t.Fatalf("%s: StatPartitionDir = %+v, want one segment and no boundary", d, st)
		}
	}
	crash := zzShipCopyDir(t, dir)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if hwm, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || hwm != n {
		t.Fatalf("after Close: ReadPersistedHighWatermark = (%d, %v, %v), want (%d, true, nil)", hwm, ok, err, n)
	}

	reopened, err := NewLog(crash, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if hwm := reopened.HighWatermark(); hwm != n {
		t.Fatalf("crash image opened with HighWatermark %d, want the record tail %d", hwm, n)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if hwm, ok, err := ReadPersistedHighWatermark(crash); err != nil || !ok || hwm != n {
		t.Fatalf("crash image after open and Close: ReadPersistedHighWatermark = (%d, %v, %v), want (%d, true, nil)", hwm, ok, err, n)
	}
	st, err := StatPartitionDir(crash)
	if err != nil || st.HighWatermark != n {
		t.Fatalf("crash image after open and Close: StatPartitionDir = (%+v, %v), want HighWatermark %d", st, err, n)
	}
}
