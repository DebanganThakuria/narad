package storage

import (
	"errors"
	"io/fs"
	"os"
	"testing"
)

// Open reads no sealed segment (it opens and stats each, then lets the
// descriptor go), so an I/O error on one surfaces on the first read that
// needs it rather than at NewLog. That read fails with the I/O error,
// not as corruption or a missing offset, which the consume path would
// skip as recorded loss. The error is not remembered: once the file
// reads again, so do its records. Records in the other segments stay
// readable throughout. Revoking access after the open stands in for
// EIO, which a test cannot inject into a read.
func TestZZShipSealedSegmentIOErrorSurfacesOnRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	const per, recSize = 4, 64
	dir := t.TempDir()
	lay := wp15WriteLayout(t, dir, 2, 3, 2, per, recSize)
	l := wp15Open(t, dir)

	sealed := lay.paths[1]
	if err := os.Chmod(sealed, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, dataFileMode) })

	off := lay.bases[1] + 1
	for attempt := range 2 {
		_, err := l.Read(off)
		if err == nil {
			t.Fatalf("attempt %d: Read(%d) served a record from a segment it cannot read", attempt, off)
		}
		if IsCorrupt(err) || errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("attempt %d: Read(%d) = %v, want the I/O error, not corruption or not found", attempt, off, err)
		}
		if !errors.Is(err, fs.ErrPermission) {
			t.Fatalf("attempt %d: Read(%d) = %v, want the file's open error", attempt, off, err)
		}
	}
	wp15ExpectRecord(t, l, lay.bases[0]+1, recSize)
	wp15ExpectRecord(t, l, lay.bases[2]+1, recSize)

	if err := os.Chmod(sealed, dataFileMode); err != nil {
		t.Fatal(err)
	}
	wp15ExpectRecord(t, l, off, recSize)
}
