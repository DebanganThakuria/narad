package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// zzShipCorruptCursor is a checksummed cursor record for c with one body
// byte changed, so it still parses as JSON but fails its checksum.
func zzShipCorruptCursor(t *testing.T, c FanoutCursor) []byte {
	t.Helper()
	buf, err := encodeFanoutCursor(c)
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(string(buf), `"next_offset":`) + len(`"next_offset":`)
	buf[i] = '9'
	if _, err := decodeFanoutCursor(buf); err == nil {
		t.Fatalf("the altered record %q still decodes", buf)
	}
	return buf
}

// The move listing validates every cursor it ships. A record that stays
// corrupt past the re-reads fails the whole listing, naming the file,
// even when a healthy sibling sits next to it: it used to ship as-is,
// and an older destination installs whatever parses as JSON.
func TestZZShipListFanoutCursorFilesFailsOnACorruptRecord(t *testing.T) {
	dir := t.TempDir()
	if err := WriteFanoutCursorCreating(dir, "good", FanoutCursor{Epoch: "e", NextOffset: 5}); err != nil {
		t.Fatal(err)
	}
	files, err := ListFanoutCursorFiles(dir)
	if err != nil || len(files) != 1 || files[0].Name != fanoutCursorFileName("good") {
		t.Fatalf("healthy listing = (%v, %v), want the one good cursor", files, err)
	}

	bad := fanoutCursorFileName("bad")
	if err := os.WriteFile(filepath.Join(dir, bad), zzShipCorruptCursor(t, FanoutCursor{Epoch: "e", NextOffset: 12}), 0o600); err != nil {
		t.Fatal(err)
	}
	files, err = ListFanoutCursorFiles(dir)
	if err == nil {
		t.Fatalf("a corrupt cursor listed as %d file(s) with no error", len(files))
	}
	if !strings.Contains(err.Error(), bad) {
		t.Fatalf("listing error %q does not name the corrupt file %s", err, bad)
	}
}

// Install refuses a record whose checksum does not match, and writes
// nothing; a legacy record with no checksum field (plain JSON, as older
// binaries wrote) still installs.
func TestZZShipInstallFanoutCursorFileRefusesABadChecksum(t *testing.T) {
	dir := t.TempDir()
	name := fanoutCursorFileName("child")
	err := InstallFanoutCursorFile(dir, SidecarFile{Name: name, Data: zzShipCorruptCursor(t, FanoutCursor{Epoch: "e", NextOffset: 3})})
	if err == nil {
		t.Fatal("installed a cursor whose checksum does not match")
	}
	if !strings.Contains(err.Error(), "corrupt cursor") {
		t.Fatalf("install error = %v, want a corrupt cursor refusal", err)
	}
	if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused install left the file behind (stat err %v)", err)
	}

	legacy := SidecarFile{Name: name, Data: []byte(`{"epoch":"e","next_offset":3}`)}
	if err := InstallFanoutCursorFile(dir, legacy); err != nil {
		t.Fatalf("legacy cursor install: %v", err)
	}
	got, ok, err := ReadFanoutCursor(dir, "child")
	if err != nil || !ok || got != (FanoutCursor{Epoch: "e", NextOffset: 3}) {
		t.Fatalf("installed legacy cursor reads (%+v, %v, %v)", got, ok, err)
	}
}
