package syncfile

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The seam is inert without a hook and, with one, fails or skips exactly
// the operation the hook names.
func TestFaultHookFailAndLie(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	f, err := OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// No hook: real syscalls.
	if n, err := Write(f, []byte("abc")); err != nil || n != 3 {
		t.Fatalf("Write without hook: n=%d err=%v", n, err)
	}
	if err := SyncData(f); err != nil {
		t.Fatalf("SyncData without hook: %v", err)
	}

	var seen []Op
	restore := SetFaultHook(func(op Op, p string) error {
		seen = append(seen, op)
		if p != path {
			t.Errorf("hook path = %q, want %q", p, path)
		}
		switch op {
		case OpWrite:
			return syscall.ENOSPC
		case OpSyncData:
			return ErrLie
		}
		return nil
	})

	// A failed write performs nothing.
	if n, err := Write(f, []byte("more")); !errors.Is(err, syscall.ENOSPC) || n != 0 {
		t.Fatalf("Write with failing hook: n=%d err=%v, want ENOSPC", n, err)
	}
	if n, err := WriteAt(f, []byte("more"), 0); !errors.Is(err, syscall.ENOSPC) || n != 0 {
		t.Fatalf("WriteAt with failing hook: n=%d err=%v, want ENOSPC", n, err)
	}
	// A lying sync reports success.
	if err := SyncData(f); err != nil {
		t.Fatalf("lying SyncData returned %v", err)
	}
	// An op the hook lets through runs for real.
	if err := Truncate(f, 1); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	restore()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "a" {
		t.Fatalf("file content = %q, want %q (failed writes must not land)", got, "a")
	}
	want := []Op{OpWrite, OpWrite, OpSyncData, OpTruncate}
	if len(seen) != len(want) {
		t.Fatalf("hook saw %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("hook saw %v, want %v", seen, want)
		}
	}

	// Restored: the hook is gone and syscalls are real again.
	if n, err := Write(f, []byte("z")); err != nil || n != 1 {
		t.Fatalf("Write after restore: n=%d err=%v", n, err)
	}
}

// A lying Write reports the buffer written and writes nothing; a lying
// OpenFile still opens (there is nothing to lie about); a failing Rename
// leaves both names alone.
func TestFaultHookLieWriteAndRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	restore := SetFaultHook(func(op Op, _ string) error {
		switch op {
		case OpWrite, OpOpen:
			return ErrLie
		case OpRename:
			return syscall.EIO
		}
		return nil
	})
	defer restore()

	f, err := OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile under a lying hook must still open: %v", err)
	}
	if n, err := Write(f, []byte("lost")); err != nil || n != 4 {
		t.Fatalf("lying Write: n=%d err=%v", n, err)
	}
	f.Close()
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatalf("lying write must land nothing: size=%d err=%v", info.Size(), err)
	}
	if err := Rename(path, filepath.Join(dir, "g")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Rename: %v, want EIO", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("failed rename must leave the source: %v", err)
	}
}
