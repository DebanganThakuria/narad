package storage

import (
	"errors"
	"testing"
)

// A closed log answers every read with ErrLogClosed, never with
// ErrOffsetNotFound or a corruption error: a consume still holding a
// closed log of a retired incarnation may read an offset it reserved on
// the successor's shard, and "not found" there skips the successor's
// frontier past records it never delivered.
func TestZZWP23ClosedLogReadsAreClosed(t *testing.T) {
	l, err := NewLog(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := l.Append([]byte("r")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{-1, 0, 4, 5, 100} {
		if _, err := l.ReadShared(off); !errors.Is(err, ErrLogClosed) {
			t.Errorf("ReadShared(%d) on a closed log: %v, want ErrLogClosed", off, err)
		}
		if _, _, _, err := l.ReadKeyedShared(off); !errors.Is(err, ErrLogClosed) {
			t.Errorf("ReadKeyedShared(%d) on a closed log: %v, want ErrLogClosed", off, err)
		}
	}
}
