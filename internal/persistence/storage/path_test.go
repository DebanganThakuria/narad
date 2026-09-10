package storage

import "testing"

func TestParsePartitionDirName(t *testing.T) {
	for name, want := range map[string]int{"p00000": 0, "p00007": 7, "p12345": 12345} {
		if got, ok := ParsePartitionDirName(name); !ok || got != want {
			t.Fatalf("parse(%q) = %d,%v want %d", name, got, ok, want)
		}
	}
	for _, bad := range []string{"", "p", "x00001", "p-1", "p1a", "hwm"} {
		if _, ok := ParsePartitionDirName(bad); ok {
			t.Fatalf("parse(%q) accepted", bad)
		}
	}
}
