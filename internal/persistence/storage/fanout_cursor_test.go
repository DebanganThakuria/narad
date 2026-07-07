package storage

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func TestFanoutCursorRoundTrip(t *testing.T) {
	dir := t.TempDir()

	if _, ok, err := ReadFanoutCursor(dir, "child-a"); err != nil || ok {
		t.Fatalf("ReadFanoutCursor(missing) = ok=%v err=%v, want absent", ok, err)
	}

	want := FanoutCursor{Epoch: "abc123", NextOffset: 42}
	if err := WriteFanoutCursorIfPartitionDirExists(dir, "child-a", want); err != nil {
		t.Fatalf("WriteFanoutCursorIfPartitionDirExists: %v", err)
	}
	got, ok, err := ReadFanoutCursor(dir, "child-a")
	if err != nil || !ok || got != want {
		t.Fatalf("ReadFanoutCursor = (%+v, %v, %v), want (%+v, true, nil)", got, ok, err, want)
	}

	// Overwrite advances in place.
	want.NextOffset = 100
	if err := WriteFanoutCursorIfPartitionDirExists(dir, "child-a", want); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	got, _, _ = ReadFanoutCursor(dir, "child-a")
	if got != want {
		t.Fatalf("ReadFanoutCursor after overwrite = %+v, want %+v", got, want)
	}

	if err := RemoveFanoutCursor(dir, "child-a"); err != nil {
		t.Fatalf("RemoveFanoutCursor: %v", err)
	}
	if _, ok, _ := ReadFanoutCursor(dir, "child-a"); ok {
		t.Fatal("cursor still present after RemoveFanoutCursor")
	}
	if err := RemoveFanoutCursor(dir, "child-a"); err != nil {
		t.Fatalf("RemoveFanoutCursor(absent) = %v, want nil", err)
	}
}

func TestFanoutCursorWriteRefusesMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	err := WriteFanoutCursorIfPartitionDirExists(missing, "c", FanoutCursor{Epoch: "e"})
	if !errors.Is(err, ErrPartitionDirMissing) {
		t.Fatalf("write into missing dir error = %v, want %v", err, ErrPartitionDirMissing)
	}
}

func TestListFanoutCursorChildren(t *testing.T) {
	dir := t.TempDir()
	for _, child := range []string{"c-b", "c-a"} {
		if err := WriteFanoutCursorIfPartitionDirExists(dir, child, FanoutCursor{Epoch: "e"}); err != nil {
			t.Fatalf("write %s: %v", child, err)
		}
	}
	// Non-cursor files are ignored.
	if err := WriteConsumerOffset(dir, 7); err != nil {
		t.Fatalf("WriteConsumerOffset: %v", err)
	}

	children, err := ListFanoutCursorChildren(dir)
	if err != nil {
		t.Fatalf("ListFanoutCursorChildren: %v", err)
	}
	slices.Sort(children)
	if !slices.Equal(children, []string{"c-a", "c-b"}) {
		t.Fatalf("children = %v, want [c-a c-b]", children)
	}

	if children, err := ListFanoutCursorChildren(filepath.Join(dir, "missing")); err != nil || children != nil {
		t.Fatalf("missing dir = (%v, %v), want (nil, nil)", children, err)
	}
}
