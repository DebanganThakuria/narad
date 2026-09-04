package wal

import (
	"context"
	"strings"
	"testing"
)

// AppendWith writes the payload straight into the group-commit buffer:
// the record must replay byte-identically to a plain Append, and a fill
// that produces the wrong number of bytes must be rejected without
// corrupting the buffer or consuming a seq.
func TestAppendWithMatchesAppend(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, Options{SyncInterval: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer log.Close()

	ctx := context.Background()
	if _, err := log.Append(ctx, []byte("plain")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	payload := strings.Repeat("in-place", 8)
	id, err := log.AppendWith(ctx, len(payload), func(dst []byte) []byte { return append(dst, payload...) })
	if err != nil {
		t.Fatalf("AppendWith: %v", err)
	}
	if id.Seq != 1 {
		t.Fatalf("seq = %d, want 1", id.Seq)
	}

	// A lying fill is rejected and leaves the log usable.
	if _, err := log.AppendWith(ctx, 10, func(dst []byte) []byte { return append(dst, "short"...) }); err == nil || !strings.Contains(err.Error(), "fill wrote") {
		t.Fatalf("short fill error = %v, want fill size error", err)
	}
	if _, err := log.AppendWith(ctx, 0, nil); err == nil {
		t.Fatal("empty AppendWith must fail")
	}
	if _, err := log.Append(ctx, []byte("after")); err != nil {
		t.Fatalf("Append after rejected fill: %v", err)
	}

	var got []string
	if err := Replay(dir, 0, 0, func(r Record) error { got = append(got, string(r.Payload)); return nil }); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	want := []string{"plain", payload, "after"}
	if len(got) != len(want) {
		t.Fatalf("replayed %d records %q, want %q", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}
