package wal

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// A frame with a valid header but a payload truncated past end-of-file is a
// torn frame. Recovery treats it as the end of the log (as it always has), but
// must now log it so a truncation that drops committed records is not silent.
func TestScanLogsTruncatedFrameAndReplayStops(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id, err := log.Append(context.Background(), []byte("good"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Append a frame whose header declares more payload than is written.
	frame := appendFrame(nil, 99, []byte("truncated-payload"))
	truncated := frame[:len(frame)-3]
	f, err := os.OpenFile(segmentPath(dir, id.SegmentBase), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := f.Write(truncated); err != nil {
		t.Fatalf("write truncated frame error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close truncated writer error = %v", err)
	}

	var buf bytes.Buffer
	opts := testOptions()
	opts.Logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopen Close() error = %v", err)
	}

	if !strings.Contains(buf.String(), "truncated frame") {
		t.Fatalf("expected a truncated-frame warning, got logs: %q", buf.String())
	}

	var got []string
	if err := Replay(dir, 0, 0, func(r Record) error {
		got = append(got, string(r.Payload))
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(got) != 1 || got[0] != "good" {
		t.Fatalf("Replay() records = %v, want [good]", got)
	}
}
