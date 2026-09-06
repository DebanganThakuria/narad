package metastore

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestRaftLogWriterForwardsLevelsAndBuffersFragments(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := NewRaftLogWriter(log)

	// Two lines, delivered in three fragments, with a debug line that
	// must land at debug and an error line that must land at error.
	fragments := []string{
		"2026-09-06T10:00:00.000+0530 [INFO]  raft: Installed remote snapshot\n2026-09-06T10:00:01.000+0530 [ERROR] raft-net: failed to accept",
		" connection: error=\"tls: bad certificate\"\n",
		"2026-09-06T10:00:02.000+0530 [DEBUG] raft: votes: needed=2\n",
	}
	for _, f := range fragments {
		if n, err := w.Write([]byte(f)); err != nil || n != len(f) {
			t.Fatalf("Write(%q) = %d, %v", f, n, err)
		}
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d records, want 3:\n%s", len(lines), out.String())
	}
	want := []struct{ level, msg string }{
		{"INFO", `msg="raft: Installed remote snapshot"`},
		{"ERROR", `msg="raft-net: failed to accept connection: error=\"tls: bad certificate\""`},
		{"DEBUG", `msg="raft: votes: needed=2"`},
	}
	for i, w := range want {
		if !strings.Contains(lines[i], "level="+w.level) || !strings.Contains(lines[i], w.msg) || !strings.Contains(lines[i], "component=raft") {
			t.Fatalf("record %d = %q, want level %s and %s", i, lines[i], w.level, w.msg)
		}
	}
}

func TestParseHCLogLineWithoutLevel(t *testing.T) {
	level, msg := parseHCLogLine("no level here")
	if level != slog.LevelInfo || msg != "no level here" {
		t.Fatalf("parseHCLogLine = %v, %q", level, msg)
	}
}
