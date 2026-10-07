package remote

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// The leader's audit line carries the fixed fields first, then every
// attribute the caller passed, in order; an empty class is left out.
func TestLeaderAuditWritesTheFixedFieldsThenEveryAttr(t *testing.T) {
	for _, n := range []int{0, 1, 40} {
		var buf bytes.Buffer
		attrs := make([]slog.Attr, n)
		for i := range attrs {
			attrs[i] = slog.Int(fmt.Sprintf("a%02d", i), i)
		}
		LeaderAudit(slog.New(slog.NewJSONHandler(&buf, nil)), AuditEvent{
			Event: "remote.create", Actor: "alice", RequestID: "0123456789abcdef", Target: "b",
			Outcome: OutcomeCommitted, Attrs: attrs,
		})
		line := strings.TrimSpace(buf.String())
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("%d attrs: line %q: %v", n, line, err)
		}
		if _, ok := m["class"]; ok {
			t.Fatalf("%d attrs: an empty class was written: %q", n, line)
		}
		keys := []string{`"component":"audit"`, `"event":"remote.create"`, `"actor":"alice"`, `"request_id":"0123456789abcdef"`, `"target":"b"`, `"outcome":"committed"`}
		for i := range n {
			keys = append(keys, fmt.Sprintf(`"a%02d":%d`, i, i))
		}
		at := 0
		for _, k := range keys {
			i := strings.Index(line[at:], k)
			if i < 0 {
				t.Fatalf("%d attrs: %s missing or out of order in %q", n, k, line)
			}
			at += i + len(k)
		}
	}
}
