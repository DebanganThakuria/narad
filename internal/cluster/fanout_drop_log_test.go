package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

// Records a remote child dropped behind are lost for the other cluster:
// the cursor logs them at error level, naming the remote and the range.
func TestRemoteChildDropBehindLogsAnError(t *testing.T) {
	var buf bytes.Buffer
	r := &FanoutRunner{logger: slog.New(slog.NewJSONHandler(&buf, nil))}
	r.recordDropped(context.Background(), fanoutCursorKey{parent: "orders", partition: 2, child: "orders-to-b", remote: true}, 5, 10, 15)
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line %q: %v", buf.String(), err)
	}
	if line["level"] != "ERROR" || line["from_offset"] != float64(10) || line["to_offset"] != float64(15) {
		t.Fatalf("drop-behind on a remote child logged %v, want ERROR", line)
	}
}
