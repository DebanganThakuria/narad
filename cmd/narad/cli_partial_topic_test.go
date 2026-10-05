package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// partialTopicServer answers topic GETs as a cluster does while one
// node is down: "orders" has partition 1 on the dead node-b, so the
// answer is partial and that partition is a zero placeholder; "logs" is
// whole.
func partialTopicServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/topics":
			fmt.Fprint(w, `{"topics":[{"name":"orders","partitions":3},{"name":"logs","partitions":1}]}`)
		case "/v1/topics/orders":
			fmt.Fprint(w, `{"name":"orders","partitions":3,"partial":true,"partition_stats":[`+
				`{"index":0,"oldest_offset":2,"next_offset":10,"high_watermark":10,"size_bytes":1000,"owner_node":"node-a","status":"ok"},`+
				`{"index":1,"oldest_offset":0,"next_offset":0,"high_watermark":0,"size_bytes":0,"owner_node":"node-b","status":"owner_unavailable","owner_liveness":"dead"},`+
				`{"index":2,"oldest_offset":0,"next_offset":5,"high_watermark":5,"size_bytes":500,"owner_node":"node-c","status":"ok"}]}`)
		case "/v1/topics/logs":
			fmt.Fprint(w, `{"name":"logs","partitions":1,"partition_stats":[`+
				`{"index":0,"oldest_offset":0,"next_offset":7,"high_watermark":7,"size_bytes":700,"owner_node":"node-a","status":"ok"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The server report leaves a partial topic's unavailable partitions out
// of its totals and its owner count, and says how many there are.
// Master counted the dead owner and printed the partial totals as if
// they were the topic's.
func TestServerReportMarksPartialTopics(t *testing.T) {
	srv := partialTopicServer(t)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", srv.URL)

	out, _, err := captureCLIOutput(t, func() error { return route([]string{"server", "report"}) }, "")
	if err != nil {
		t.Fatalf("server report: %v", err)
	}
	var orders, logs string
	for _, line := range strings.Split(out, "\n") {
		switch fields := strings.Fields(line); {
		case len(fields) > 0 && fields[0] == "orders":
			orders = line
		case len(fields) > 0 && fields[0] == "logs":
			logs = line
		}
	}
	// TOPIC PARTS MESSAGES SIZE OWNERS ROLE
	if !regexp.MustCompile(`^orders\s+3\s+15\s+1\.5KiB\s+2\s`).MatchString(orders) {
		t.Fatalf("orders line = %q, want 15 messages, 1.5KiB and 2 owners (the dead node's placeholder left out)\n%s", orders, out)
	}
	if !strings.Contains(orders, "1 of 3 partitions unavailable") {
		t.Fatalf("orders line = %q, want it marked 1 of 3 partitions unavailable\n%s", orders, out)
	}
	if strings.Contains(logs, "unavailable") {
		t.Fatalf("logs line = %q, want no marker on a whole topic", logs)
	}
}

// partitionRange (narad replay) refuses a partition whose owner is down
// instead of reading the placeholder's zeros as an empty range.
func TestReplayRangeRefusesAnUnavailablePartition(t *testing.T) {
	c := newHTTPClient(partialTopicServer(t).URL)
	oldest, hwm, err := partitionRange(c, "orders", 1)
	if err == nil || !strings.Contains(err.Error(), "partition 1") || !strings.Contains(err.Error(), "node-b") {
		t.Fatalf("partitionRange(orders, 1) = [%d, %d) err %v, want an error naming partition 1 and its owner node-b", oldest, hwm, err)
	}
	if oldest, hwm, err := partitionRange(c, "orders", 0); err != nil || oldest != 2 || hwm != 10 {
		t.Fatalf("partitionRange(orders, 0) = [%d, %d) err %v, want [2, 10)", oldest, hwm, err)
	}
}

// peekStartCursors (narad sub --peek) refuses an unavailable partition
// rather than starting it at offset 0, which would replay its whole
// history once the owner returns; with --from the stats are not needed.
func TestPeekStartRefusesAnUnavailablePartition(t *testing.T) {
	c := newHTTPClient(partialTopicServer(t).URL)
	if cursors, err := peekStartCursors(c, "orders", 1, -1); err == nil || !strings.Contains(err.Error(), "partition 1") {
		t.Fatalf("peekStartCursors(orders, 1) = %v err %v, want an error naming partition 1", cursors, err)
	}
	if cursors, err := peekStartCursors(c, "orders", -1, -1); err == nil || !strings.Contains(err.Error(), "partition 1") {
		t.Fatalf("peekStartCursors(orders, all) = %v err %v, want an error naming partition 1", cursors, err)
	}
	cursors, err := peekStartCursors(c, "orders", 0, -1)
	if err != nil || len(cursors) != 1 || cursors[0] != 10 {
		t.Fatalf("peekStartCursors(orders, 0) = %v err %v, want partition 0 from its tail, 10", cursors, err)
	}
	cursors, err = peekStartCursors(c, "orders", -1, 3)
	if err != nil || len(cursors) != 3 || cursors[1] != 3 {
		t.Fatalf("peekStartCursors(orders, all, --from 3) = %v err %v, want every partition from 3", cursors, err)
	}
}

// oldestOffset refuses an unavailable partition instead of answering 0.
func TestOldestOffsetRefusesAnUnavailablePartition(t *testing.T) {
	c := newHTTPClient(partialTopicServer(t).URL)
	if off, err := oldestOffset(c, "orders", 1); err == nil || !strings.Contains(err.Error(), "partition 1") {
		t.Fatalf("oldestOffset(orders, 1) = %d err %v, want an error naming partition 1", off, err)
	}
	if off, err := oldestOffset(c, "orders", 0); err != nil || off != 2 {
		t.Fatalf("oldestOffset(orders, 0) = %d err %v, want 2", off, err)
	}
}
