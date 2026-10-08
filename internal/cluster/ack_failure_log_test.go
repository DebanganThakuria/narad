package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

// warnLines returns the log lines with msg.
func (l *syncLog) warnLines(msg string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for line := range bytes.Lines(l.buf.Bytes()) {
		var m map[string]any
		if json.Unmarshal(line, &m) == nil && m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

// Every forwarded ack that fails is counted, per record, by op and by
// what the client was told, and each owner gets at most one warning a
// minute that sums up the failures since its last one. A client that
// went away is counted but not logged: nothing is wrong with the owner.
func TestAckForwardFailuresAreCountedAndLoggedOncePerMinute(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	rt := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	reg := prometheus.NewRegistry()
	rt.RegisterMetrics(reg)
	logs := &syncLog{}
	rt.SetLogger(slog.New(slog.NewJSONHandler(logs, nil)))
	now := time.Unix(1_000_000, 0)
	rt.ackFailures.now = func() time.Time { return now }

	notSent := failingFrames{err: fmt.Errorf("%w: dial %s: connection refused", clusterrpc.ErrNotSent, ackTestOwnerAddr)}
	noReply := failingFrames{err: fmt.Errorf("cluster rpc request timed out: %w", context.DeadlineExceeded)}
	handle := consumer.Handle{Partition: 1, Offset: 3, Nonce: 4}
	ack := func(ctx context.Context, frames failingFrames, route func(*Router, context.Context) bool) {
		t.Helper()
		rt.peer = &PeerClient{frames: frames}
		if !route(rt, ctx) {
			t.Fatal("not forwarded")
		}
	}
	single := func(r *Router, ctx context.Context) bool {
		return r.RouteAck(ctx, httptest.NewRecorder(), nil, "orders", handle)
	}
	extend := func(r *Router, ctx context.Context) bool {
		return r.RouteExtendAck(ctx, httptest.NewRecorder(), nil, "orders", handle)
	}

	for range 3 {
		ack(context.Background(), notSent, single)
	}
	ack(context.Background(), noReply, extend)
	ack(context.Background(), noReply, extend)
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	ack(gone, noReply, single)

	lines := logs.warnLines("forwarded acks failed")
	if len(lines) != 1 {
		t.Fatalf("%d warnings within a minute, want 1: %v", len(lines), lines)
	}
	first := lines[0]
	if first["owner"] != ackTestOwnerAddr || first["not_sent"] != 1.0 || first["unknown"] != 0.0 || first["phase"] != "round_trip" ||
		!strings.Contains(fmt.Sprint(first["last_error"]), "connection refused") {
		t.Fatalf("first warning = %v", first)
	}

	// A batch ack whose OpAckBatch fails counts each of its records.
	rt.peer = &PeerClient{frames: noReply}
	statuses, msgs := make([]int, 2), make([]string, 2)
	rt.RouteAckBatch(context.Background(), "orders", "nack", []consumer.Handle{handle, {Partition: 1, Offset: 4, Nonce: 4}}, statuses, msgs)

	now = now.Add(ackFailureLogInterval)
	ack(context.Background(), noReply, single)
	lines = logs.warnLines("forwarded acks failed")
	if len(lines) != 2 {
		t.Fatalf("%d warnings after a minute, want 2: %v", len(lines), lines)
	}
	if second := lines[1]; second["not_sent"] != 2.0 || second["unknown"] != 5.0 {
		t.Fatalf("second warning = %v, want the 2 not_sent and 5 unknown failures since the first", second)
	}

	for _, c := range []struct {
		op, outcome string
		want        float64
	}{
		{"ack", "not_sent", 3},
		{"extend", "unknown", 2},
		{"nack", "unknown", 2},
		{"ack", "unknown", 1},
		{"ack", "client_gone", 1},
	} {
		got := testutil.ToFloat64(rt.ackFailures.counter.WithLabelValues(c.op, c.outcome))
		if got != c.want {
			t.Errorf("narad_cluster_ack_forward_failures_total{op=%q,outcome=%q} = %v, want %v", c.op, c.outcome, got, c.want)
		}
	}
	if n, err := testutil.GatherAndCount(reg, "narad_cluster_ack_forward_failures_total"); err != nil || n != 5 {
		t.Fatalf("gathered %d series (%v), want 5", n, err)
	}
}
