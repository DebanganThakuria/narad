package main

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// shipLogRecord is one record shipLogs kept: its level, message and
// attributes.
type shipLogRecord struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

// shipLogs is a slog.Handler that keeps every record, at every level.
// The warmup logs from its workers, so it is safe for concurrent use.
type shipLogs struct {
	mu      sync.Mutex
	records []shipLogRecord
}

func (h *shipLogs) Enabled(context.Context, slog.Level) bool { return true }

func (h *shipLogs) Handle(_ context.Context, r slog.Record) error {
	rec := shipLogRecord{level: r.Level, msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, rec)
	h.mu.Unlock()
	return nil
}

func (h *shipLogs) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *shipLogs) WithGroup(string) slog.Handler      { return h }

// named returns the records logged with msg.
func (h *shipLogs) named(msg string) []shipLogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []shipLogRecord
	for _, r := range h.records {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

// shipWarmupStore starts a single-node metastore holding the given
// topics, each with its partitions all owned by node-1.
func shipWarmupStore(t *testing.T, topics map[string]int) *metastore.Store {
	t.Helper()
	ctx := context.Background()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	waitForLeadership(t, store)
	for name, partitions := range topics {
		if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: partitions, RetentionMs: 3_600_000}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
		for p := range partitions {
			if err := store.AssignPartition(ctx, name, p, "node-1"); err != nil {
				t.Fatalf("AssignPartition(%s,%d): %v", name, p, err)
			}
		}
	}
	if !waitMetastoreCaughtUp(ctx, store, 5*time.Second) {
		t.Fatal("metastore did not catch up")
	}
	return store
}

// A warmup whose topic listing fails logs it and returns without
// opening anything, so serve still goes on to mark the node ready. The
// listing fails once the metastore's database is closed.
func TestShipWarmupListTopicsFailureLogsAndReturns(t *testing.T) {
	store := shipWarmupStore(t, map[string]int{"orders": 2})
	logs := runtime.NewLogs(filepath.Join(t.TempDir(), "data"), storage.Options{}, store, nil)
	defer logs.CloseAll()
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close() error = %v", err)
	}

	h := &shipLogs{}
	done := make(chan struct{})
	go func() {
		openOwnedPartitionLogs(context.Background(), store, logs, "node-1", slog.New(h))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the warmup did not return after its topic listing failed")
	}

	failed := h.named("retention warmup: list topics failed")
	if len(failed) != 1 || failed[0].level != slog.LevelWarn || failed[0].attrs["err"] == nil {
		t.Fatalf("list failure records = %+v, want one warning carrying the error", failed)
	}
	for p := range 2 {
		if _, open := logs.Peek("orders", p); open {
			t.Fatalf("orders/%d was opened although the listing failed", p)
		}
	}
	if opened := h.named("retention warmup: opened owned partition logs"); len(opened) != 0 {
		t.Fatalf("the warmup reported opened logs after a failed listing: %+v", opened)
	}
}

// shipFailFirstOpen is the metastore the partition-log registry reads
// at every open. The first open of topic fail is refused; every other
// read passes through.
type shipFailFirstOpen struct {
	metastore.Metastore
	fail string

	mu     sync.Mutex
	failed bool
}

func (m *shipFailFirstOpen) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	m.mu.Lock()
	refuse := name == m.fail && !m.failed
	if refuse {
		m.failed = true
	}
	m.mu.Unlock()
	if refuse {
		return topic.Topic{}, errors.New("injected open failure")
	}
	return m.Metastore.GetTopic(ctx, name)
}

// A partition that fails to open is logged as a warning and skipped: the
// warmup goes on with the topic's next partition and with the other
// topics, and returns once every owned partition is open or has failed.
func TestShipWarmupOpenFailureContinues(t *testing.T) {
	store := shipWarmupStore(t, map[string]int{"bad": 2, "good": 2})
	defer store.Close()
	ms := &shipFailFirstOpen{Metastore: store, fail: "bad"}
	logs := runtime.NewLogs(filepath.Join(t.TempDir(), "data"), storage.Options{}, ms, nil)
	defer logs.CloseAll()

	h := &shipLogs{}
	openOwnedPartitionLogs(context.Background(), store, logs, "node-1", slog.New(h))

	failures := h.named("retention warmup: open owned partition failed")
	if len(failures) != 1 {
		t.Fatalf("open failure records = %+v, want exactly one", failures)
	}
	f := failures[0]
	if f.level != slog.LevelWarn || f.attrs["topic"] != "bad" || f.attrs["err"] == nil {
		t.Fatalf("open failure record = %+v, want a warning for topic bad with its error", f)
	}
	failedPartition, ok := f.attrs["partition"].(int64)
	if !ok {
		t.Fatalf("open failure record's partition = %#v, want an int", f.attrs["partition"])
	}
	for _, name := range []string{"bad", "good"} {
		for p := range 2 {
			_, open := logs.Peek(name, p)
			if want := name != "bad" || int64(p) != failedPartition; open != want {
				t.Fatalf("%s/%d open=%v after the warmup returned, want %v", name, p, open, want)
			}
		}
	}
	opened := h.named("retention warmup: opened owned partition logs")
	if len(opened) != 1 || opened[0].attrs["count"] != int64(3) {
		t.Fatalf("opened records = %+v, want one counting 3 of the 4 owned partitions", opened)
	}
}

// On a hybrid host whose memory controller is in the v1 hierarchy, the
// v1 limit decides even when it is unlimited: the unified hierarchy's
// memory.max carries no controller there, so a finite value in it is
// not the process's limit and no soft limit is set.
func TestShipCgroupHybridUnlimitedV1IgnoresV2(t *testing.T) {
	root := zzWP17CgroupRoot(t, map[string]string{
		"proc/self/cgroup":                           "12:memory:/docker/abc\n0::/\n",
		"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
		"sys/fs/cgroup/memory.max":                   "1073741824\n",
	})
	if limit, source := cgroupMemoryLimit(root); limit != 0 || source != "" {
		t.Fatalf("cgroupMemoryLimit = %d from %q, want 0: the v2 file must not be consulted", limit, source)
	}
	if soft, _, _ := containerMemoryLimit(zzWP17NoEnv, root); soft != 0 {
		t.Fatalf("containerMemoryLimit set %d, want no soft limit", soft)
	}
}

// A key flagged base64 that does not decode is shown as it arrived
// rather than dropped or shown as garbage hex.
func TestShipDisplayKeyUndecodableBase64(t *testing.T) {
	for _, tc := range []struct {
		msg  consumedMessage
		want string
	}{
		{consumedMessage{Key: "not base64!", KeyEncoding: "base64"}, "not base64!"},
		{consumedMessage{Key: "/w==", KeyEncoding: "base64"}, "ff (binary)"},
		{consumedMessage{Key: "/w==", KeyEncoding: ""}, "/w=="},
	} {
		if got := displayKey(tc.msg); got != tc.want {
			t.Fatalf("displayKey(%+v) = %q, want %q", tc.msg, got, tc.want)
		}
	}
}
