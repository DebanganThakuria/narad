package runtime

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// TestWP8DeletedTopicSeriesStayPrunedWithOpenLog is the integrated
// form: real Logs with metrics, Snapshotter and Poller. The topic is
// deleted from metadata while its log stays open (the purge was skipped
// or failed), the poller prunes it, and the shared reaper's next sweep
// of the still-open log observes into its recorder. The series must not
// come back.
func TestWP8DeletedTopicSeriesStayPrunedWithOpenLog(t *testing.T) {
	dataDir := t.TempDir()
	ms := newRuntimeFakeMetastore()
	ms.topics["gone"] = topic.Topic{Name: "gone", Partitions: 1, RetentionMs: int64(time.Hour / time.Millisecond)}
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	logs := NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, m)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(wp8Caps, nil)
	l, err := logs.Get("gone", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
		t.Fatal(err)
	}
	if err := l.AdvanceHighWatermark(1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	poller := metrics.NewPoller(m, NewSnapshotter(ms, offsets, logs, logger, ""), logger)
	wp8PollOnce(poller)
	if wp8TopicSeriesCount(t, reg, "gone") == 0 {
		t.Fatal("live topic exports no series")
	}

	delete(ms.topics, "gone")
	wp8PollOnce(poller)
	if n := wp8TopicSeriesCount(t, reg, "gone"); n != 0 {
		t.Fatalf("%d series right after the prune", n)
	}
	l.SweepRetentionNow()
	wp8PollOnce(poller)
	wp8PollOnce(poller)
	if n := wp8TopicSeriesCount(t, reg, "gone"); n != 0 {
		t.Fatalf("%d series of the deleted topic came back after a retention sweep of its open log", n)
	}
}
