package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// wp8TopicSeries counts the series in reg that carry topic=name.
func wp8TopicSeries(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "topic" && l.GetValue() == name {
					n++
				}
			}
		}
	}
	return n
}

// wp8HookedProvider returns snaps and runs during (when set) while the
// poller is inside Snapshot, after it took its epoch and before it
// prunes: the window in which a recreated topic's log can open.
type wp8HookedProvider struct {
	snaps  []TopicSnapshot
	during func()
}

func (p *wp8HookedProvider) Snapshot(context.Context) ([]TopicSnapshot, error) {
	if p.during != nil {
		p.during()
		p.during = nil
	}
	return p.snaps, nil
}

func wp8Topic(name string) []TopicSnapshot {
	return []TopicSnapshot{{Topic: name, Partitions: []PartitionSnapshot{{Partition: 0, HighWatermark: 1}}}}
}

// TestWP8PrunedTopicStaysPruned: a deleted topic whose log is still
// open (its purge was skipped or failed) keeps observing into its
// storage recorder, and a request that straddled the delete bumps its
// partition counters. Neither may bring the topic's series back for
// good once the poller pruned them.
func TestWP8PrunedTopicStaysPruned(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	provider := &wp8HookedProvider{snaps: wp8Topic("gone")}
	p := NewPoller(m, provider, discardLogger())

	zombie := m.StorageRecorder("gone", 0)
	zombie.ObserveFlush(time.Millisecond, 10)
	m.PartitionCounters("gone", 0).MessagesProduced.Inc()
	p.tick(context.Background())
	if wp8TopicSeries(t, reg, "gone") == 0 {
		t.Fatal("live topic exports no series")
	}

	provider.snaps = nil // metadata delete applied; the log stays open
	p.tick(context.Background())
	if n := wp8TopicSeries(t, reg, "gone"); n != 0 {
		t.Fatalf("%d series left right after the prune", n)
	}

	// The shared reaper's next sweep of the still-open log, a flush, and
	// a consume that straddled the delete.
	zombie.ObserveRetentionRun(time.Millisecond)
	zombie.ObserveFlush(time.Millisecond, 10)
	zombie.ObserveHighWatermarkPersist(time.Millisecond, "ok")
	zombie.IncRetentionDeletion("age", 1, 1)
	m.PartitionCounters("gone", 0).MessagesConsumed.Inc()
	p.tick(context.Background())
	p.tick(context.Background())
	// A prune of any other topic makes every recorder re-resolve again.
	m.pruneTopicSeries("other", m.snapshotEpoch())
	zombie.ObserveFsync(time.Millisecond)
	p.tick(context.Background())
	if n := wp8TopicSeries(t, reg, "gone"); n != 0 {
		t.Fatalf("%d series of the deleted topic survive every later tick", n)
	}
}

// TestWP8RecreatedTopicKeepsItsSeries: marking the deleted topic's
// recorders dead must not silence a same-named successor, whether its
// log opened after the prune or inside the tick that pruned.
func TestWP8RecreatedTopicKeepsItsSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	provider := &wp8HookedProvider{snaps: wp8Topic("t")}
	p := NewPoller(m, provider, discardLogger())
	old := m.StorageRecorder("t", 0)
	old.ObserveFlush(time.Millisecond, 1)
	p.tick(context.Background())

	// Deleted, and recreated while the poller is between its topic list
	// and the prune: the new incarnation's log opens in that window.
	var inWindow interface {
		ObserveFlush(time.Duration, int64)
	}
	provider.snaps = nil
	provider.during = func() { inWindow = m.StorageRecorder("t", 0) }
	p.tick(context.Background())
	provider.snaps = wp8Topic("t")
	after := m.StorageRecorder("t", 1)

	inWindow.ObserveFlush(time.Millisecond, 100)
	after.ObserveFlush(time.Millisecond, 1000)
	old.ObserveFlush(time.Millisecond, 7) // the old incarnation's log: dropped
	m.PartitionCounters("t", 0).MessagesProduced.Add(3)
	p.tick(context.Background())
	p.tick(context.Background())

	if got, _ := readCounter(t, reg, "narad_storage_flush_bytes_total", map[string]string{"topic": "t", "partition": "0"}); got != 100 {
		t.Fatalf("partition 0 flush bytes = %v, want 100 (the recorder opened inside the pruning tick is live; the old one is not)", got)
	}
	if got, _ := readCounter(t, reg, "narad_storage_flush_bytes_total", map[string]string{"topic": "t", "partition": "1"}); got != 1000 {
		t.Fatalf("partition 1 flush bytes = %v, want 1000", got)
	}
	if got, _ := readCounter(t, reg, "narad_messages_produced_total", map[string]string{"topic": "t", "partition": "0"}); got != 3 {
		t.Fatalf("messages produced = %v, want 3", got)
	}
}
