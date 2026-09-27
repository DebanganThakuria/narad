package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// wp15SlowOpens is the metastore the partition-log registry reads at
// every open: GetTopic runs under the topic's guard, right before the
// log itself is opened, so it sees each open while it is in progress.
// It stretches every open by a few milliseconds and records how many
// were in progress at once, overall and per topic.
type wp15SlowOpens struct {
	metastore.Metastore
	hold time.Duration

	mu        sync.Mutex
	inFlight  int
	maxFlight int
	perTopic  map[string]int
	maxTopic  int
	opens     map[string]int
}

func (m *wp15SlowOpens) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	m.mu.Lock()
	m.inFlight++
	m.maxFlight = max(m.maxFlight, m.inFlight)
	m.perTopic[name]++
	m.maxTopic = max(m.maxTopic, m.perTopic[name])
	m.opens[name]++
	m.mu.Unlock()

	time.Sleep(m.hold)

	m.mu.Lock()
	m.inFlight--
	m.perTopic[name]--
	m.mu.Unlock()
	return m.Metastore.GetTopic(ctx, name)
}

// The startup warmup opens the partitions of different topics
// concurrently, a bounded number at a time, never two of one topic at
// once, and returns only when every owned partition is open, so the
// readiness marked after it still covers them all. Partitions owned by
// another node are left alone.
func TestWP15WarmupOpensTopicsConcurrently(t *testing.T) {
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
	defer store.Close()
	waitForLeadership(t, store)

	const topics, partitions = 12, 3
	for i := range topics {
		name := fmt.Sprintf("t%02d", i)
		if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: partitions, RetentionMs: 3_600_000}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
		for p := range partitions {
			owner := "node-1"
			if p == partitions-1 {
				owner = "node-2" // one partition of every topic is someone else's
			}
			if err := store.AssignPartition(ctx, name, p, owner); err != nil {
				t.Fatalf("AssignPartition(%s,%d): %v", name, p, err)
			}
		}
	}
	if !waitMetastoreCaughtUp(ctx, store, 5*time.Second) {
		t.Fatal("metastore did not catch up")
	}

	slow := &wp15SlowOpens{Metastore: store, hold: 20 * time.Millisecond, perTopic: map[string]int{}, opens: map[string]int{}}
	logs := runtime.NewLogs(filepath.Join(t.TempDir(), "data"), storage.Options{}, slow, nil)
	defer logs.CloseAll()

	openOwnedPartitionLogs(ctx, store, logs, "node-1", slog.New(slog.NewTextHandler(io.Discard, nil)))

	for i := range topics {
		name := fmt.Sprintf("t%02d", i)
		for p := range partitions {
			_, open := logs.Peek(name, p)
			if want := p != partitions-1; open != want {
				t.Fatalf("%s/%d open=%v after the warmup returned, want %v", name, p, open, want)
			}
		}
		if got := slow.opens[name]; got != partitions-1 {
			t.Fatalf("%s opened %d times, want %d", name, got, partitions-1)
		}
	}
	bound := min(warmupOpenTopics, goruntime.GOMAXPROCS(0))
	if slow.maxFlight > bound {
		t.Fatalf("%d opens in progress at once, want at most %d", slow.maxFlight, bound)
	}
	if bound > 1 && slow.maxFlight < 2 {
		t.Fatalf("opens never overlapped (max %d in progress, bound %d): the warmup opens topics one at a time", slow.maxFlight, bound)
	}
	if slow.maxTopic > 1 {
		t.Fatalf("%d opens of one topic in progress at once, want 1", slow.maxTopic)
	}
	t.Logf("max opens in progress: %d (bound %d)", slow.maxFlight, bound)
}
