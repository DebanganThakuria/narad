package runtime

// Idle eviction correctness: eviction must close only logs that are
// genuinely idle and owe no retention work, must never race a produce
// or a fresh Get, and an evicted log must reopen transparently with
// its durable state (records + high watermark) intact.

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func testLogs(t *testing.T) *Logs {
	t.Helper()
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, nil, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	return g
}

func appendAndCommit(t *testing.T, g *Logs, topicName string, idx int, payload string) {
	t.Helper()
	l, err := g.Get(topicName, idx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte(payload))); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := l.AdvanceHighWatermark(l.NextOffset()); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
}

func backdate(t *testing.T, g *Logs, topicName string, idx int, by time.Duration) {
	t.Helper()
	g.mu.RLock()
	e, ok := g.logs[keyOf(topicName, idx)]
	g.mu.RUnlock()
	if !ok {
		t.Fatalf("log %s/%d not open", topicName, idx)
	}
	e.lastAccess.Store(time.Now().Add(-by).UnixNano())
}

func TestEvictIdleClosesOnlyIdleLogs(t *testing.T) {
	g := testLogs(t)
	appendAndCommit(t, g, "idle", 0, "a")
	appendAndCommit(t, g, "busy", 0, "b")
	backdate(t, g, "idle", 0, time.Hour)

	if n := g.EvictIdleOnce(30 * time.Minute); n != 1 {
		t.Fatalf("EvictIdleOnce() = %d, want 1", n)
	}
	if _, open := g.Peek("idle", 0); open {
		t.Fatal("idle log still open after eviction")
	}
	if _, open := g.Peek("busy", 0); !open {
		t.Fatal("busy log was evicted")
	}
}

func TestEvictedLogReopensWithDurableState(t *testing.T) {
	g := testLogs(t)
	appendAndCommit(t, g, "orders", 0, "payload-1")
	backdate(t, g, "orders", 0, time.Hour)

	if n := g.EvictIdleOnce(30 * time.Minute); n != 1 {
		t.Fatalf("EvictIdleOnce() = %d, want 1", n)
	}

	// The durable HWM file must be exact for a cleanly closed log —
	// this is what the fan-out closed-path check relies on.
	dir := storage.TopicPartitionDir(g.DataDir(), "orders", 0)
	hwm, found, err := storage.ReadPersistedHighWatermark(dir)
	if err != nil || !found || hwm != 1 {
		t.Fatalf("ReadPersistedHighWatermark = (%d, %v, %v), want (1, true, nil)", hwm, found, err)
	}

	// Lazy reopen restores everything.
	l, err := g.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get after eviction: %v", err)
	}
	if l.HighWatermark() != 1 || l.NextOffset() != 1 {
		t.Fatalf("reopened log HWM=%d next=%d, want 1/1", l.HighWatermark(), l.NextOffset())
	}
	_, _, payload, err := l.ReadKeyed(0)
	if err != nil || string(payload) != "payload-1" {
		t.Fatalf("ReadKeyed(0) = %q, %v; want payload-1", payload, err)
	}
}

// A Get between the candidate scan and the close must abort the
// eviction: the recheck under the map lock sees the fresh stamp.
func TestEvictionAbortsWhenLogTouchedAfterScan(t *testing.T) {
	g := testLogs(t)
	appendAndCommit(t, g, "orders", 0, "x")
	backdate(t, g, "orders", 0, time.Hour)

	// Simulate the interleaving directly: refresh the stamp the way a
	// concurrent Get would, then sweep. Nothing may be evicted.
	if _, err := g.Get("orders", 0); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n := g.EvictIdleOnce(30 * time.Minute); n != 0 {
		t.Fatalf("EvictIdleOnce() = %d, want 0 (log was just touched)", n)
	}
	if _, open := g.Peek("orders", 0); !open {
		t.Fatal("touched log was evicted")
	}
}

// Peek (the metrics path) must not keep a log warm.
func TestPeekDoesNotResetIdleness(t *testing.T) {
	g := testLogs(t)
	appendAndCommit(t, g, "orders", 0, "x")
	backdate(t, g, "orders", 0, time.Hour)

	for range 10 {
		if _, ok := g.Peek("orders", 0); !ok {
			t.Fatal("Peek: log should be open")
		}
	}
	if n := g.EvictIdleOnce(30 * time.Minute); n != 1 {
		t.Fatalf("EvictIdleOnce() = %d, want 1 (Peek must not stamp)", n)
	}
}

// Retention that still owes sealed-segment deletions defers eviction;
// a keep-forever log (MaxAge 0) evicts regardless of segment count.
func TestEvictionDefersToPendingRetention(t *testing.T) {
	base := t.TempDir()

	// Age-based retention + multiple segments: deferred.
	aged := NewLogs(base+"/aged", storage.Options{
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  1, // roll every synced frame
		Retention:     storage.RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Hour},
	}, nil, nil)
	t.Cleanup(func() { _ = aged.CloseAll() })
	appendAndCommit(t, aged, "t", 0, "a")
	appendAndCommit(t, aged, "t", 0, "b")
	appendAndCommit(t, aged, "t", 0, "c")
	backdate(t, aged, "t", 0, time.Hour)
	l, _ := aged.Get("t", 0)
	backdate(t, aged, "t", 0, time.Hour) // re-stamp after the Get above
	if l.SegmentCount() <= 1 {
		t.Fatalf("test setup: want multiple segments, got %d", l.SegmentCount())
	}
	if n := aged.EvictIdleOnce(30 * time.Minute); n != 0 {
		t.Fatalf("EvictIdleOnce() = %d, want 0 (retention owes deletions)", n)
	}

	// Keep-forever + multiple segments: evicts.
	forever := NewLogs(base+"/forever", storage.Options{
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  1,
	}, nil, nil)
	t.Cleanup(func() { _ = forever.CloseAll() })
	appendAndCommit(t, forever, "t", 0, "a")
	appendAndCommit(t, forever, "t", 0, "b")
	backdate(t, forever, "t", 0, time.Hour)
	if n := forever.EvictIdleOnce(30 * time.Minute); n != 1 {
		t.Fatalf("EvictIdleOnce() = %d, want 1 (keep-forever evicts)", n)
	}
}

// Produce racing eviction: hammer produce on one partition while
// sweeping with a zero idle window (every log always a candidate).
// The locks must serialize everything — no lost records, no panics.
func TestEvictionRacesProduceSafely(t *testing.T) {
	g := testLogs(t)
	const records = 200

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range records {
			if err := g.WithProduceLock("orders", 0, func(l *storage.Log) error {
				if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte("r"))); err != nil {
					return err
				}
				if err := l.Sync(); err != nil {
					return err
				}
				return l.AdvanceHighWatermark(l.NextOffset())
			}); err != nil {
				t.Errorf("produce: %v", err)
				return
			}
		}
	}()

	for i := 0; done != nil; i++ {
		select {
		case <-done:
			done = nil
		default:
			g.EvictIdleOnce(0) // idleAfter 0: everything idle, maximal pressure
		}
	}

	l, err := g.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if l.NextOffset() != records {
		t.Fatalf("NextOffset = %d, want %d (records lost across evictions)", l.NextOffset(), records)
	}
}

func TestSplitKey(t *testing.T) {
	for _, tt := range []struct {
		key   string
		topic string
		idx   int
		ok    bool
	}{
		{"orders/0", "orders", 0, true},
		{"my.topic-2/17", "my.topic-2", 17, true},
		{"orders/", "", 0, false},
		{"/3", "", 0, false},
		{"orders/x1", "", 0, false},
	} {
		topicName, idx, ok := splitKey(tt.key)
		if topicName != tt.topic || idx != tt.idx || ok != tt.ok {
			t.Fatalf("splitKey(%q) = (%q, %d, %v), want (%q, %d, %v)",
				tt.key, topicName, idx, ok, tt.topic, tt.idx, tt.ok)
		}
	}
}
