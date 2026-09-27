package runtime

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func zzWP7bTwoTopicLogs(t *testing.T) *Logs {
	t.Helper()
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "a", ID: "000000000000000a", Partitions: 2})
	ms.put(topic.Topic{Name: "b", ID: "000000000000000b", Partitions: 1})
	return NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)
}

// zzWP7bCommit appends and commits n records on l.
func zzWP7bCommit(t *testing.T, l *storage.Log, n int) {
	t.Helper()
	recs := make([][]byte, n)
	for i := range recs {
		recs[i] = storage.EncodeKeyedRecord("k", 1, []byte("x"))
	}
	first, last, err := l.AppendBatchOwned(recs)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.CommitDurable(first, last); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// Every close of a topic's logs (a purge, a retention change, a reclaim,
// an idle eviction, shutdown) used to hold the node-wide log map lock
// across Log.Close, and a purge across the unlink of the whole topic
// directory too, so every Get on the node (each produce commit, each
// consume scan, of every topic) waited for that I/O: a delete of a large
// topic froze the data plane of every other topic for hundreds of
// milliseconds. The closes now hold only the closed topic's guard.
func TestZZWP7bClosingATopicDoesNotStallOtherTopics(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(g *Logs) error
	}{
		{"PurgeTopic", func(g *Logs) error { _, err := g.PurgeTopic("a", "000000000000000a"); return err }},
		{"CloseTopic", func(g *Logs) error { return g.CloseTopic("a") }},
		{"ClosePartition", func(g *Logs) error { return g.ClosePartition("a", 0) }},
		{"EvictIdleOnce", func(g *Logs) error { g.EvictIdleOnce(time.Minute); return nil }},
		{"CloseAll", func(g *Logs) error { return g.CloseAll() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := zzWP7bTwoTopicLogs(t)
			t.Cleanup(func() { _ = g.CloseAll() })
			la, err := g.Get("a", 0)
			if err != nil {
				t.Fatalf("Get(a): %v", err)
			}
			zzWP7bCommit(t, la, 3)
			lb, err := g.Get("b", 0)
			if err != nil {
				t.Fatalf("Get(b): %v", err)
			}
			backdate(t, g, "a", 0, 2*time.Minute)

			gate := newZZWP7bCloseGate(la)
			defer gate.open()
			gate.armed.Store(true)
			closeDone := make(chan error, 1)
			go func() { closeDone <- tc.close(g) }()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the close never reached a/0")
			}

			// a/0's close is parked inside Log.Close. Topic b must not
			// notice.
			other := make(chan error, 1)
			go func() {
				l, err := g.Get("b", 0)
				if err == nil && l != lb {
					t.Errorf("Get(b) returned a different log")
				}
				if _, ok := g.Peek("b", 0); !ok {
					t.Errorf("Peek(b) = not open")
				}
				if _, ok := g.PeekHighWatermark("b", 0); !ok {
					t.Errorf("PeekHighWatermark(b) = not ok")
				}
				_ = g.OpenCount()
				other <- err
			}()
			select {
			case err := <-other:
				if err != nil {
					t.Fatalf("Get(b): %v", err)
				}
			case <-time.After(2 * time.Second):
				gate.open()
				<-other
				t.Fatalf("%s of topic a stalled Get, Peek and OpenCount of topic b until its Log.Close finished", tc.name)
			}

			gate.open()
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s never finished", tc.name)
			}
			if _, ok := g.Peek("a", 0); ok {
				t.Fatalf("a/0 still open after %s", tc.name)
			}
		})
	}
}

// Observers of the partition being closed still wait for the close, as
// they did behind the map lock: Peek reports "not open" only once the
// high-watermark file holds the closed log's boundary, so a caller that
// then reads the file (PeekHighWatermark, a move's transfer info) never
// sees it half written. A Get waits too and then reopens the partition.
func TestZZWP7bObserversOfTheClosingPartitionWaitForTheClose(t *testing.T) {
	g := zzWP7bTwoTopicLogs(t)
	t.Cleanup(func() { _ = g.CloseAll() })
	la, err := g.Get("a", 0)
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	zzWP7bCommit(t, la, 3)
	if _, err := g.Get("a", 1); err != nil {
		t.Fatalf("Get(a/1): %v", err)
	}

	gate := newZZWP7bCloseGate(la)
	defer gate.open()
	gate.armed.Store(true)
	closeDone := make(chan error, 1)
	go func() { closeDone <- g.ClosePartition("a", 0) }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ClosePartition never reached Log.Close")
	}

	// Another partition of the same topic is observed meanwhile.
	sibling := make(chan bool, 1)
	go func() { _, ok := g.Peek("a", 1); sibling <- ok }()
	select {
	case ok := <-sibling:
		if !ok {
			t.Fatal("Peek(a/1) = not open while a/0 closes")
		}
	case <-time.After(2 * time.Second):
		gate.open()
		t.Fatal("Peek(a/1) waited for a/0's Log.Close")
	}

	type hwmResult struct {
		hwm int64
		ok  bool
	}
	peeked := make(chan bool, 1)
	hwmRead := make(chan hwmResult, 1)
	got := make(chan *storage.Log, 1)
	go func() { _, ok := g.Peek("a", 0); peeked <- ok }()
	go func() { hwm, ok := g.PeekHighWatermark("a", 0); hwmRead <- hwmResult{hwm, ok} }()
	go func() {
		l, err := g.Get("a", 0)
		if err != nil {
			t.Errorf("Get(a/0) after the close: %v", err)
		}
		got <- l
	}()
	select {
	case <-peeked:
		t.Fatal("Peek(a/0) returned while its log was still closing")
	case <-hwmRead:
		t.Fatal("PeekHighWatermark(a/0) returned while its log was still closing")
	case <-got:
		t.Fatal("Get(a/0) returned while its log was still closing")
	case <-time.After(100 * time.Millisecond):
	}

	gate.open()
	if err := <-closeDone; err != nil {
		t.Fatalf("ClosePartition: %v", err)
	}
	<-peeked
	if r := <-hwmRead; !r.ok || r.hwm != 3 {
		t.Fatalf("PeekHighWatermark(a/0) = (%d, %v), want (3, true)", r.hwm, r.ok)
	}
	l := <-got
	if l == nil || l == la {
		t.Fatalf("Get(a/0) = %p, want a fresh log (old %p)", l, la)
	}
	if l.HighWatermark() != 3 {
		t.Fatalf("reopened a/0 hwm = %d, want 3", l.HighWatermark())
	}
	zzWP7bCommit(t, l, 1)
}
