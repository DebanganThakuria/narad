package runtime

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// A close that lands between a commit's append and its CommitDurable
// used to make the batch durable in Close's final drain with no commit
// attached; CommitDurable then failed with ErrLogClosed, nothing
// discarded the batch, and the dispatcher's retry appended the same
// records again under a high-watermark covering both copies. Every
// close of a live log (a retention change's CloseTopic, a reclaim's
// ClosePartition, shutdown's CloseAll) now waits out the commit.
func TestZZWP7bCloseDuringCommitDoesNotDuplicate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(g *Logs) error
	}{
		{"CloseTopic", func(g *Logs) error { return g.CloseTopic("orders") }},
		{"ClosePartition", func(g *Logs) error { return g.ClosePartition("orders", 0) }},
		{"CloseAll", func(g *Logs) error { return g.CloseAll() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := newZZWP7bMetastore()
			ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 1, RetentionMs: 3_600_000})
			g := NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)
			t.Cleanup(func() { _ = g.CloseAll() })

			batch := func(tag string) [][]byte {
				return [][]byte{[]byte("r0-" + tag), []byte("r1-" + tag), []byte("r2-" + tag)}
			}
			commit := func(tag string) error {
				return g.WithProduceLock("orders", 0, func(l *storage.Log) error {
					first, last, err := l.AppendBatchOwned(batch(tag))
					if err != nil {
						return err
					}
					return l.CommitDurable(first, last)
				})
			}
			if err := commit("a"); err != nil {
				t.Fatalf("commit a: %v", err)
			}

			closeDone := make(chan error, 1)
			firstErr := g.WithProduceLock("orders", 0, func(l *storage.Log) error {
				first, last, err := l.AppendBatchOwned(batch("b"))
				if err != nil {
					return err
				}
				// The close arrives now, from another goroutine. Give it
				// every chance to run to completion before the commit.
				go func() { closeDone <- tc.close(g) }()
				select {
				case err := <-closeDone:
					closeDone <- err
				case <-time.After(300 * time.Millisecond):
				}
				return l.CommitDurable(first, last)
			})
			if err := <-closeDone; err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if firstErr != nil {
				// What the dispatcher does with a failed commit: retry the
				// same records.
				t.Errorf("commit b under the produce lock failed with %v: the close did not wait for it", firstErr)
				if err := commit("b"); err != nil {
					t.Fatalf("retry of b: %v", err)
				}
			}

			l, err := g.Get("orders", 0)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			hwm := l.HighWatermark()
			seen := map[string][]int64{}
			for off := range hwm {
				rec, err := l.Read(off)
				if err != nil {
					t.Fatalf("Read(%d): %v", off, err)
				}
				seen[string(rec)] = append(seen[string(rec)], off)
			}
			for rec, offs := range seen {
				if len(offs) > 1 {
					t.Errorf("record %s visible at offsets %v", rec, offs)
				}
			}
			if hwm != 6 {
				t.Errorf("high-watermark = %d, want 6 (two batches of three)", hwm)
			}
		})
	}
}

// zzWP7bCloseGate installs a wake notifier on l that, once armed, parks
// the goroutine running Log.Close (Close wakes waiters after its final
// drain) until released, so a test can act while a close is in flight.
type zzWP7bCloseGate struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newZZWP7bCloseGate(l *storage.Log) *zzWP7bCloseGate {
	cg := &zzWP7bCloseGate{entered: make(chan struct{}), release: make(chan struct{})}
	l.SetWakeNotifier(func() {
		if cg.armed.CompareAndSwap(true, false) {
			close(cg.entered)
			<-cg.release
		}
	})
	return cg
}

func (cg *zzWP7bCloseGate) open() { cg.once.Do(func() { close(cg.release) }) }

// ReplacePartitionDir used to take the topic guard first and then, while
// holding it, retire the partition's produce mutex. A commit takes that
// mutex first and the guard second (WithProduceLock, then Get's slow
// path when the log is not open), so a commit arriving while the
// install closed the log deadlocked the two for good: the install
// waited for the commit's mutex, the commit for the install's guard.
func TestZZWP7bReplacePartitionDirDoesNotDeadlockWithCommit(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 1})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)

	l, err := g.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	gate := newZZWP7bCloseGate(l)
	defer gate.open()
	gate.armed.Store(true)

	installDone := make(chan error, 1)
	go func() {
		installDone <- g.ReplacePartitionDir("orders", 0, func() error { return nil })
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ReplacePartitionDir never closed the open log")
	}
	// A commit for the partition arrives while the install is closing
	// its log, and gets as far as it can before the close finishes.
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- g.WithProduceLock("orders", 0, func(*storage.Log) error { return nil })
	}()
	time.Sleep(50 * time.Millisecond)
	gate.open()

	deadline := time.After(5 * time.Second)
	for range 2 {
		select {
		case err := <-installDone:
			if err != nil {
				t.Fatalf("ReplacePartitionDir: %v", err)
			}
		case err := <-commitDone:
			if err != nil {
				t.Fatalf("WithProduceLock: %v", err)
			}
		case <-deadline:
			// Deadlocked: leave the goroutines (and the Logs) be, since
			// closing it would block on the same locks.
			t.Fatal("ReplacePartitionDir and a commit on the same partition deadlocked")
		}
	}
	if err := g.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
}
