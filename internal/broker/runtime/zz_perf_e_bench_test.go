package runtime

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzPerfETimer is the part of *testing.B a close drives: the timer runs
// across the close and nowhere else.
type zzPerfETimer interface {
	StartTimer()
	StopTimer()
}

// zzPerfECloseAll builds a node with topics x parts open logs, each
// holding one record committed the way the owner commits (Append, then
// CommitDurable, whose first advance empties the hwm file), and times
// one Logs.CloseAll over them: every log's Close then writes its exact
// hwm file and syncs it. It returns that time.
func zzPerfECloseAll(tb testing.TB, tm zzPerfETimer, topics, parts int) time.Duration {
	tb.Helper()
	ms := &zzWP7bBenchMetastore{
		topics:   make(map[string]topic.Topic, topics),
		versions: make(map[string]*atomic.Uint64, topics),
	}
	names := make([]string, topics)
	for i := range names {
		names[i] = fmt.Sprintf("orders-%04d", i)
		v := &atomic.Uint64{}
		v.Store(1)
		ms.topics[names[i]] = topic.Topic{Name: names[i], ID: fmt.Sprintf("%016x", i+1), Partitions: parts}
		ms.versions[names[i]] = v
	}
	g := NewLogs(tb.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	tb.Cleanup(func() { _ = g.CloseAll() })

	// Setup commits run 8 at a time: one record per log costs a data
	// sync, and thousands of them one after another would dominate the
	// run's wall time.
	type key struct{ topic, part int }
	work := make(chan key)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	rec := storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range work {
				err := g.WithProduceLock(names[k.topic], k.part, func(l *storage.Log) error {
					off, err := l.Append(rec)
					if err != nil {
						return err
					}
					return l.CommitDurable(off, off)
				})
				if err != nil {
					errs <- err
					for range work {
					}
					return
				}
			}
		}()
	}
	for t := range topics {
		for p := range parts {
			work <- key{t, p}
		}
	}
	close(work)
	wg.Wait()
	close(errs)
	for err := range errs {
		tb.Fatal(err)
	}
	if got := g.OpenCount(); got != topics*parts {
		tb.Fatalf("%d logs open, want %d", got, topics*parts)
	}

	start := time.Now()
	tm.StartTimer()
	err := g.CloseAll()
	tm.StopTimer()
	took := time.Since(start)
	if err != nil {
		tb.Fatal(err)
	}
	if got := g.OpenCount(); got != 0 {
		tb.Fatalf("%d logs open after CloseAll", got)
	}
	return took
}

// zzPerfECommitterClose builds a committer over parts partitions whose
// acked frontier moved past their consumer.offset since it was last
// levelled, with every consumer.ahead already written out, and times
// its Close: the final tick then levels (writes and syncs)
// consumer.offset in every partition. It returns that time.
func zzPerfECommitterClose(tb testing.TB, tm zzPerfETimer, parts int) time.Duration {
	tb.Helper()
	c, src, _ := zzWP16Committer(tb, parts)
	// The first writeout after priming levels every partition; the
	// second, a moment later, is inside consumerOffsetLevelEvery and
	// leaves the new frontier for Close to level.
	for i := 1; i <= 2; i++ {
		src.step(c, i)
		if err := c.flush(); err != nil {
			tb.Fatal(err)
		}
	}
	if st := c.lastStats; st.levelled != 0 || st.wroteOut != parts {
		tb.Fatalf("setup flush wrote out %d and levelled %d of %d partitions, want all written out and none levelled", st.wroteOut, st.levelled, parts)
	}

	start := time.Now()
	tm.StartTimer()
	err := c.Close()
	tm.StopTimer()
	took := time.Since(start)
	if err != nil {
		tb.Fatal(err)
	}
	if st := c.lastStats; st.levelled != parts || st.wroteOut != 0 {
		tb.Fatalf("Close wrote out %d and levelled %d of %d partitions, want none written out and all levelled", st.wroteOut, st.levelled, parts)
	}
	return took
}

// BenchmarkPerfECloseAll times Logs.CloseAll at shutdown over
// topics x partitions logs that each committed since they opened, so
// every Close writes and syncs its hwm file. ns/partition is the
// shutdown cost per recently committed partition.
func BenchmarkPerfECloseAll(b *testing.B) {
	for _, tc := range []struct{ topics, parts int }{
		{1, 2000},
		{50, 40},
		{2000, 1},
	} {
		b.Run(fmt.Sprintf("%dx%d", tc.topics, tc.parts), func(b *testing.B) {
			b.StopTimer()
			var total time.Duration
			for range b.N {
				total += zzPerfECloseAll(b, b, tc.topics, tc.parts)
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(tc.topics*tc.parts), "ns/partition")
		})
	}
}

// BenchmarkPerfECommitterClose times the consumer offset committer's
// Close at shutdown, which levels consumer.offset in every partition
// whose frontier moved since its last level. ns/partition is the cost
// per such partition.
func BenchmarkPerfECommitterClose(b *testing.B) {
	for _, parts := range []int{2000} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			b.StopTimer()
			var total time.Duration
			for range b.N {
				total += zzPerfECommitterClose(b, b, parts)
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/float64(parts), "ns/partition")
		})
	}
}
