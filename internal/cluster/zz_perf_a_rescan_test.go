package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// zzPerfASink is a local committer that takes delay per commit (the
// owner's fsync) and records every WAL seq it committed, per partition,
// in commit order.
type zzPerfASink struct {
	delay     time.Duration
	committed atomic.Int64
	mu        sync.Mutex
	seqs      map[int][]uint64
}

func (s *zzPerfASink) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := s.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (s *zzPerfASink) CommitAcceptedProduceBatch(_ context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	if s.seqs == nil {
		s.seqs = map[int][]uint64{}
	}
	for _, r := range recs {
		s.seqs[r.TargetPartition] = append(s.seqs[r.TargetPartition], r.WAL.Seq)
	}
	s.mu.Unlock()
	s.committed.Add(int64(len(recs)))
	return make([]int64, len(recs)), nil
}

// zzPerfACheckOnceInOrder fails unless s committed n records in all,
// each partition's in strictly increasing seq order: so every record
// exactly once, in WAL order per partition.
func zzPerfACheckOnceInOrder(t *testing.T, s *zzPerfASink, n int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for part, seqs := range s.seqs {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("partition %d: seq %d committed after seq %d (commit %d of %d): a duplicate or out of order",
					part, seqs[i], seqs[i-1], i, len(seqs))
			}
		}
		total += len(seqs)
	}
	if total != n {
		t.Fatalf("committed %d records, want each of %d exactly once", total, n)
	}
}

// zzPerfARunUntil runs d's Run loop until done reports true or limit
// passes, then stops it and waits for it to return, so the test may read
// d.state afterwards.
func zzPerfARunUntil(t *testing.T, d *ProduceDispatcher, done func() bool, limit time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { d.Run(ctx); close(stopped) }()
	deadline := time.Now().Add(limit)
	for !done() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-stopped
	if !done() {
		t.Fatalf("the Run loop did not finish within %v", limit)
	}
}

// Item 1 of the PCA perf follow-up. A lone hot destination fills its
// queue (perDestCap) at once, so the rest of its backlog up to the
// lookahead horizon is skipped; after each commit, the rescan read every
// one of those skipped records again (a decode each), placed a queue's
// worth and skipped the rest again: about 11 decodes per record. A
// rescan now stops once no destination it could place records for is
// left, so each skipped record is read again about once. Every record
// still commits exactly once, in WAL order. The test counts reads, not
// time, so a slow runner does not change its outcome.
func TestPerfARescanReadsEachSkippedRecordAboutOnce(t *testing.T) {
	const n = 131072
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 1)
	m := zzWP6Manager(t)
	zzPerfAFill(t, m, n, 1)
	sink := &zzPerfASink{delay: time.Millisecond}
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	zzPerfARunUntil(t, d, func() bool { return sink.committed.Load() >= n }, 3*time.Minute)

	zzPerfACheckOnceInOrder(t, sink, n)
	st := d.state
	if st.skipped != 0 || st.held != 0 || st.nextSeq != m.DurableProduceNext() {
		t.Fatalf("after the drain: skipped=%d held=%d checkpoint=%d durable=%d; want 0, 0 and the checkpoint at the durable frontier",
			st.skipped, st.held, st.nextSeq, m.DurableProduceNext())
	}
	t.Logf("%d skipped records read again for %d records (%.2f per record)", st.rereads, n, float64(st.rereads)/n)
	if limit := uint64(n) * 11 / 10; st.rereads > limit {
		t.Fatalf("the rescans read %d skipped records again for %d records (%.2f per record), want at most %d: each skipped record read again about once",
			st.rereads, n, float64(st.rereads)/n, limit)
	}
}
