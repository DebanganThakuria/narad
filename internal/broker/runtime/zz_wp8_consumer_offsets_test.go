package runtime

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestWP8CommitterNeverWritesALowerFrontier pins that a frontier
// commit arriving after a higher one was already written (acks call
// Commit after dropping the shard lock, so two advances can reach the
// committer in reverse order across a flush) does not move
// consumer.offset backwards.
func TestWP8CommitterNeverWritesALowerFrontier(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := zzWP23ManualCommitter(dataDir)
	c.Commit("t", 0, 11)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	c.Commit("t", 0, 10)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := storage.ReadConsumerOffset(dir); err != nil || !ok || got != 11 {
		t.Fatalf("consumer.offset = %d (ok %v err %v), want 11: a late lower commit overwrote the frontier", got, ok, err)
	}
}

// wp8Caps is a caps resolver for committer wiring tests.
func wp8Caps(context.Context, string) (consumer.Caps, error) {
	return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
}

// wp8RecoveredInFlight builds an InFlight that recovers from dataDir
// exactly the way cmd/narad's serve wiring does.
func wp8RecoveredInFlight(dataDir string) *consumer.InFlight {
	f := consumer.NewInFlight(wp8Caps, nil)
	f.SetCommittedRecovery(func(topicName string, p int) (int64, bool) {
		v, ok, _ := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, topicName, p))
		return v, ok
	})
	f.SetAheadRecovery(func(topicName string, p int) (int64, []int64, bool) {
		rec, ok, _ := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, topicName, p))
		return rec.Committed, rec.Offsets, ok
	})
	return f
}

// TestWP8GracefulRestartKeepsTheAckedFrontier is the end-to-end form:
// production wiring, the ack goroutine for offset 0 parked between the
// shard unlock and the committer call, the ack for offset 1 flushed
// first. After a graceful Close and a recovery like the broker's, the
// first reserve must not hand out an acked offset.
func TestWP8GracefulRestartKeepsTheAckedFrontier(t *testing.T) {
	dataDir := t.TempDir()
	mustCreatePartitionDir(t, dataDir, "t", 0)
	c := zzWP23ManualCommitter(dataDir)
	park, parked := make(chan struct{}), make(chan struct{})
	var first sync.Once
	onCommit := func(topicName string, p int, off int64) {
		if off == 0 {
			first.Do(func() { close(parked); <-park })
		}
		c.Commit(topicName, p, off)
	}
	f := consumer.NewInFlight(wp8Caps, onCommit)
	c.SetAheadSource(f.AheadSnapshot)
	ctx := context.Background()
	r0, err := f.ReserveNext(ctx, "t", 0, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := f.ReserveNext(ctx, "t", 0, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	done0 := make(chan error, 1)
	go func() { done0 <- f.CommitHandle("t", 0, r0.Offset, r0.Nonce) }()
	<-parked
	if err := f.CommitHandle("t", 0, r1.Offset, r1.Nonce); err != nil {
		t.Fatal(err)
	}
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	close(park)
	if err := <-done0; err != nil {
		t.Fatal(err)
	}
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	mem, _ := f.CommittedOffset("t", 0)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	next, err := wp8RecoveredInFlight(dataDir).ReserveNext(ctx, "t", 0, time.Minute, 5)
	if err != nil {
		t.Fatal(err)
	}
	if next.Offset <= mem {
		t.Fatalf("first reserve after a graceful restart = %d, want > %d (acked frontier)", next.Offset, mem)
	}
}

// TestWP8CommitterStressRecoversTheAckedFrontier runs production
// wiring with concurrent acks in near order and a short flush interval,
// then recovers like the broker after a graceful Close: the recovered
// frontier must never be below the in-memory one.
func TestWP8CommitterStressRecoversTheAckedFrontier(t *testing.T) {
	const rounds, n, k = 40, 400, 8
	for range rounds {
		dataDir := t.TempDir()
		dir := mustCreatePartitionDir(t, dataDir, "t", 0)
		c := NewConsumerOffsetCommitter(dataDir, time.Millisecond, nil)
		f := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: n + 10, MaxAckedAhead: n + 10}, nil
		}, c.Commit)
		c.SetAheadSource(f.AheadSnapshot)
		ctx := context.Background()
		type rsv struct{ off, nonce int64 }
		res := make([]rsv, n)
		for i := range n {
			r, err := f.ReserveNext(ctx, "t", 0, time.Minute, n)
			if err != nil || !r.Reserved {
				t.Fatalf("reserve %d: %+v %v", i, r, err)
			}
			res[i] = rsv{r.Offset, r.Nonce}
		}
		var wg sync.WaitGroup
		var failed atomic.Bool
		start := make(chan struct{})
		for g := range k {
			wg.Go(func() {
				<-start
				for i := g; i < n; i += k {
					if err := f.CommitHandle("t", 0, res[i].off, res[i].nonce); err != nil {
						failed.Store(true)
					}
				}
			})
		}
		close(start)
		wg.Wait()
		if failed.Load() {
			t.Fatal("an ack failed")
		}
		mem, _ := f.CommittedOffset("t", 0)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		off, _, _ := storage.ReadConsumerOffset(dir)
		if off != mem {
			t.Fatalf("consumer.offset after graceful Close = %d, want the acked frontier %d", off, mem)
		}
		if _, err := os.Stat(dir); err != nil {
			t.Fatal(err)
		}
	}
}
