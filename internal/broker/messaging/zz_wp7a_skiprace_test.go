package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// consume-local#3. Consumers of a partition whose frontier fell behind
// retention all fail their read and race to skip the gap. The first skip
// drops every reservation below the oldest offset, the others' included;
// their skips then found no reservation and the consume answered
// ErrHandleStale (HTTP 410) to requests that never held a handle.
func TestZZWP7aSkipRaceDoesNotFailConsume(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["t"] = topic.Topic{Name: "t", Partitions: 1, VisibilityTimeoutMs: 60_000, RetentionMs: 1}
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 2 * time.Millisecond, SegmentBytes: 256}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1000, MaxAckedAhead: 1000}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	ctx := context.Background()
	for i := range 40 {
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("t", "", 0, 5, fmt.Sprint("a", i))); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	log, err := e.logs.Get("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	log.SweepRetentionNow()
	for i := range 40 {
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("t", "", 0, 5, fmt.Sprint("b", i))); err != nil {
			t.Fatal(err)
		}
	}
	if log.OldestOffset() < 3 {
		t.Fatalf("retention reaped too little: oldest=%d", log.OldestOffset())
	}

	const iterations, consumers = 200, 8
	stale := 0
	p0 := 0
	for range iterations {
		if err := offsets.Init(ctx, "t", 0, -1); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		errs := make(chan error, consumers)
		for range consumers {
			go func() {
				<-start
				_, _, err := e.Consume(ctx, "t", ConsumeOpts{Partition: &p0})
				errs <- err
			}()
		}
		close(start)
		for range consumers {
			err := <-errs
			if errors.Is(err, consumer.ErrHandleStale) {
				stale++
			} else if err != nil {
				t.Fatalf("consume: %v", err)
			}
		}
	}
	if stale > 0 {
		t.Fatalf("plain consumes answered ErrHandleStale %d times", stale)
	}
}
