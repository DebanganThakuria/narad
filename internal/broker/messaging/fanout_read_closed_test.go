package messaging

// The fan-out closed-log path: a caught-up cursor read against an
// idle-evicted (closed) partition log must be served from the durable
// HWM file WITHOUT reopening the log — otherwise the once-a-second
// cursor poll of an attached-but-silent child would hold its parent
// open forever and idle eviction could never fire. Backlog, by
// contrast, must always open the log: correctness over eviction.

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func fanoutTestEngine(t *testing.T) *Engine {
	t.Helper()
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 1}
	return newTestEngine(t, ms, nil, nil)
}

func produceOne(t *testing.T, e *Engine, payload string) {
	t.Helper()
	if _, _, err := e.Produce(context.Background(), "parent", "", []byte(payload)); err != nil {
		t.Fatalf("Produce: %v", err)
	}
}

func TestFanoutReadCaughtUpOnClosedLogDoesNotReopen(t *testing.T) {
	e := fanoutTestEngine(t)
	produceOne(t, e, `1`)

	// Close the log the way idle eviction would (durable state synced).
	if err := e.logs.CloseTopic("parent"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}

	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, topic.FanoutReadOpts{
		FromOffset: 1, // caught up: HWM is 1
		MaxRecords: 100,
		MaxBytes:   1 << 20,
		Wait:       300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	if len(slab.Records) != 0 || slab.NextOffset != 1 || slab.HighWatermark != 1 {
		t.Fatalf("slab = %d records, next %d, hwm %d; want 0/1/1", len(slab.Records), slab.NextOffset, slab.HighWatermark)
	}
	if _, open := e.logs.Peek("parent", 0); open {
		t.Fatal("caught-up fan-out read reopened the closed log — idle eviction can never hold")
	}
}

func TestFanoutReadBacklogOnClosedLogOpensAndDrains(t *testing.T) {
	e := fanoutTestEngine(t)
	produceOne(t, e, `1`)
	produceOne(t, e, `2`)
	if err := e.logs.CloseTopic("parent"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}

	// Cursor behind the durable HWM: the read MUST open the log and
	// serve the backlog — a closed log never hides committed records.
	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, topic.FanoutReadOpts{
		FromOffset: 0,
		MaxRecords: 100,
		MaxBytes:   1 << 20,
	})
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	if len(slab.Records) != 2 || slab.NextOffset != 2 {
		t.Fatalf("slab = %d records, next %d; want 2 records, next 2", len(slab.Records), slab.NextOffset)
	}
	if _, open := e.logs.Peek("parent", 0); !open {
		t.Fatal("backlog read did not open the log")
	}
}

// A produce landing while a caught-up read sleeps against the closed
// log must be picked up within the same long-poll: the sleep re-checks
// Peek each slice, falls back to the open path, and serves the record.
func TestFanoutReadServesRecordProducedMidClosedWait(t *testing.T) {
	e := fanoutTestEngine(t)
	produceOne(t, e, `1`)
	if err := e.logs.CloseTopic("parent"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}

	type result struct {
		slab topic.FanoutSlab
		err  error
	}
	got := make(chan result, 1)
	go func() {
		slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, topic.FanoutReadOpts{
			FromOffset: 1,
			MaxRecords: 100,
			MaxBytes:   1 << 20,
			Wait:       5 * time.Second,
		})
		got <- result{slab, err}
	}()

	time.Sleep(400 * time.Millisecond) // let the read enter its closed-path sleep
	produceOne(t, e, `2`)              // reopens the log through Get

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("ReadFanoutSlab: %v", r.err)
		}
		if len(r.slab.Records) != 1 || r.slab.NextOffset != 2 {
			t.Fatalf("slab = %d records, next %d; want the mid-wait record (1 record, next 2)", len(r.slab.Records), r.slab.NextOffset)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("read never returned the record produced mid-wait")
	}
}
