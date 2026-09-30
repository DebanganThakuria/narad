package messaging

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// A closed parent partition whose hwm file holds no boundary (a crash
// image nothing has opened yet, or a Close whose hwm write failed) is
// not caught up: its records are backlog, so the fan-out read opens the
// log and drains them instead of sleeping out its wait on a boundary of
// 0.
func TestFanoutReadCrashImageOnClosedLogOpensAndDrains(t *testing.T) {
	e := fanoutTestEngine(t)
	produceOne(t, e, `1`)
	produceOne(t, e, `2`)
	if err := e.logs.CloseTopic("parent"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	if err := os.WriteFile(partitionHWMPath(e.logs.DataDir(), "parent", 0), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, topic.FanoutReadOpts{
		FromOffset: 0,
		MaxRecords: 100,
		MaxBytes:   1 << 20,
		Wait:       300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	if len(slab.Records) != 2 || slab.NextOffset != 2 || slab.HighWatermark != 2 {
		t.Fatalf("slab = %d records, next %d, hwm %d; want the 2 records the crash image holds", len(slab.Records), slab.NextOffset, slab.HighWatermark)
	}
	if _, open := e.logs.Peek("parent", 0); !open {
		t.Fatal("crash-image read did not open the log")
	}
}

// A closed partition with an empty hwm file but no record bytes (every
// record aged out) is caught up at the offset its newest segment is
// named for, answered without an open.
func TestFanoutReadClosedLogWithoutRecordBytesStaysClosed(t *testing.T) {
	e := fanoutTestEngine(t)
	const base = int64(4242)
	dir := storage.TopicPartitionDir(e.logs.DataDir(), "parent", 0)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteSegmentFile(dir, base, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "hwm"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, topic.FanoutReadOpts{
		FromOffset: base,
		MaxRecords: 100,
		MaxBytes:   1 << 20,
	})
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	if len(slab.Records) != 0 || slab.NextOffset != base || slab.HighWatermark != base {
		t.Fatalf("slab = %d records, next %d, hwm %d; want caught up at %d", len(slab.Records), slab.NextOffset, slab.HighWatermark, base)
	}
	if _, open := e.logs.Peek("parent", 0); open {
		t.Fatal("a read with no record bytes to drain opened the closed log")
	}
}

// Cursor stats never open a log, so a closed crash-image partition,
// whose boundary only an open recovers, is left out instead of being
// reported at 0 below its cursor; the listing then says its lag is
// incomplete.
func TestFanoutCursorStatsSkipsClosedCrashImage(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 2, Role: topic.RoleParent, Children: []string{"child"}}
	e := newTestEngine(t, ms, nil, nil)
	for p := range 2 {
		commitFanoutFixture(t, e, "parent", p, 5)
		dir := storage.TopicPartitionDir(e.logs.DataDir(), "parent", p)
		if err := storage.WriteFanoutCursorIfPartitionDirExists(dir, "child", storage.FanoutCursor{Epoch: "e", NextOffset: 2}); err != nil {
			t.Fatalf("WriteFanoutCursor: %v", err)
		}
	}
	if err := e.logs.CloseTopic("parent"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	if err := os.WriteFile(partitionHWMPath(e.logs.DataDir(), "parent", 1), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	stats, err := e.FanoutCursorStats(context.Background(), "parent")
	if err != nil {
		t.Fatalf("FanoutCursorStats: %v", err)
	}
	if len(stats) != 1 || stats[0].Partition != 0 || stats[0].NextOffset != 2 || stats[0].HighWatermark != 5 {
		t.Fatalf("stats = %+v, want only child/0 next=2 hwm=5", stats)
	}
	for p := range 2 {
		if _, open := e.logs.Peek("parent", p); open {
			t.Fatalf("FanoutCursorStats opened the closed log for parent/%d", p)
		}
	}
}
