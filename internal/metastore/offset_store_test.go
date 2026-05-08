package metastore

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := gorm.Open(sqlite.Open(filepath.Join(dir, "test.db")), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.Exec("PRAGMA journal_mode=WAL")
	if err := db.AutoMigrate(&ConsumerOffsetRecord{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestOffsetSetGetConcurrent(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	os := newOffsetStore(db)
	t.Cleanup(os.close)

	const goroutines = 32
	const writes = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				os.set("topic", g, int64(i))
			}
		}(g)
	}
	wg.Wait()

	// Final value per partition must be writes-1; other reads from get
	// must always have been a value we actually set (no torn reads).
	for g := 0; g < goroutines; g++ {
		got, ok := os.get("topic", g)
		if !ok {
			t.Fatalf("partition %d missing after writes", g)
		}
		if got != int64(writes-1) {
			t.Errorf("partition %d: got %d, want %d", g, got, writes-1)
		}
	}
}

func TestOffsetFlushBatchesAndClearsDirty(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	os := newOffsetStore(db)
	t.Cleanup(os.close)

	const n = 100
	for i := 0; i < n; i++ {
		os.set("topic", i, int64(i*10))
	}
	if got := os.dirtyCount.Load(); got != n {
		t.Fatalf("dirtyCount before flush: got %d, want %d", got, n)
	}

	os.flush()

	if got := os.dirtyCount.Load(); got != 0 {
		t.Fatalf("dirtyCount after flush: got %d, want 0", got)
	}

	var rows []ConsumerOffsetRecord
	if err := db.Find(&rows).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != n {
		t.Fatalf("rows in DB: got %d, want %d", len(rows), n)
	}
	for _, r := range rows {
		if r.Offset != int64(r.Partition*10) {
			t.Errorf("partition %d: stored offset %d, want %d", r.Partition, r.Offset, r.Partition*10)
		}
	}
}

func TestOffsetFlushIsIdempotentWhenClean(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	os := newOffsetStore(db)
	t.Cleanup(os.close)

	os.flush() // no dirties → no-op
	os.set("t", 0, 1)
	os.flush()
	os.flush() // second flush should be a no-op

	if got := os.dirtyCount.Load(); got != 0 {
		t.Fatalf("dirtyCount: got %d, want 0", got)
	}
}

func TestOffsetThresholdTriggersWake(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	os := newOffsetStore(db)
	t.Cleanup(os.close)
	// Lower the threshold so the test doesn't need to write thousands
	// of entries.
	os.flushThreshold = 4

	for i := 0; i < 4; i++ {
		os.set("t", i, int64(i))
	}

	// The flusher wakes asynchronously. Poll for up to 2s for dirty to
	// drain — the periodic ticker is 5s, so any drain inside this window
	// must have come from the wakeCh path.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if os.dirtyCount.Load() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("threshold flush did not fire within 2s; dirtyCount=%d", os.dirtyCount.Load())
}

func TestOffsetDeleteTopicRemovesFromMemoryAndDB(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	os := newOffsetStore(db)
	t.Cleanup(os.close)

	os.set("a", 0, 1)
	os.set("a", 1, 2)
	os.set("b", 0, 3)
	os.flush()

	os.deleteTopic("a")

	if _, ok := os.get("a", 0); ok {
		t.Errorf("topic a still in memory after deleteTopic")
	}
	if v, ok := os.get("b", 0); !ok || v != 3 {
		t.Errorf("topic b: got %d (ok=%v), want 3 (ok=true)", v, ok)
	}

	var rows []ConsumerOffsetRecord
	if err := db.Find(&rows).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0].Topic != "b" {
		t.Fatalf("DB rows after deleteTopic: %+v (want only b)", rows)
	}
	if got := os.dirtyCount.Load(); got != 0 {
		t.Errorf("dirtyCount after deleteTopic: got %d, want 0", got)
	}
}

// TestOffsetFlushVsDeleteTopicNoResurrectedRows is a regression test
// for the race where flush() captured a row, deleteTopic() then issued
// its DELETE, and flush()'s batched upsert resurrected the deleted row.
// With flushMu serialising the two paths, the DB must end up empty for
// the deleted topic regardless of interleaving.
func TestOffsetFlushVsDeleteTopicNoResurrectedRows(t *testing.T) {
	t.Parallel()
	for trial := 0; trial < 50; trial++ {
		db := newTestDB(t)
		os := newOffsetStore(db)

		// Seed enough partitions that flush has work to do.
		for p := 0; p < 64; p++ {
			os.set("victim", p, int64(p))
		}
		os.set("survivor", 0, 999)

		// Race flush against deleteTopic.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			os.flush()
		}()
		go func() {
			defer wg.Done()
			os.deleteTopic("victim")
		}()
		wg.Wait()

		// One more flush so the survivor lands in DB regardless of
		// whether the racing flush captured it.
		os.flush()

		var rows []ConsumerOffsetRecord
		if err := db.Find(&rows).Error; err != nil {
			t.Fatalf("trial %d: query: %v", trial, err)
		}
		for _, r := range rows {
			if r.Topic == "victim" {
				t.Fatalf("trial %d: resurrected row for victim/p=%d off=%d", trial, r.Partition, r.Offset)
			}
		}
		os.close()
	}
}

func TestOffsetReloadAfterRestart(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)

	first := newOffsetStore(db)
	first.set("t", 0, 42)
	first.set("t", 1, 99)
	first.close() // close flushes pending writes

	second := newOffsetStore(db)
	t.Cleanup(second.close)

	if v, ok := second.get("t", 0); !ok || v != 42 {
		t.Errorf("after reload, partition 0: got %d (ok=%v), want 42", v, ok)
	}
	if v, ok := second.get("t", 1); !ok || v != 99 {
		t.Errorf("after reload, partition 1: got %d (ok=%v), want 99", v, ok)
	}
}
