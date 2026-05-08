package metastore

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultFlushInterval  = 5 * time.Second
	defaultFlushThreshold = 1024
	flushBatchSize        = 256
)

// offsetStore keeps consumer offsets in memory and flushes dirty entries
// to SQLite. Both reads and writes are lock-free: each entry holds its
// value and dirty bit as atomics.
//
// Flushes fire on a periodic tick OR when dirtyCount crosses
// flushThreshold (whichever comes first). On deleteTopic we remove the
// in-memory entries AND issue a synchronous DELETE so SQLite stays in
// sync.
type offsetStore struct {
	offsets sync.Map // "topic:partition" -> *offsetEntry

	dirtyCount     atomic.Int64
	wakeCh         chan struct{} // capacity 1; producers send non-blocking
	flushThreshold int64

	// flushMu serialises flush() against deleteTopic(). Without it, a
	// flush can capture a row, deleteTopic can issue its DELETE, and
	// then flush's batched upsert can resurrect the deleted row in the
	// DB. Producers and readers don't take this lock; only the two
	// background-writer paths do.
	flushMu sync.Mutex

	db     *gorm.DB
	stopCh chan struct{}
	doneCh chan struct{}
}

type offsetEntry struct {
	val       atomic.Int64
	dirty     atomic.Bool
	topic     string
	partition int
}

func newOffsetStore(db *gorm.DB) *offsetStore {
	os := &offsetStore{
		wakeCh:         make(chan struct{}, 1),
		flushThreshold: defaultFlushThreshold,
		db:             db,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	os.loadAll()
	go os.flushLoop()
	return os
}

func (os *offsetStore) loadAll() {
	var records []ConsumerOffsetRecord
	if err := os.db.Find(&records).Error; err != nil {
		return
	}
	for _, r := range records {
		e := &offsetEntry{topic: r.Topic, partition: r.Partition}
		e.val.Store(r.Offset)
		os.offsets.Store(offsetKey(r.Topic, r.Partition), e)
	}
}

func (os *offsetStore) get(topic string, partition int) (int64, bool) {
	v, ok := os.offsets.Load(offsetKey(topic, partition))
	if !ok {
		return 0, false
	}
	return v.(*offsetEntry).val.Load(), true
}

func (os *offsetStore) set(topic string, partition int, offset int64) {
	key := offsetKey(topic, partition)
	v, ok := os.offsets.Load(key)
	if !ok {
		fresh := &offsetEntry{topic: topic, partition: partition}
		fresh.val.Store(offset)
		actual, loaded := os.offsets.LoadOrStore(key, fresh)
		if !loaded {
			// Newly inserted — mark dirty exactly once.
			fresh.dirty.Store(true)
			os.bumpDirty()
			return
		}
		v = actual
	}
	e := v.(*offsetEntry)
	e.val.Store(offset)
	if e.dirty.CompareAndSwap(false, true) {
		os.bumpDirty()
	}
}

func (os *offsetStore) bumpDirty() {
	if os.dirtyCount.Add(1) >= os.flushThreshold {
		// Non-blocking signal — multiple crossings collapse into one wake.
		select {
		case os.wakeCh <- struct{}{}:
		default:
		}
	}
}

// deleteTopic eagerly removes every entry for topic from memory and
// issues a synchronous DELETE so SQLite is consistent immediately.
// Called from SQLiteStore.DeleteTopic.
//
// flushMu is held for the entire range+DELETE so any concurrent flush
// either sees all of this topic's entries (and writes them out before
// we DELETE) or none (we drop them before the flush ranges) — never a
// half-and-half where flush's batch resurrects rows after our DELETE.
func (os *offsetStore) deleteTopic(topic string) {
	os.flushMu.Lock()
	defer os.flushMu.Unlock()

	prefix := topic + ":"
	os.offsets.Range(func(k, v any) bool {
		if !strings.HasPrefix(k.(string), prefix) {
			return true
		}
		e := v.(*offsetEntry)
		if e.dirty.CompareAndSwap(true, false) {
			os.dirtyCount.Add(-1)
		}
		os.offsets.Delete(k)
		return true
	})
	os.db.Where("topic = ?", topic).Delete(&ConsumerOffsetRecord{})
}

func (os *offsetStore) flushLoop() {
	defer close(os.doneCh)
	ticker := time.NewTicker(defaultFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			os.flush()
		case <-os.wakeCh:
			os.flush()
		case <-os.stopCh:
			os.flush()
			return
		}
	}
}

func (os *offsetStore) flush() {
	if os.dirtyCount.Load() == 0 {
		return
	}
	os.flushMu.Lock()
	defer os.flushMu.Unlock()

	// Snapshot dirty entries by CAS-clearing the dirty bit. A concurrent
	// set() that races with this CAS will re-mark the entry on the next
	// write — at worst we pick up a slightly newer offset on the next
	// flush, which is fine for at-least-once consumer semantics.
	rows := make([]ConsumerOffsetRecord, 0, 64)
	os.offsets.Range(func(_, v any) bool {
		e := v.(*offsetEntry)
		if e.dirty.CompareAndSwap(true, false) {
			os.dirtyCount.Add(-1)
			rows = append(rows, ConsumerOffsetRecord{
				Topic:     e.topic,
				Partition: e.partition,
				Offset:    e.val.Load(),
			})
		}
		return true
	})
	if len(rows) == 0 {
		return
	}
	os.db.Clauses(clause.OnConflict{UpdateAll: true}).
		CreateInBatches(rows, flushBatchSize)
}

func (os *offsetStore) close() {
	close(os.stopCh)
	<-os.doneCh
}

func offsetKey(topic string, partition int) string {
	return topic + ":" + strconv.Itoa(partition)
}
