package messaging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func commitFanoutFixture(t *testing.T, e *Engine, topicName string, partition, n int) {
	t.Helper()
	records := make([]ingress.ProduceRecord, 0, n)
	for i := range n {
		records = append(records, ingress.ProduceRecord{
			Topic:           topicName,
			Key:             fmt.Sprintf("key-%d", i%3),
			TargetPartition: partition,
			Payload:         fmt.Appendf(nil, `{"seq":%d}`, i),
		})
	}
	if _, err := e.CommitAcceptedProduceBatch(context.Background(), records); err != nil {
		t.Fatalf("CommitAcceptedProduceBatch: %v", err)
	}
}

func readOpts(from int64, maxRecords int, maxBytes int64, wait time.Duration) topic.FanoutReadOpts {
	return topic.FanoutReadOpts{FromOffset: from, MaxRecords: maxRecords, MaxBytes: maxBytes, Wait: wait}
}

func TestReadFanoutSlabReturnsKeyedRecords(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	before := time.Now().UnixMilli()
	commitFanoutFixture(t, e, "parent", 1, 5)

	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 1, readOpts(0, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	if len(slab.Records) != 5 || slab.NextOffset != 5 || slab.HighWatermark != 5 {
		t.Fatalf("slab = records=%d next=%d hwm=%d, want 5/5/5", len(slab.Records), slab.NextOffset, slab.HighWatermark)
	}
	if slab.DroppedBehind != 0 || slab.SkippedCorrupt != 0 || slab.BlockedUntilUnixMs != 0 {
		t.Fatalf("slab flags = %d/%d/%d, want 0/0/0", slab.DroppedBehind, slab.SkippedCorrupt, slab.BlockedUntilUnixMs)
	}
	after := time.Now().UnixMilli()
	for i, rec := range slab.Records {
		wantKey := fmt.Sprintf("key-%d", i%3)
		wantPayload := fmt.Sprintf(`{"seq":%d}`, i)
		if rec.Key != wantKey || string(rec.Payload) != wantPayload {
			t.Fatalf("record %d = (%q, %q), want (%q, %q)", i, rec.Key, rec.Payload, wantKey, wantPayload)
		}
		if rec.CommittedAtUnixMs < before || rec.CommittedAtUnixMs > after {
			t.Fatalf("record %d commit time %d outside [%d, %d]", i, rec.CommittedAtUnixMs, before, after)
		}
	}
}

func TestReadFanoutSlabTailInitAndCaps(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	commitFanoutFixture(t, e, "parent", 0, 10)

	// Tail init: no records, cursor anchored at the committed frontier.
	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, readOpts(topic.FanoutTailOffset, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("ReadFanoutSlab(tail): %v", err)
	}
	if len(slab.Records) != 0 || slab.NextOffset != 10 {
		t.Fatalf("tail slab = records=%d next=%d, want 0/10", len(slab.Records), slab.NextOffset)
	}

	// maxRecords caps the read.
	slab, err = e.ReadFanoutSlab(context.Background(), "parent", 0, readOpts(0, 4, 1<<20, 0))
	if err != nil {
		t.Fatalf("ReadFanoutSlab(cap records): %v", err)
	}
	if len(slab.Records) != 4 || slab.NextOffset != 4 {
		t.Fatalf("capped slab = records=%d next=%d, want 4/4", len(slab.Records), slab.NextOffset)
	}

	// maxBytes stops the read once payload bytes cross the bound.
	slab, err = e.ReadFanoutSlab(context.Background(), "parent", 0, readOpts(0, 100, 1, 0))
	if err != nil {
		t.Fatalf("ReadFanoutSlab(cap bytes): %v", err)
	}
	if len(slab.Records) != 1 {
		t.Fatalf("byte-capped slab records = %d, want 1", len(slab.Records))
	}

	// Reading at the frontier with no wait returns an empty slab.
	slab, err = e.ReadFanoutSlab(context.Background(), "parent", 0, readOpts(10, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("ReadFanoutSlab(frontier): %v", err)
	}
	if len(slab.Records) != 0 || slab.NextOffset != 10 {
		t.Fatalf("frontier slab = records=%d next=%d, want 0/10", len(slab.Records), slab.NextOffset)
	}
}

// The due gate: a MaxCommittedAt before the records' commit time blocks
// the read at the head, reports the blocking commit time, and never
// advances NextOffset past it — and it returns immediately instead of
// long-polling (new commits can't make the head due).
func TestReadFanoutSlabDueGate(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	commitFanoutFixture(t, e, "parent", 0, 3)

	slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, readOpts(0, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	committedAt := slab.Records[0].CommittedAtUnixMs

	// Cutoff before every record: fully blocked, no long-poll stall.
	opts := readOpts(0, 100, 1<<20, 5*time.Second)
	opts.MaxCommittedAt = committedAt - 1
	start := time.Now()
	slab, err = e.ReadFanoutSlab(context.Background(), "parent", 0, opts)
	if err != nil {
		t.Fatalf("ReadFanoutSlab(blocked): %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("blocked read long-polled for %v despite an undue head", elapsed)
	}
	if len(slab.Records) != 0 || slab.NextOffset != 0 {
		t.Fatalf("blocked slab = records=%d next=%d, want 0/0", len(slab.Records), slab.NextOffset)
	}
	if slab.BlockedUntilUnixMs != committedAt {
		t.Fatalf("BlockedUntilUnixMs = %d, want %d", slab.BlockedUntilUnixMs, committedAt)
	}

	// Cutoff at/after the commit time: everything is deliverable.
	opts.MaxCommittedAt = time.Now().UnixMilli()
	slab, err = e.ReadFanoutSlab(context.Background(), "parent", 0, opts)
	if err != nil {
		t.Fatalf("ReadFanoutSlab(due): %v", err)
	}
	if len(slab.Records) != 3 || slab.NextOffset != 3 || slab.BlockedUntilUnixMs != 0 {
		t.Fatalf("due slab = records=%d next=%d blocked=%d, want 3/3/0", len(slab.Records), slab.NextOffset, slab.BlockedUntilUnixMs)
	}
}

func TestReadFanoutSlabLongPollWakesOnCommit(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	commitFanoutFixture(t, e, "parent", 0, 1)

	done := make(chan topic.FanoutSlab, 1)
	go func() {
		slab, err := e.ReadFanoutSlab(context.Background(), "parent", 0, readOpts(1, 100, 1<<20, 5*time.Second))
		if err != nil {
			t.Errorf("ReadFanoutSlab: %v", err)
		}
		done <- slab
	}()

	time.Sleep(50 * time.Millisecond) // let the reader park on the notify channel
	commitFanoutFixture(t, e, "parent", 0, 2)

	select {
	case slab := <-done:
		if len(slab.Records) != 2 || slab.NextOffset != 3 {
			t.Fatalf("woken slab = records=%d next=%d, want 2/3", len(slab.Records), slab.NextOffset)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long-polled slab read did not wake on commit")
	}
}

func TestReadFanoutSlabValidation(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)

	if _, err := e.ReadFanoutSlab(context.Background(), "ghost", 0, readOpts(0, 10, 1024, 0)); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("missing topic error = %v, want %v", err, ErrTopicNotFound)
	}
	if _, err := e.ReadFanoutSlab(context.Background(), "parent", 99, readOpts(0, 10, 1024, 0)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("partition out of range error = %v, want %v", err, ErrInvalid)
	}
}

// fakeFanoutLog simulates retention aging out a prefix of the log.
type fakeFanoutLog struct {
	oldest  int64
	hwm     int64
	corrupt map[int64]bool
	// committedAt maps offset → commit time; unset offsets use offset
	// as their timestamp.
	committedAt map[int64]int64
}

func (f *fakeFanoutLog) HighWatermark() int64 { return f.hwm }
func (f *fakeFanoutLog) OldestOffset() int64  { return f.oldest }
func (f *fakeFanoutLog) ReadKeyed(offset int64) (string, int64, []byte, error) {
	if offset < f.oldest || offset >= f.hwm {
		return "", 0, nil, storage.ErrOffsetNotFound
	}
	if f.corrupt[offset] {
		return "", 0, nil, fmt.Errorf("%w: synthetic", errs.ErrCorruptRecord)
	}
	ts := offset
	if v, ok := f.committedAt[offset]; ok {
		ts = v
	}
	return fmt.Sprintf("k%d", offset), ts, fmt.Appendf(nil, `{"o":%d}`, offset), nil
}

func TestReadFanoutSlabOnceDropBehindAndCorruptSkip(t *testing.T) {
	e := &Engine{}

	// Cursor at 3, but offsets below 10 aged out: drop-behind reports
	// the 7 lost offsets and resumes at the oldest retained record.
	log := &fakeFanoutLog{oldest: 10, hwm: 15}
	slab, err := e.readFanoutSlabOnce(log, readOpts(3, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("readFanoutSlabOnce: %v", err)
	}
	if slab.DroppedBehind != 7 {
		t.Fatalf("DroppedBehind = %d, want 7", slab.DroppedBehind)
	}
	if len(slab.Records) != 5 || slab.NextOffset != 15 {
		t.Fatalf("slab = records=%d next=%d, want 5/15", len(slab.Records), slab.NextOffset)
	}

	// A corrupt record inside the retained range is skipped, counted,
	// and does not stall the read.
	log = &fakeFanoutLog{oldest: 0, hwm: 4, corrupt: map[int64]bool{1: true}}
	slab, err = e.readFanoutSlabOnce(log, readOpts(0, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("readFanoutSlabOnce(corrupt): %v", err)
	}
	if slab.SkippedCorrupt != 1 || len(slab.Records) != 3 || slab.NextOffset != 4 {
		t.Fatalf("slab = skipped=%d records=%d next=%d, want 1/3/4", slab.SkippedCorrupt, len(slab.Records), slab.NextOffset)
	}

	// The whole requested range aged out: pure drop-behind, no records.
	log = &fakeFanoutLog{oldest: 20, hwm: 20}
	slab, err = e.readFanoutSlabOnce(log, readOpts(5, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("readFanoutSlabOnce(all aged): %v", err)
	}
	if slab.DroppedBehind != 15 || slab.NextOffset != 20 || len(slab.Records) != 0 {
		t.Fatalf("slab = dropped=%d next=%d records=%d, want 15/20/0", slab.DroppedBehind, slab.NextOffset, len(slab.Records))
	}

	// A crash-regressed HWM below the cursor must never yield a
	// negative drop count: only offsets that were visible count.
	log = &fakeFanoutLog{oldest: 10, hwm: 2}
	slab, err = e.readFanoutSlabOnce(log, readOpts(5, 100, 1<<20, 0))
	if err != nil {
		t.Fatalf("readFanoutSlabOnce(regressed hwm): %v", err)
	}
	if slab.DroppedBehind != 0 || slab.NextOffset != 10 || len(slab.Records) != 0 {
		t.Fatalf("slab = dropped=%d next=%d records=%d, want 0/10/0", slab.DroppedBehind, slab.NextOffset, len(slab.Records))
	}

	// Due gate stops mid-slab at the first record past the cutoff
	// (commit times are monotonic per partition).
	log = &fakeFanoutLog{oldest: 0, hwm: 6}
	opts := readOpts(0, 100, 1<<20, 0)
	opts.MaxCommittedAt = 3 // offsets 0..3 due (ts == offset), 4+ blocked
	slab, err = e.readFanoutSlabOnce(log, opts)
	if err != nil {
		t.Fatalf("readFanoutSlabOnce(due gate): %v", err)
	}
	if len(slab.Records) != 4 || slab.NextOffset != 4 || slab.BlockedUntilUnixMs != 4 {
		t.Fatalf("slab = records=%d next=%d blocked=%d, want 4/4/4", len(slab.Records), slab.NextOffset, slab.BlockedUntilUnixMs)
	}
}

// Direct produce to a delayed child is rejected on both produce paths;
// the internal fan-out commit path stays open.
func TestDelayedChildRejectsDirectProduce(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["delayed"] = topic.Topic{
		Name: "delayed", Partitions: 3,
		Role: topic.RoleChild, Parent: "parent", FanoutDelayMs: 60_000,
	}
	e := newTestEngine(t, ms, nil, nil)

	if _, _, err := e.Produce(context.Background(), "delayed", "k", []byte(`{"v":1}`)); !errors.Is(err, errs.ErrDelayedChildProduce) {
		t.Fatalf("Produce error = %v, want %v", err, errs.ErrDelayedChildProduce)
	}

	// The fan-out commit path is internal and must not be gated.
	offsets, err := e.CommitAcceptedProduceBatch(context.Background(), []ingress.ProduceRecord{
		{Topic: "delayed", Key: "k", TargetPartition: 0, Payload: []byte(`{"v":1}`)},
	})
	if err != nil || len(offsets) != 1 {
		t.Fatalf("CommitAcceptedProduceBatch = (%v, %v), want one offset", offsets, err)
	}
}
