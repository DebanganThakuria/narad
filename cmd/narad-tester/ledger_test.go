package main

import (
	"fmt"
	"testing"
	"time"
)

func TestLedgerConsumeLifecycle(t *testing.T) {
	t.Parallel()

	l := newLedger()
	now := time.Unix(100, 0)
	rec := ledgerRecord{
		ID:               "msg-1",
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         1,
		Key:              "key-1",
		ProducedAtUnixMs: now.Add(time.Second).UnixMilli(),
		Partition:        2,
		Offset:           99,
	}
	if err := l.recordProduced(rec); err != nil {
		t.Fatalf("record produced: %v", err)
	}

	stats := l.statsAndCompact(now.Add(2*time.Second), time.Minute)
	if stats.ProducedOutstanding != 1 {
		t.Fatalf("produced outstanding = %d, want 1", stats.ProducedOutstanding)
	}
	if stats.Pending != 0 {
		t.Fatalf("pending = %d, want 0", stats.Pending)
	}

	got, err := l.markConsumed(messageFromRecord(rec), rec.Topic, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("mark consumed: %v", err)
	}
	if got.Outcome != consumeOutcomeValid {
		t.Fatalf("outcome = %q, want %q", got.Outcome, consumeOutcomeValid)
	}
	if got.ProducedAtUnixMs != rec.ProducedAtUnixMs {
		t.Fatalf("produced at = %d, want %d", got.ProducedAtUnixMs, rec.ProducedAtUnixMs)
	}

	got, err = l.markConsumed(messageFromRecord(rec), rec.Topic, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("mark duplicate consumed: %v", err)
	}
	if got.Outcome != consumeOutcomeDuplicate {
		t.Fatalf("outcome = %q, want %q", got.Outcome, consumeOutcomeDuplicate)
	}

	stats = l.statsAndCompact(now.Add(5*time.Second), time.Minute)
	if stats.ProducedOutstanding != 0 {
		t.Fatalf("produced outstanding after consume = %d, want 0", stats.ProducedOutstanding)
	}
	if stats.ConsumedSequences != 1 {
		t.Fatalf("consumed sequences = %d, want 1", stats.ConsumedSequences)
	}
}

func TestLedgerClassifiesDuplicateWithoutMarkerWindow(t *testing.T) {
	t.Parallel()

	l := newLedger()
	now := time.Unix(100, 0)
	rec := ledgerRecord{
		ID:               "msg-1",
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         1,
		Key:              "key-1",
		ProducedAtUnixMs: now.UnixMilli(),
		Partition:        0,
		Offset:           1,
	}
	if err := l.recordProduced(rec); err != nil {
		t.Fatalf("record produced: %v", err)
	}
	if _, err := l.markConsumed(messageFromRecord(rec), rec.Topic, now.Add(time.Second)); err != nil {
		t.Fatalf("mark consumed: %v", err)
	}

	got, err := l.markConsumed(messageFromRecord(rec), rec.Topic, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("mark duplicate consumed: %v", err)
	}
	if got.Outcome != consumeOutcomeDuplicate {
		t.Fatalf("outcome = %q, want %q", got.Outcome, consumeOutcomeDuplicate)
	}
}

func TestLedgerStatsMissing(t *testing.T) {
	t.Parallel()

	l := newLedger()
	now := time.Unix(100, 0)
	rec := ledgerRecord{
		ID:               "produced",
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         1,
		Key:              "key-1",
		ProducedAtUnixMs: now.UnixMilli(),
		Partition:        0,
		Offset:           1,
	}
	if err := l.recordProduced(rec); err != nil {
		t.Fatalf("record produced: %v", err)
	}

	stats := l.statsAndCompact(now.Add(10*time.Minute), time.Minute)
	if stats.Pending != 0 {
		t.Fatalf("pending = %d, want 0", stats.Pending)
	}
	if stats.ProducedOutstanding != 1 {
		t.Fatalf("produced outstanding = %d, want 1", stats.ProducedOutstanding)
	}
	if stats.Missing != 1 {
		t.Fatalf("missing = %d, want 1", stats.Missing)
	}
	if stats.OldestProducedAge <= 0 {
		t.Fatalf("oldest produced age = %f, want > 0", stats.OldestProducedAge)
	}
}

func TestLedgerDeleteProducedRemovesOutstanding(t *testing.T) {
	t.Parallel()

	l := newLedger()
	now := time.Unix(100, 0)
	rec := ledgerRecord{
		ID:               "msg-1",
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         1,
		Key:              "key-1",
		ProducedAtUnixMs: now.UnixMilli(),
		Partition:        -1,
		Offset:           -1,
	}
	if err := l.recordProduced(rec); err != nil {
		t.Fatalf("record produced: %v", err)
	}
	if !l.deleteProduced(rec.ID) {
		t.Fatal("deleteProduced() = false, want true")
	}
	if l.deleteProduced(rec.ID) {
		t.Fatal("second deleteProduced() = true, want false")
	}
	stats := l.statsAndCompact(now.Add(time.Second), time.Minute)
	if stats.ProducedOutstanding != 0 {
		t.Fatalf("produced outstanding = %d, want 0", stats.ProducedOutstanding)
	}
}

func TestLedgerConsumesPreRecordedProduceBeforeLocationUpdate(t *testing.T) {
	t.Parallel()

	l := newLedger()
	now := time.Unix(100, 0)
	rec := ledgerRecord{
		ID:               "msg-1",
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         1,
		Key:              "key-1",
		ProducedAtUnixMs: now.UnixMilli(),
		Partition:        -1,
		Offset:           -1,
	}
	if err := l.recordProduced(rec); err != nil {
		t.Fatalf("record produced: %v", err)
	}

	got, err := l.markConsumed(messageFromRecord(rec), rec.Topic, now.Add(time.Second))
	if err != nil {
		t.Fatalf("mark consumed: %v", err)
	}
	if got.Outcome != consumeOutcomeValid {
		t.Fatalf("outcome = %q, want %q", got.Outcome, consumeOutcomeValid)
	}
	if l.updateProducedLocation(rec.ID, 2, 99) {
		t.Fatal("updateProducedLocation() after consume = true, want false")
	}

	stats := l.statsAndCompact(now.Add(2*time.Second), time.Minute)
	if stats.ProducedOutstanding != 0 {
		t.Fatalf("produced outstanding = %d, want 0", stats.ProducedOutstanding)
	}
	if stats.ConsumedSequences != 1 {
		t.Fatalf("consumed sequences = %d, want 1", stats.ConsumedSequences)
	}
}

func TestLedgerConsumedSequenceTrackingDoesNotEvict(t *testing.T) {
	t.Parallel()

	l := newLedger()
	now := time.Unix(100, 0)
	first, second := idsInSameShard(t)

	for i, id := range []string{first, second} {
		rec := ledgerRecord{
			ID:               id,
			RunID:            "run-1",
			Topic:            "topic-1",
			Sequence:         int64(i + 1),
			Key:              id + "-key",
			ProducedAtUnixMs: now.Add(time.Duration(i) * time.Second).UnixMilli(),
			Partition:        0,
			Offset:           int64(i),
		}
		if err := l.recordProduced(rec); err != nil {
			t.Fatalf("record produced %s: %v", id, err)
		}
		got, err := l.markConsumed(messageFromRecord(rec), rec.Topic, now.Add(time.Duration(i+1)*time.Second))
		if err != nil {
			t.Fatalf("mark consumed %s: %v", id, err)
		}
		if got.Outcome != consumeOutcomeValid {
			t.Fatalf("outcome for %s = %q, want %q", id, got.Outcome, consumeOutcomeValid)
		}
	}

	firstMsg := testerMessage{
		ID:               first,
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         1,
		Key:              first + "-key",
		ProducedAtUnixMs: now.UnixMilli(),
	}
	got, err := l.markConsumed(firstMsg, "topic-1", now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("mark duplicate first id: %v", err)
	}
	if got.Outcome != consumeOutcomeDuplicate {
		t.Fatalf("first outcome = %q, want %q", got.Outcome, consumeOutcomeDuplicate)
	}

	secondMsg := testerMessage{
		ID:               second,
		RunID:            "run-1",
		Topic:            "topic-1",
		Sequence:         2,
		Key:              second + "-key",
		ProducedAtUnixMs: now.Add(time.Second).UnixMilli(),
	}
	got, err = l.markConsumed(secondMsg, "topic-1", now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("mark duplicate second id: %v", err)
	}
	if got.Outcome != consumeOutcomeDuplicate {
		t.Fatalf("second outcome = %q, want %q", got.Outcome, consumeOutcomeDuplicate)
	}
}

func messageFromRecord(rec ledgerRecord) testerMessage {
	return testerMessage{
		ID:               rec.ID,
		RunID:            rec.RunID,
		Topic:            rec.Topic,
		Sequence:         rec.Sequence,
		Key:              rec.Key,
		ProducedAtUnixMs: rec.ProducedAtUnixMs,
	}
}

func idsInSameShard(t *testing.T) (string, string) {
	t.Helper()

	byShard := make(map[uint32]string)
	for i := 0; ; i++ {
		id := fmt.Sprintf("msg-%d", i)
		shard := ledgerShardIndex(id)
		if existing, ok := byShard[shard]; ok {
			return existing, id
		}
		byShard[shard] = id
	}
}
