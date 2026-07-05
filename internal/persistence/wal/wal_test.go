package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestLogAppendReplayAndRestart(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	id0, err := log.Append(context.Background(), []byte("first"))
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	id1, err := log.Append(context.Background(), []byte("second"))
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if id0.Seq != 0 || id1.Seq != 1 {
		t.Fatalf("seqs = %d,%d, want 0,1", id0.Seq, id1.Seq)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var got []Record
	if err := Replay(dir, 0, testOptions().MaxRecord, func(record Record) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	assertPayloads(t, got, "first", "second")

	reopened, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	id2, err := reopened.Append(context.Background(), []byte("third"))
	if err != nil {
		t.Fatalf("Append(third) error = %v", err)
	}
	if id2.Seq != 2 {
		t.Fatalf("third seq = %d, want 2", id2.Seq)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}
}

func TestLogCompactBeforeDeletesCompleteSegments(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.SegmentBytes = frameHeaderSize + 8

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id0, err := log.Append(context.Background(), []byte("first"))
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	id1, err := log.Append(context.Background(), []byte("second"))
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if id0.SegmentBase == id1.SegmentBase {
		t.Fatalf("expected segment roll, both records in segment %d", id0.SegmentBase)
	}
	if got := segmentCount(t, dir); got != 2 {
		t.Fatalf("segments before compact = %d, want 2", got)
	}

	if err := log.CompactBefore(id1.Seq); err != nil {
		t.Fatalf("CompactBefore() error = %v", err)
	}
	if got := segmentCount(t, dir); got != 1 {
		t.Fatalf("segments after compact = %d, want 1", got)
	}
	if _, err := os.Stat(segmentPath(dir, id0.SegmentBase)); !os.IsNotExist(err) {
		t.Fatalf("old segment stat error = %v, want not exist", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLogAppendWakesSyncBeforeTimer(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.SyncInterval = time.Hour

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer log.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := log.Append(ctx, []byte("first")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func TestReplayIgnoresTrailingPartialFrame(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id, err := log.Append(context.Background(), []byte("first"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(segmentPath(dir, id.SegmentBase), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.Write([]byte{0x4e, 0x57}); err != nil {
		t.Fatalf("write partial frame error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close partial writer error = %v", err)
	}

	var got []Record
	if err := Replay(dir, 0, testOptions().MaxRecord, func(record Record) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	assertPayloads(t, got, "first")

	reopened, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	data, err := os.ReadFile(segmentPath(dir, id.SegmentBase))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if bytes.HasSuffix(data, []byte{0x4e, 0x57}) {
		t.Fatal("partial frame was not truncated on reopen")
	}
}

func TestLogConcurrentAppendBatchesAndReplays(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.SegmentBytes = 1 << 20
	opts.SyncInterval = time.Millisecond

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	const records = 64
	var wg sync.WaitGroup
	errs := make(chan error, records)
	ids := make(chan RecordID, records)
	for i := range records {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			id, err := log.Append(ctx, fmt.Appendf(nil, "record-%02d", i))
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		})
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	seenSeq := make(map[uint64]struct{}, records)
	for id := range ids {
		seenSeq[id.Seq] = struct{}{}
	}
	if len(seenSeq) != records {
		t.Fatalf("unique seqs = %d, want %d", len(seenSeq), records)
	}

	seenPayloads := make(map[string]struct{}, records)
	if err := Replay(dir, 0, opts.MaxRecord, func(record Record) error {
		seenPayloads[string(record.Payload)] = struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(seenPayloads) != records {
		t.Fatalf("replayed payloads = %d, want %d", len(seenPayloads), records)
	}
	for i := range records {
		payload := fmt.Sprintf("record-%02d", i)
		if _, ok := seenPayloads[payload]; !ok {
			t.Fatalf("missing payload %q", payload)
		}
	}
}

func TestLogConcurrentAppendWithSegmentRolls(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.SegmentBytes = frameHeaderSize + 12
	opts.SyncInterval = time.Hour

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	const records = 64
	var wg sync.WaitGroup
	errs := make(chan error, records)
	for i := range records {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := log.Append(ctx, fmt.Appendf(nil, "record-%02d", i))
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var replayed int
	if err := Replay(dir, 0, opts.MaxRecord, func(Record) error {
		replayed++
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed != records {
		t.Fatalf("replayed records = %d, want %d", replayed, records)
	}
}

func TestSyncLockedRefusesWriteAfterLatchedFailure(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Simulate flushSync latching a torn-write failure under fileOps while
	// a roll-path syncLocked was waiting on fileOps: the latch must stop
	// the later batch from being written on top of the torn region.
	tornErr := errors.New("torn write")
	log.fileOps.Lock()
	log.writeFailed = tornErr
	log.fileOps.Unlock()

	log.mu.Lock()
	_, _, err = log.appendLocked([]byte("later"))
	if err != nil {
		log.mu.Unlock()
		t.Fatalf("appendLocked() error = %v", err)
	}
	batch, syncErr := log.syncLocked()
	log.mu.Unlock()
	completeBatch(batch, syncErr)
	if !errors.Is(syncErr, tornErr) {
		t.Fatalf("syncLocked() error = %v, want %v", syncErr, tornErr)
	}

	// Nothing may have been written to the file.
	info, err := os.Stat(segmentPath(dir, 0))
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("segment size = %d after refused batch, want 0", info.Size())
	}
	if err := log.Close(); err == nil {
		t.Fatal("Close() error = nil, want latched sync error")
	}
}

func TestOpenTruncatesCorruptTailInLastSegment(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id, err := log.Append(context.Background(), []byte("first"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Append a full-size garbage frame (bad magic) to the active segment.
	file, err := os.OpenFile(segmentPath(dir, id.SegmentBase), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.Write(bytes.Repeat([]byte{0xff}, frameHeaderSize+8)); err != nil {
		t.Fatalf("write garbage error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close garbage writer error = %v", err)
	}

	reopened, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("reopen with corrupt tail error = %v", err)
	}
	id2, err := reopened.Append(context.Background(), []byte("second"))
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if id2.Seq != 1 {
		t.Fatalf("second seq = %d, want 1", id2.Seq)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close() error = %v", err)
	}

	var got []Record
	if err := Replay(dir, 0, testOptions().MaxRecord, func(record Record) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	assertPayloads(t, got, "first", "second")
}

func TestOpenFailsOnMidFileCorruptionInLastSegment(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id, err := log.Append(context.Background(), []byte("first"))
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("second")); err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Flip a payload byte of the first frame: mid-file corruption of
	// already-fsynced data, with a valid acked frame after it.
	path := segmentPath(dir, id.SegmentBase)
	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.WriteAt([]byte{0xff}, frameHeaderSize); err != nil {
		t.Fatalf("corrupt payload error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupter error = %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() before reopen error = %v", err)
	}

	// A later valid frame exists, so this is not a torn tail: Open must
	// fail loudly instead of truncating away the acked second record.
	reopened, err := Open(dir, testOptions())
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want mid-file corruption error")
	}

	// Nothing may have been destroyed.
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() after reopen error = %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("segment size = %d after failed Open, want %d (untouched)", after.Size(), before.Size())
	}
}

func TestOpenFailsOnCorruptNonLastSegment(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.SegmentBytes = frameHeaderSize + 8

	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	id0, err := log.Append(context.Background(), []byte("first"))
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	id1, err := log.Append(context.Background(), []byte("second"))
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if id0.SegmentBase == id1.SegmentBase {
		t.Fatalf("expected segment roll, both records in segment %d", id0.SegmentBase)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Corrupt the magic of the first (non-last) segment.
	file, err := os.OpenFile(segmentPath(dir, id0.SegmentBase), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.WriteAt([]byte{0, 0, 0, 0}, 0); err != nil {
		t.Fatalf("corrupt magic error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupter error = %v", err)
	}

	reopened, err := Open(dir, opts)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("Open() error = nil, want corrupt segment error")
	}
}

func TestReplayFromCursorStartsAfterRecord(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, testOptions())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("first")); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("second")); err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("third")); err != nil {
		t.Fatalf("Append(third) error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var cursor Cursor
	if err := ReplayFromCursor(dir, Cursor{}, testOptions().MaxRecord, func(record Record, next Cursor) error {
		if string(record.Payload) == "first" {
			cursor = next
		}
		return nil
	}); err != nil {
		t.Fatalf("initial ReplayFromCursor() error = %v", err)
	}
	if cursor.Seq != 1 || cursor.Offset <= 0 {
		t.Fatalf("cursor after first = %+v, want seq 1 and positive offset", cursor)
	}

	var got []Record
	if err := ReplayFromCursor(dir, cursor, testOptions().MaxRecord, func(record Record, _ Cursor) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("ReplayFromCursor(cursor) error = %v", err)
	}
	assertPayloads(t, got, "second", "third")
}

func testOptions() Options {
	return Options{
		SegmentBytes: 1024,
		SyncInterval: time.Hour,
		MaxRecord:    1024,
	}
}

func assertPayloads(t *testing.T, records []Record, want ...string) {
	t.Helper()
	if len(records) != len(want) {
		t.Fatalf("records = %d, want %d", len(records), len(want))
	}
	for i, record := range records {
		if string(record.Payload) != want[i] {
			t.Fatalf("record %d payload = %q, want %q", i, record.Payload, want[i])
		}
	}
}

func segmentCount(t *testing.T, dir string) int {
	t.Helper()
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments() error = %v", err)
	}
	return len(segments)
}
