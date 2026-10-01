package wal

import (
	"bytes"
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// An append that needs a roll while the sync loop's group-commit write
// is failing must not seal the prepared segment over that write. The
// roller waits out the flush on fileOps and finds its buffer empty (the
// flush detached it), so before the writeFailed check in rollLocked it
// trimmed the segment to a size that counted the unwritten frames and
// sealed it by creating the successor: a zero tail in a sealed segment,
// which Open refuses as corruption. Both appends must fail, and the
// directory must reopen and replay every acked record.
func TestZZShipRollOverFailedFlushKeepsTheWALOpenable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := zzWP5PreallocOptions()
	opts.SyncInterval = time.Hour
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	n := zzWP5AppendThroughRolls(t, l, 0, 1)
	l.mu.Lock()
	prepared, base := l.activePrepared, l.segmentBase
	l.mu.Unlock()
	if !prepared {
		t.Fatal("the active segment is not a prepared one")
	}
	activePath := segmentPath(dir, base)

	room := func() int64 {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.opts.SegmentBytes - l.segmentSize
	}
	for room() >= 600 {
		if _, err := l.Append(ctx, zzWP5Payload(n)); err != nil {
			t.Fatal(err)
		}
		n++
	}
	// A fits in the segment; B then overflows it and must roll.
	payloadA := bytes.Repeat([]byte{'A'}, int(room())-frameHeaderSize-10)
	payloadB := bytes.Repeat([]byte{'B'}, 100)

	entered := make(chan struct{})
	release := make(chan struct{})
	var fired atomic.Int32
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpWrite && path == activePath && fired.Add(1) == 1 {
			close(entered)
			<-release
			return syscall.EIO
		}
		return nil
	})
	defer restore()

	aErr := make(chan error, 1)
	go func() { _, err := l.Append(ctx, payloadA); aErr <- err }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the flush never reached its write")
	}
	bErr := make(chan error, 1)
	go func() { _, err := l.Append(ctx, payloadB); bErr <- err }()
	// B takes mu and keeps it while it waits for fileOps in rollLocked;
	// nothing else takes mu while the flush is parked in the hook.
	deadline := time.Now().Add(10 * time.Second)
	for held := 0; held < 3; {
		if l.mu.TryLock() {
			l.mu.Unlock()
			held = 0
		} else {
			held++
		}
		if time.Now().After(deadline) {
			t.Fatal("the rolling append never parked holding mu")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	if err := <-aErr; err == nil {
		t.Fatal("the append whose write failed succeeded")
	}
	if err := <-bErr; err == nil {
		t.Fatal("the rolling append succeeded over a failed write")
	}
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if last := segs[len(segs)-1].base; last != base {
		t.Fatalf("last segment base = %d, want %d: the roll sealed the segment over the failed write", last, base)
	}

	_ = l.Close()
	restore()
	l2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen after a roll that raced a failed flush: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, n)
}
