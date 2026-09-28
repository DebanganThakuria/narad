package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// A process dies inside the WAL's group-commit fsync: the batch's frame
// is in the page cache only. The restarted process's Open must not
// report or replay that frame as synced until a sync has covered it, or
// a dispatcher checkpoint can pass a record that a power loss then
// removes (and the next start refuses a checkpoint ahead of the WAL).
//
// The power loss is modelled by cutting the segment back to its size at
// the last sync that completed.
func TestWP22OpenReplaysOnlySyncedFrames(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Append(context.Background(), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	segs, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil || len(segs) != 1 {
		t.Fatalf("segments %v, %v", segs, err)
	}
	seg := segs[0]
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}

	var synced atomic.Int64
	synced.Store(st.Size())
	var armed atomic.Bool
	armed.Store(true)
	blocked := make(chan struct{})
	release := make(chan struct{})
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if (op != syncfile.OpSyncData && op != syncfile.OpSync) || path != seg {
			return nil
		}
		if armed.CompareAndSwap(true, false) {
			close(blocked)
			<-release
			return errors.New("process killed during fsync")
		}
		if fi, err := os.Stat(path); err == nil {
			synced.Store(fi.Size())
		}
		return nil
	})
	defer restore()

	errc := make(chan error, 1)
	go func() {
		_, err := a.Append(context.Background(), []byte("inflight"))
		errc <- err
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("append never reached its fsync")
	}

	// The restarted process opens the page-cache image.
	b, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	durable := b.durableSize
	b.mu.Unlock()
	var replayed []string
	if err := b.ReplayFromCursor(Cursor{}, func(r Record, _ Cursor) error {
		replayed = append(replayed, string(r.Payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := synced.Load(); durable > got {
		t.Errorf("Open reports %d bytes of the active segment as synced, but only %d were: replayed %v", durable, got, replayed)
	}

	// Power loss: every record the reopened log replayed survives it.
	if err := os.Truncate(seg, synced.Load()); err != nil {
		t.Fatal(err)
	}
	var survived []string
	if err := Replay(dir, 0, 0, func(r Record) error {
		survived = append(survived, string(r.Payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(survived) < len(replayed) {
		t.Errorf("the reopened log replayed %v, a power loss left %v", replayed, survived)
	}

	close(release)
	<-errc
	restore()
	_ = a.Close()
	_ = b.Close()
}

// A failed sync of the active segment fails Open.
func TestWP22OpenFailsWhenActiveSegmentSyncFails(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Append(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("injected EIO")
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSync && strings.HasSuffix(path, segmentSuffix) {
			return boom
		}
		return nil
	})
	b, err := Open(dir, faultOptions())
	restore()
	if err == nil {
		_ = b.Close()
		t.Fatal("Open succeeded although the active segment could not be synced")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Open error = %v, want it to wrap %v", err, boom)
	}
}
