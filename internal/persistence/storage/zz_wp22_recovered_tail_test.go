package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// A process dies inside a commit's segment fsync: the commit's frame is
// in the page cache, not on disk. The restarted process reopens the
// partition, and then the machine loses power before anything else
// fsyncs the segment. Every record the restarted process exposed must
// survive that power loss, or consumers ack offsets that later hold
// other records.
//
// The power loss is modelled by cutting the segment back to its size at
// the last segment sync that completed.
func TestWP22ReopenAfterCrashExposesOnlyDurableRecords(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	opts := Options{FlushInterval: 2 * time.Millisecond}
	batch := func(tag string) [][]byte {
		out := make([][]byte, 5)
		for i := range out {
			out[i] = fmt.Appendf(nil, "%s-%03d", tag, i)
		}
		return out
	}

	a, err := NewLog(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	first, last, err := a.AppendBatch(batch("durable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CommitDurable(first, last); err != nil {
		t.Fatal(err)
	}
	segs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(segs) != 1 {
		t.Fatalf("segments %v, %v", segs, err)
	}
	seg := segs[0]
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}

	// synced is the segment size at the last sync that went through;
	// the first sync after arming never returns (the process dies in it).
	var synced atomic.Int64
	synced.Store(st.Size())
	var armed atomic.Bool
	armed.Store(true)
	blocked := make(chan struct{})
	release := make(chan struct{})
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if (op != syncfile.OpSyncData && op != syncfile.OpSync) || !strings.HasSuffix(path, ".log") {
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
		f, l, err := a.AppendBatch(batch("inflight"))
		if err != nil {
			errc <- err
			return
		}
		errc <- a.CommitDurable(f, l)
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("commit never reached its fsync")
	}

	// The restarted process opens the page-cache image.
	b, err := NewLog(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	exposed := b.HighWatermark()
	served := make([]string, 0, exposed)
	for off := range exposed {
		r, err := b.Read(off)
		if err != nil {
			t.Fatalf("read %d: %v", off, err)
		}
		served = append(served, string(r))
	}

	// Power loss: the segment keeps only what a completed sync covered.
	if err := os.Truncate(seg, synced.Load()); err != nil {
		t.Fatal(err)
	}
	c, err := NewLog(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	survived := c.HighWatermark()
	if survived < exposed {
		t.Fatalf("the reopen exposed [0,%d) but only [0,%d) survived a power loss: it served records that were never fsynced", exposed, survived)
	}
	for off := range exposed {
		r, err := c.Read(off)
		if err != nil {
			t.Fatalf("after power loss, read %d: %v", off, err)
		}
		if string(r) != served[off] {
			t.Fatalf("after power loss, offset %d holds %q, was served as %q", off, r, served[off])
		}
	}

	close(release)
	<-errc
	restore()
	for _, l := range []*Log{a, b, c} {
		_ = l.Close()
	}
}

// A failed sync of the recovered tail fails the open, instead of
// exposing records of unknown durability.
func TestWP22ReopenFailsWhenRecoveredTailSyncFails(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	a, err := NewLog(dir, Options{FlushInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	first, last, err := a.AppendBatch([][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CommitDurable(first, last); err != nil {
		t.Fatal(err)
	}
	// Crash image: the first commit emptied the hwm file, so the next
	// open takes the boundary from the tail.
	if data, err := os.ReadFile(hwmFilePath(dir)); err != nil || len(data) != 0 {
		t.Fatalf("hwm file after a commit: %d bytes, %v; want empty", len(data), err)
	}

	boom := errors.New("injected EIO")
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSyncData && strings.HasSuffix(path, ".log") {
			return boom
		}
		return nil
	})
	b, err := NewLog(dir, Options{FlushInterval: 2 * time.Millisecond})
	restore()
	if err == nil {
		_ = b.Close()
		t.Fatal("NewLog succeeded although the recovered tail could not be synced")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("NewLog error = %v, want it to wrap %v", err, boom)
	}
	_ = a.Close()
}
