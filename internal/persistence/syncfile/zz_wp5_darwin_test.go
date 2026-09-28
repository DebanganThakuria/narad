//go:build darwin

package syncfile

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// On Darwin a sync must run in syscall state. os.File.Sync reaches
// F_FULLFSYNC through runtime.fcntl, a libc call that never enters
// syscall state, so the flushing goroutine keeps its P and a
// stop-the-world waits for the flush to return: a goroutine dump
// (itself a stop-the-world) can then only ever catch the syncing
// goroutine back in Go code. Issued through x/sys, the flush releases
// its P, the dump proceeds without waiting, and the goroutine shows up
// as [syscall] with the sync on its stack.
func TestZZWP5DarwinSyncRunsInSyscallState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
		sync  func(*os.File) error
	}{
		{"SyncData", "syncfile.SyncData(", SyncData},
		{"Sync", "syncfile.Sync(", Sync},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.Create(filepath.Join(t.TempDir(), "data"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			stop := make(chan struct{})
			var wg sync.WaitGroup
			var loopErr error
			wg.Go(func() {
				buf := make([]byte, 4096)
				for {
					select {
					case <-stop:
						return
					default:
					}
					if _, err := f.Write(buf); err != nil {
						loopErr = err
						return
					}
					if err := tc.sync(f); err != nil {
						loopErr = err
						return
					}
				}
			})

			found := false
			deadline := time.Now().Add(5 * time.Second)
			stacks := make([]byte, 1<<20)
			samples := 0
			for !found && time.Now().Before(deadline) {
				n := runtime.Stack(stacks, true)
				samples++
				for g := range strings.SplitSeq(string(stacks[:n]), "\n\n") {
					if strings.Contains(g, tc.frame) && strings.Contains(g, "[syscall") {
						found = true
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
			close(stop)
			wg.Wait()
			if loopErr != nil {
				t.Fatalf("sync loop: %v", loopErr)
			}
			if !found {
				t.Fatalf("%d goroutine dumps never caught %s in syscall state: the flush holds its P for its whole duration", samples, tc.name)
			}
		})
	}
}

// A closed file keeps os.File.Sync's error shape, which callers match
// with errors.Is(err, os.ErrClosed) (storage's segment close).
func TestZZWP5DarwinSyncClosedFileIsErrClosed(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	for name, fn := range map[string]func(*os.File) error{"SyncData": SyncData, "Sync": Sync} {
		err := fn(f)
		var pathErr *os.PathError
		if !errors.Is(err, os.ErrClosed) || !errors.As(err, &pathErr) || pathErr.Op != "sync" {
			t.Fatalf("%s on a closed file = %v, want *os.PathError{Op: sync} wrapping os.ErrClosed", name, err)
		}
	}
}
