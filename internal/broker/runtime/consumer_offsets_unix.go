//go:build unix

package runtime

import (
	"os"
	"runtime"
	"syscall"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// offsetWriteOut hands f's written pages to the device, the committer's
// per-partition half of a durability point. Linux: fdatasync, which is
// already durable (syncfile.SyncData). macOS: plain fsync(2), which
// sends the pages to the drive but leaves them in its volatile cache;
// offsetFlushDevice then persists every file written out this way on
// the same device with one F_FULLFSYNC (fcntl(2): data fsync'd earlier
// on the device is on stable storage when F_FULLFSYNC returns), instead
// of one F_FULLFSYNC per partition. EINTR is retried.
func offsetWriteOut(f *os.File) error {
	if runtime.GOOS != "darwin" {
		return syncfile.SyncData(f)
	}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	if err := rc.Control(func(fd uintptr) {
		for {
			if syncErr = syscall.Fsync(int(fd)); syncErr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return &os.PathError{Op: "fsync", Path: f.Name(), Err: os.ErrClosed}
	}
	if syncErr != nil {
		return &os.PathError{Op: "fsync", Path: f.Name(), Err: syncErr}
	}
	return nil
}

// offsetFlushDevice completes a durability point for every file
// written out on f's device since the last one: F_FULLFSYNC on macOS
// (syncfile.Sync, which falls back to fsync where the file system has
// no F_FULLFSYNC), nothing elsewhere, where offsetWriteOut was durable
// on its own.
func offsetFlushDevice(f *os.File) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	return syncfile.Sync(f)
}

// offsetSyncDir makes a directory's new entry durable with the files
// of the same tick: Linux fsyncs it, macOS writes it out with fsync(2)
// and leaves the drive cache to the tick's offsetFlushDevice, as for
// file data, rather than an F_FULLFSYNC per created file (a first tick
// after an upgrade creates consumer.ahead for every acked partition).
func offsetSyncDir(dir *os.File) error {
	if runtime.GOOS != "darwin" {
		return syncfile.Sync(dir)
	}
	return offsetWriteOut(dir)
}

// offsetDevice is the device a file lives on, so a tick issues one
// offsetFlushDevice per device it wrote out.
func offsetDevice(info os.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev)
	}
	return 0
}

// offsetFDCap is how many consumer.ahead descriptors the committer
// holds open: a quarter of the soft open-file limit, at most
// consumerOffsetMaxFDs, so the rest of the broker keeps its share.
func offsetFDCap() int {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return consumerOffsetMaxFDs
	}
	return int(min(uint64(rl.Cur)/4, consumerOffsetMaxFDs))
}
