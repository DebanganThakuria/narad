//go:build darwin

package syncfile

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncData on Darwin issues fcntl(F_FULLFSYNC). XNU has no fdatasync,
// and plain fsync on macOS does not force the drive's write cache, so
// F_FULLFSYNC is the only call that makes the data actually durable:
// the "cheapest correct primitive" here is the expensive one.
func syncData(f *os.File) error { return fullFsync(f) }

// fullSync is Sync's primitive: the same F_FULLFSYNC, which on Darwin
// is also what os.File.Sync issues.
func fullSync(f *os.File) error { return fullFsync(f) }

// fullFsync is os.File.Sync's F_FULLFSYNC issued through x/sys instead
// of the standard library. os.File.Sync reaches the kernel through
// runtime.fcntl, a libc call that never enters syscall state: the
// goroutine keeps its P for the whole flush (tens of milliseconds on
// APFS), the scheduler cannot hand that P to other work, and every GC
// stop-the-world waits for the longest flush in flight. x/sys goes
// through the runtime's syscall path, which releases the P and leaves
// the goroutine suspendable while the kernel flushes.
//
// Durability is unchanged: the call is still F_FULLFSYNC, EINTR is
// retried, and a file system without F_FULLFSYNC (ENOTSUP) falls back
// to fsync, as internal/poll does. The fcntl runs inside
// RawConn.Control, which holds a reference on the descriptor so a
// concurrent Close cannot close and reuse it mid-call. Errors keep
// os.File.Sync's shape: an *os.PathError with Op "sync", wrapping
// os.ErrClosed for a closed file.
func fullFsync(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	if err := rc.Control(func(fd uintptr) { syncErr = fcntlFullFsync(int(fd)) }); err != nil {
		// Control fails only when the file is already closed.
		return &os.PathError{Op: "sync", Path: f.Name(), Err: os.ErrClosed}
	}
	if syncErr != nil {
		return &os.PathError{Op: "sync", Path: f.Name(), Err: syncErr}
	}
	return nil
}

// fcntlFullFsync flushes fd to stable storage with F_FULLFSYNC,
// retrying EINTR and falling back to fsync where the file system does
// not support F_FULLFSYNC.
func fcntlFullFsync(fd int) error {
	for {
		_, err := unix.FcntlInt(uintptr(fd), unix.F_FULLFSYNC, 0)
		switch err {
		case unix.EINTR:
			continue
		case unix.ENOTSUP:
			return retryEINTR(func() error { return unix.Fsync(fd) })
		}
		return err
	}
}

// retryEINTR runs call until it returns something other than EINTR.
func retryEINTR(call func() error) error {
	for {
		if err := call(); err != unix.EINTR {
			return err
		}
	}
}
