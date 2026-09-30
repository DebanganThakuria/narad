//go:build linux || freebsd || netbsd || openbsd || dragonfly || solaris

package syncfile

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncData flushes f's data (and the size needed to read it back) with
// fdatasync(2), which every kernel in the build tag provides: Linux,
// FreeBSD, NetBSD, OpenBSD, DragonFly, and Solaris/illumos (the
// illumos build tag implies solaris). EINTR is retried: a signal
// during the flush must not be reported as a failed sync: the caller
// would fail a batch whose data may be perfectly durable.
//
// The call runs inside RawConn.Control, as Darwin's F_FULLFSYNC does,
// which holds a reference on the descriptor so a concurrent Close
// cannot close and reuse it mid-call. The consumer offset committer
// relies on that: its tick writes out a held descriptor that a Forget
// may close meanwhile. Reading the descriptor with f.Fd() instead was a
// data race with that Close, and could flush whatever file an open
// reused the number for. Errors keep os.File.Sync's shape: an
// *os.PathError with Op "sync", wrapping os.ErrClosed for a closed file.
func syncData(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	if err := rc.Control(func(fd uintptr) {
		for {
			if syncErr = unix.Fdatasync(int(fd)); syncErr != unix.EINTR {
				return
			}
		}
	}); err != nil {
		// Control fails only when the file is already closed.
		return &os.PathError{Op: "sync", Path: f.Name(), Err: os.ErrClosed}
	}
	if syncErr != nil {
		return &os.PathError{Op: "sync", Path: f.Name(), Err: syncErr}
	}
	return nil
}
