//go:build linux || freebsd || netbsd || openbsd || dragonfly || solaris

package syncfile

import (
	"os"

	"golang.org/x/sys/unix"
)

// SyncData flushes f's data (and the size needed to read it back) with
// fdatasync(2), which every kernel in the build tag provides: Linux,
// FreeBSD, NetBSD, OpenBSD, DragonFly, and Solaris/illumos (the
// illumos build tag implies solaris). EINTR is retried: a signal
// during the flush must not be reported as a failed sync — the caller
// would fail a batch whose data may be perfectly durable.
func SyncData(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if err != unix.EINTR {
			return err
		}
	}
}
