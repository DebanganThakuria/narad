//go:build !linux && !freebsd && !netbsd && !openbsd && !dragonfly && !solaris && !darwin && !windows

package syncfile

import "os"

// syncData falls back to full os.File.Sync on kernels without an
// audited fdatasync path (e.g. AIX, wasm). Correct everywhere Go runs,
// just not data-only.
func syncData(f *os.File) error { return f.Sync() }
