//go:build darwin

package syncfile

import "os"

// syncData on Darwin is full os.File.Sync, which issues
// fcntl(F_FULLFSYNC). XNU has no fdatasync, and plain fsync on macOS
// does not force the drive's write cache — F_FULLFSYNC is the only
// call that makes the data actually durable, so the "cheapest correct
// primitive" here is the expensive one.
func syncData(f *os.File) error { return f.Sync() }
