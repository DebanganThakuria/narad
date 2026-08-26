//go:build windows

package syncfile

import "os"

// SyncData on Windows is full os.File.Sync, which calls
// FlushFileBuffers. NT exposes no data-only flush; this is the one
// durable primitive the kernel offers.
func SyncData(f *os.File) error { return f.Sync() }
