//go:build !darwin

package syncfile

import "os"

// fullSync is Sync's primitive: os.File.Sync, which enters syscall
// state everywhere but Darwin (see syncdata_darwin.go for why Darwin
// needs its own).
func fullSync(f *os.File) error { return f.Sync() }
