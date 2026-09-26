package storage

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// wp3FailReadBackOnce makes the next commit on l fail after its frames
// are written and fsynced: that fsync scribbles over the last bytes of
// the frame just written, so the commit's CRC read-back rejects it. The
// commit fails after its frames are durable and before they are exposed,
// which is also where a failed high-watermark release fails one.
func wp3FailReadBackOnce(t *testing.T, l *Log) {
	t.Helper()
	var armed atomic.Bool
	armed.Store(true)
	fsyncHook = func(s *segment) error {
		if filepath.Dir(s.path) != l.dir || !armed.CompareAndSwap(true, false) {
			return nil
		}
		f, err := os.OpenFile(s.path, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		// Inside the last frame's payload: every record carries a
		// 4-byte length prefix, so a payload is never shorter.
		_, err = f.WriteAt([]byte{0xde, 0xad, 0xbe, 0xef}, s.sizeBytes-4)
		return err
	}
	t.Cleanup(func() { fsyncHook = nil })
}
