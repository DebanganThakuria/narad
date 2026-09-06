package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// A crash can leave the active segment's last frame complete in size
// but not in content (the file size reached the disk, the data did
// not: a zero-filled or scrambled last sector). Its header still parses,
// so before the fix recovery kept the frame (only a short frame was a
// torn tail) and the next commit's frames landed after it at the same
// offsets; header-only navigation then resolved those offsets to the
// corrupt frame first, the commit's CRC read-back failed once, and with
// the read-back disabled the records read as corrupt for good. A
// corrupt frame with nothing valid after it is a torn tail: truncated.
func TestRecoveryTruncatesCorruptTailFrameOfActiveSegment(t *testing.T) {
	for _, tc := range []struct {
		name string
		hurt func(t *testing.T, path string, lastFramePos, size int64)
	}{
		{"zero-filled-payload", func(t *testing.T, path string, lastFramePos, size int64) {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			// Keep the header intact, zero the payload.
			if _, err := f.WriteAt(make([]byte, size-lastFramePos-headerSize), lastFramePos+headerSize); err != nil {
				t.Fatal(err)
			}
		}},
		{"scrambled-crc", func(t *testing.T, path string, lastFramePos, _ int64) {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.WriteAt([]byte{0xde, 0xad, 0xbe, 0xef}, lastFramePos+23); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testLogPath(t)
			opts := slowFlushOpts(t, codec.NewNoopCodec())
			opts.DisableCommitVerify = true // the read-back must not be what saves us
			mustWriteAndClose(t, dir, opts, func(l *Log) {
				for i := range 3 {
					off, err := l.Append(fmt.Appendf(nil, "frame-%d", i))
					if err != nil {
						t.Fatal(err)
					}
					if err := l.CommitDurable(off, off); err != nil {
						t.Fatal(err)
					}
				}
			})
			frames := scanFramePositions(t, dir)
			if len(frames) != 3 {
				t.Fatalf("frames on disk = %d, want 3", len(frames))
			}
			path := activeSegmentPath(t, dir)
			size := fileSize(t, dir)
			tc.hurt(t, path, frames[2], size)

			l, err := NewLog(dir, opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer l.Close()
			if got := fileSize(t, dir); got != frames[2] {
				t.Fatalf("corrupt tail frame not truncated: size %d, want %d", got, frames[2])
			}
			if got := l.NextOffset(); got != 2 {
				t.Fatalf("NextOffset = %d, want 2", got)
			}
			if got := l.HighWatermark(); got != 2 {
				t.Fatalf("HighWatermark = %d, want 2 (clamped to the recovered tail)", got)
			}

			// The ingress WAL re-commits the lost record at the offset it
			// had; it must be readable, not shadowed by the corpse.
			off, err := l.Append([]byte("frame-2-again"))
			if err != nil {
				t.Fatal(err)
			}
			if off != 2 {
				t.Fatalf("re-commit landed at %d, want 2", off)
			}
			if err := l.CommitDurable(off, off); err != nil {
				t.Fatalf("CommitDurable after recovery: %v", err)
			}
			got, err := l.Read(2)
			if err != nil || !bytes.Equal(got, []byte("frame-2-again")) {
				t.Fatalf("Read(2) = (%q, %v)", got, err)
			}
			if err := l.VerifyDurable(0, 2); err != nil {
				t.Fatalf("VerifyDurable: %v", err)
			}
			for i := range int64(2) {
				got, err := l.Read(i)
				if err != nil || !bytes.Equal(got, fmt.Appendf(nil, "frame-%d", i)) {
					t.Fatalf("Read(%d) = (%q, %v)", i, got, err)
				}
			}
		})
	}
}

// Mid-file corruption stays: a corrupt frame followed by a valid one is
// resynced past, never truncated, exactly as before.
func TestRecoveryKeepsCorruptFrameUnderValidLaterFrame(t *testing.T) {
	dir := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	mustWriteAndClose(t, dir, opts, func(l *Log) {
		for i := range 3 {
			off, err := l.Append(fmt.Appendf(nil, "frame-%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if err := l.CommitDurable(off, off); err != nil {
				t.Fatal(err)
			}
		}
	})
	frames := scanFramePositions(t, dir)
	path := activeSegmentPath(t, dir)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xde, 0xad, 0xbe, 0xef}, frames[1]+23); err != nil {
		t.Fatal(err)
	}
	f.Close()
	sizeBefore := fileSize(t, dir)

	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	if got := fileSize(t, dir); got != sizeBefore {
		t.Fatalf("mid-file corruption must not be truncated: size %d, want %d", got, sizeBefore)
	}
	if got := l.NextOffset(); got != 3 {
		t.Fatalf("NextOffset = %d, want 3", got)
	}
	if _, err := l.Read(1); !IsCorrupt(err) && !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) of the corrupt frame: %v, want a corrupt or not-found error", err)
	}
	got, err := l.Read(2)
	if err != nil || !bytes.Equal(got, []byte("frame-2")) {
		t.Fatalf("Read(2) = (%q, %v)", got, err)
	}
}
