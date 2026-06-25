package storage

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestIsCorrupt(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"crc mismatch", fmt.Errorf("%w: crc want=1 got=2", errCorrupt), true},
		{"bad magic", errBadMagic, true},
		{"malformed record stream", fmt.Errorf("%w: split", ErrCorruptRecord), true},
		{"bare errCorrupt", errCorrupt, true},
		{"offset not found is not corruption", ErrOffsetNotFound, false},
		{"log closed is transient", ErrLogClosed, false},
		{"io error is transient", io.ErrUnexpectedEOF, false},
		{"nil is not corruption", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCorrupt(tc.err); got != tc.want {
				t.Fatalf("IsCorrupt(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Guard the exact error readFrameAt returns on a CRC mismatch is classified as
// corruption (the consume skip path keys off this).
func TestReadFrameCrcErrorIsCorrupt(t *testing.T) {
	if !errors.Is(fmt.Errorf("%w: crc", errCorrupt), errCorrupt) {
		t.Fatal("sanity: errCorrupt wrapping broken")
	}
	if !IsCorrupt(fmt.Errorf("%w: crc want=0x1 got=0x2 at pos=0", errCorrupt)) {
		t.Fatal("CRC-mismatch error not classified as corrupt")
	}
}
