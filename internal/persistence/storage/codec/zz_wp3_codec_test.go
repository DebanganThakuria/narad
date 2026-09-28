package codec

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// NewZstdDecoder decodes what NewZstdCodec encodes, and can still
// encode (building an encoder on first use).
func TestNewZstdDecoderRoundTrip(t *testing.T) {
	enc, err := NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatalf("NewZstdCodec() error = %v", err)
	}
	dec, err := NewZstdDecoder()
	if err != nil {
		t.Fatalf("NewZstdDecoder() error = %v", err)
	}
	if dec.Flag() != FlagZstd {
		t.Fatalf("Flag() = %d, want %d", dec.Flag(), FlagZstd)
	}
	src := bytes.Repeat([]byte("narad-payload-"), 32)
	got, err := dec.Decode(nil, enc.Encode(nil, src), len(src))
	if err != nil || !bytes.Equal(got, src) {
		t.Fatalf("Decode() = (%q, %v), want the source", got, err)
	}
	back, err := enc.Decode(nil, dec.Encode(nil, src), len(src))
	if err != nil || !bytes.Equal(back, src) {
		t.Fatalf("round trip through the decoder's Encode = (%q, %v)", back, err)
	}
}
