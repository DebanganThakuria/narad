package codec

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestNewNoopCodec(t *testing.T) {
	c := NewNoopCodec()
	if c.Flag() != FlagNone {
		t.Fatalf("Flag() = %d, want %d", c.Flag(), FlagNone)
	}

	dst := []byte("prefix-")
	encoded := c.Encode(append([]byte(nil), dst...), []byte("payload"))
	if string(encoded) != "prefix-payload" {
		t.Fatalf("Encode() = %q, want %q", encoded, "prefix-payload")
	}

	decoded, err := c.Decode(append([]byte(nil), dst...), []byte("payload"), 128)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if string(decoded) != "prefix-payload" {
		t.Fatalf("Decode() = %q, want %q", decoded, "prefix-payload")
	}
}

func TestNewZstdCodecRoundTrip(t *testing.T) {
	c, err := NewZstdCodec(zstd.SpeedBetterCompression)
	if err != nil {
		t.Fatalf("NewZstdCodec() error = %v", err)
	}
	if c.Flag() != FlagZstd {
		t.Fatalf("Flag() = %d, want %d", c.Flag(), FlagZstd)
	}

	src := bytes.Repeat([]byte("narad-payload-"), 32)
	prefix := []byte("prefix-")
	encoded := c.Encode(nil, src)
	decoded, err := c.Decode(append([]byte(nil), prefix...), encoded, len(src))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	want := append(prefix, src...)
	if !bytes.Equal(decoded, want) {
		t.Fatalf("Decode() = %q, want %q", decoded, want)
	}
}

func TestZstdDecodeHandlesInvalidPayload(t *testing.T) {
	c, err := NewZstdCodec(zstd.SpeedDefault)
	if err != nil {
		t.Fatalf("NewZstdCodec() error = %v", err)
	}

	if _, err := c.Decode(nil, []byte("not-zstd"), 32); err == nil {
		t.Fatal("Decode() error = nil, want error")
	}
}

func TestZstdDecodePreservesExistingPrefixWhenGrowing(t *testing.T) {
	c, err := NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatalf("NewZstdCodec() error = %v", err)
	}

	src := bytes.Repeat([]byte("x"), 256)
	encoded := c.Encode(nil, src)
	prefix := []byte("prefix-")
	dst := append([]byte(nil), prefix...)

	decoded, err := c.Decode(dst, encoded, len(src))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	want := append(prefix, src...)
	if !bytes.Equal(decoded, want) {
		t.Fatalf("Decode() = %q, want %q", decoded, want)
	}
}
