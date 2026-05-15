// Package codec defines the Codec interface and provides the zstd and
// no-op implementations used by the storage layer.
//
// A Codec compresses and decompresses frame payloads. Implementations
// must be safe for concurrent use; the storage layer shares codec
// instances across goroutines.
package codec

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Flag constants map codec identities to their on-disk representation
// in the frame flags byte (lower 3 bits).
const (
	FlagNone = uint8(0)
	FlagZstd = uint8(1)
)

// Codec compresses and decompresses frame payloads.
type Codec interface {
	// Flag returns the 3-bit codec ID written into the frame header.
	Flag() uint8
	// Encode appends the compressed form of src to dst.
	Encode(dst, src []byte) []byte
	// Decode appends the decompressed form of src to dst.
	// dstSizeHint, when > 0, is the expected uncompressed size.
	Decode(dst, src []byte, dstSizeHint int) ([]byte, error)
}

// -- noop ----------------------------------------------------------------

type noopCodec struct{}

// NewNoopCodec returns a passthrough codec that stores frames uncompressed.
func NewNoopCodec() Codec { return noopCodec{} }

func (noopCodec) Flag() uint8                   { return FlagNone }
func (noopCodec) Encode(dst, src []byte) []byte { return append(dst, src...) }
func (noopCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src...), nil
}

// -- zstd ----------------------------------------------------------------

type zstdCodec struct {
	encPool sync.Pool
	decPool sync.Pool
	level   zstd.EncoderLevel
}

// NewZstdCodec returns a zstd codec at the given encoder level.
// zstd's decompression speed is independent of the encoder level —
// there is no read-side cost to using SpeedBestCompression.
func NewZstdCodec(level zstd.EncoderLevel) (Codec, error) {
	c := &zstdCodec{level: level}
	c.encPool.New = func() any {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(level),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			return err
		}
		return enc
	}
	c.decPool.New = func() any {
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
		)
		if err != nil {
			return err
		}
		return dec
	}
	if e, ok := c.encPool.Get().(*zstd.Encoder); ok {
		c.encPool.Put(e)
	} else {
		return nil, fmt.Errorf("codec: zstd encoder init failed")
	}
	if d, ok := c.decPool.Get().(*zstd.Decoder); ok {
		c.decPool.Put(d)
	} else {
		return nil, fmt.Errorf("codec: zstd decoder init failed")
	}
	return c, nil
}

func (z *zstdCodec) Flag() uint8 { return FlagZstd }

func (z *zstdCodec) Encode(dst, src []byte) []byte {
	enc := z.encPool.Get().(*zstd.Encoder)
	defer z.encPool.Put(enc)
	return enc.EncodeAll(src, dst)
}

func (z *zstdCodec) Decode(dst, src []byte, dstSizeHint int) ([]byte, error) {
	dec := z.decPool.Get().(*zstd.Decoder)
	defer z.decPool.Put(dec)
	if dstSizeHint > 0 && cap(dst)-len(dst) < dstSizeHint {
		grown := make([]byte, len(dst), len(dst)+dstSizeHint)
		copy(grown, dst)
		dst = grown
	}
	return dec.DecodeAll(src, dst)
}
