// Package codec defines the Codec interface and provides the zstd and
// no-op implementations used by the storage layer.
//
// A Codec compresses and decompresses frame payloads. Implementations
// must be safe for concurrent use; the storage layer shares codec
// instances across goroutines.
package codec

import (
	"fmt"

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
	encPool chan *zstd.Encoder
	encSem  chan struct{}
	decPool chan *zstd.Decoder
	decSem  chan struct{}
	level   zstd.EncoderLevel
}

const maxRetainedZstdWorkers = 4

// NewZstdCodec returns a zstd codec at the given encoder level.
// zstd's decompression speed is independent of the encoder level —
// there is no read-side cost to using SpeedBestCompression.
func NewZstdCodec(level zstd.EncoderLevel) (Codec, error) {
	c := &zstdCodec{
		level:   level,
		encPool: make(chan *zstd.Encoder, maxRetainedZstdWorkers),
		encSem:  make(chan struct{}, maxRetainedZstdWorkers),
		decPool: make(chan *zstd.Decoder, maxRetainedZstdWorkers),
		decSem:  make(chan struct{}, maxRetainedZstdWorkers),
	}
	c.encSem <- struct{}{}
	enc, err := c.newEncoder()
	if err != nil {
		<-c.encSem
		return nil, err
	}
	c.putEncoder(enc)
	c.decSem <- struct{}{}
	dec, err := c.newDecoder()
	if err != nil {
		<-c.decSem
		return nil, err
	}
	c.putDecoder(dec)
	return c, nil
}

func (z *zstdCodec) Flag() uint8 { return FlagZstd }

func (z *zstdCodec) Encode(dst, src []byte) []byte {
	enc := z.getEncoder()
	defer z.putEncoder(enc)
	return enc.EncodeAll(src, dst)
}

func (z *zstdCodec) Decode(dst, src []byte, dstSizeHint int) ([]byte, error) {
	dec := z.getDecoder()
	defer z.putDecoder(dec)
	if dstSizeHint > 0 && cap(dst)-len(dst) < dstSizeHint {
		grown := make([]byte, len(dst), len(dst)+dstSizeHint)
		copy(grown, dst)
		dst = grown
	}
	return dec.DecodeAll(src, dst)
}

func (z *zstdCodec) getEncoder() *zstd.Encoder {
	select {
	case enc := <-z.encPool:
		return enc
	default:
	}
	select {
	case z.encSem <- struct{}{}:
		enc, err := z.newEncoder()
		if err != nil {
			<-z.encSem
			panic(err)
		}
		return enc
	case enc := <-z.encPool:
		return enc
	}
}

func (z *zstdCodec) putEncoder(enc *zstd.Encoder) {
	z.encPool <- enc
}

func (z *zstdCodec) newEncoder() (*zstd.Encoder, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(z.level),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("codec: zstd encoder init failed: %w", err)
	}
	return enc, nil
}

func (z *zstdCodec) getDecoder() *zstd.Decoder {
	select {
	case dec := <-z.decPool:
		return dec
	default:
	}
	select {
	case z.decSem <- struct{}{}:
		dec, err := z.newDecoder()
		if err != nil {
			<-z.decSem
			panic(err)
		}
		return dec
	case dec := <-z.decPool:
		return dec
	}
}

func (z *zstdCodec) putDecoder(dec *zstd.Decoder) {
	z.decPool <- dec
}

func (z *zstdCodec) newDecoder() (*zstd.Decoder, error) {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
	)
	if err != nil {
		return nil, fmt.Errorf("codec: zstd decoder init failed: %w", err)
	}
	return dec, nil
}
