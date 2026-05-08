package storage

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Codec compresses/decompresses frame payloads. Implementations must be
// safe for concurrent use.
type Codec interface {
	Flag() uint8
	Encode(dst, src []byte) []byte
	Decode(dst, src []byte, dstSizeHint int) ([]byte, error)
}

type noopCodec struct{}

func NewNoopCodec() Codec { return noopCodec{} }

func (noopCodec) Flag() uint8                   { return codecNone }
func (noopCodec) Encode(dst, src []byte) []byte { return append(dst, src...) }
func (noopCodec) Decode(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src...), nil
}

type zstdCodec struct {
	encPool sync.Pool
	decPool sync.Pool
	level   zstd.EncoderLevel
}

// NewZstdCodec returns a zstd codec at the given encoder level. zstd's
// decompression speed is independent of the encoder level — there is
// no read-side cost to using SpeedBestCompression.
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
		return nil, fmt.Errorf("storage: zstd encoder init failed")
	}
	if d, ok := c.decPool.Get().(*zstd.Decoder); ok {
		c.decPool.Put(d)
	} else {
		return nil, fmt.Errorf("storage: zstd decoder init failed")
	}
	return c, nil
}

func (z *zstdCodec) Flag() uint8 { return codecZstd }

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

func codecForFlag(flag uint8, log *Log) (Codec, error) {
	switch flag {
	case codecNone:
		return noopCodec{}, nil
	case codecZstd:
		if log != nil && log.codec != nil && log.codec.Flag() == codecZstd {
			return log.codec, nil
		}
		return NewZstdCodec(zstd.SpeedDefault)
	default:
		return nil, fmt.Errorf("%w: unknown codec flag 0x%x", ErrCorruptRecord, flag)
	}
}
