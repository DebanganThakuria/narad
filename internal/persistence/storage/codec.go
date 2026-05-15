package storage

import (
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// codecForFlag resolves the codec to use when reading a frame whose
// header carries the given flag byte. existing, if non-nil, is the
// Log's configured codec; passing it avoids allocating a fresh pool
// when the frame's codec matches the one already in use.
func codecForFlag(flag uint8, existing codec.Codec) (codec.Codec, error) {
	switch flag {
	case codec.FlagNone:
		return codec.NewNoopCodec(), nil
	case codec.FlagZstd:
		if existing != nil && existing.Flag() == codec.FlagZstd {
			return existing, nil
		}
		return codec.NewZstdCodec(zstd.SpeedDefault)
	default:
		return nil, fmt.Errorf("%w: unknown codec flag 0x%x", ErrCorruptRecord, flag)
	}
}
