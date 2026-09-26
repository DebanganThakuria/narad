package storage

import (
	"fmt"
	"sync"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// foreignZstd reads zstd frames for logs whose own codec is not zstd:
// frames written before the log was reopened with a different codec
// (storage.codec switched from zstd to none). Built on the first such
// frame and shared by every log; before, each cold read of such a frame
// built and dropped a whole zstd encoder and decoder.
var foreignZstd struct {
	once sync.Once
	c    codec.Codec
	err  error
}

func foreignZstdCodec() (codec.Codec, error) {
	foreignZstd.once.Do(func() {
		foreignZstd.c, foreignZstd.err = codec.NewZstdDecoder()
	})
	return foreignZstd.c, foreignZstd.err
}

// codecForFlag resolves the codec to use when reading a frame whose
// header carries the given flag byte. existing, if non-nil, is the
// Log's configured codec, used when the frame's codec matches it; a
// zstd frame read by a log with another codec uses the shared
// foreignZstdCodec.
func codecForFlag(flag uint8, existing codec.Codec) (codec.Codec, error) {
	switch flag {
	case codec.FlagNone:
		return codec.NewNoopCodec(), nil
	case codec.FlagZstd:
		if existing != nil && existing.Flag() == codec.FlagZstd {
			return existing, nil
		}
		return foreignZstdCodec()
	default:
		return nil, fmt.Errorf("%w: unknown codec flag 0x%x", ErrCorruptRecord, flag)
	}
}
