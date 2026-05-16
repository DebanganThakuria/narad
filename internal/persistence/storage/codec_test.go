package storage

import (
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"

	storagecodec "github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

func TestCodecForFlag(t *testing.T) {
	noop := storagecodec.NewNoopCodec()

	cases := []struct {
		name        string
		flag        uint8
		existing    storagecodec.Codec
		wantFlag    uint8
		wantReuse   bool
		wantErr     bool
	}{
		{name: "none returns noop codec", flag: storagecodec.FlagNone, wantFlag: storagecodec.FlagNone},
		{name: "zstd reuses existing codec", flag: storagecodec.FlagZstd, existing: mustZstdCodec(t), wantFlag: storagecodec.FlagZstd, wantReuse: true},
		{name: "unknown flag returns error", flag: 0xff, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := codecForFlag(tc.flag, tc.existing)
			if tc.wantErr {
				if err == nil || !errors.Is(err, ErrCorruptRecord) {
					t.Fatalf("codecForFlag() error = %v, want %v", err, ErrCorruptRecord)
				}
				return
			}
			if err != nil {
				t.Fatalf("codecForFlag() error = %v", err)
			}
			if got.Flag() != tc.wantFlag {
				t.Fatalf("codecForFlag() flag = %d, want %d", got.Flag(), tc.wantFlag)
			}
			if tc.wantReuse && got != tc.existing {
				t.Fatal("codecForFlag() did not reuse existing codec")
			}
			if !tc.wantReuse && tc.existing == nil && tc.flag == storagecodec.FlagNone && got == noop {
				return
			}
		})
	}
}

func mustZstdCodec(t *testing.T) storagecodec.Codec {
	t.Helper()
	c, err := storagecodec.NewZstdCodec(zstd.SpeedDefault)
	if err != nil {
		t.Fatalf("NewZstdCodec() error = %v", err)
	}
	return c
}
