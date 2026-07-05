package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	"github.com/debanganthakuria/narad/internal/platform/config"
)

// storageOptions translates the storage config section into the storage
// engine's option set.
func storageOptions(sc config.StorageConfig) (storage.Options, error) {
	storageCodec, err := buildCodec(sc)
	if err != nil {
		return storage.Options{}, err
	}
	return storage.Options{
		Codec:           storageCodec,
		FlushBytes:      sc.FlushBytes,
		FlushRecords:    sc.FlushRecords,
		FlushInterval:   time.Duration(sc.FlushIntervalMs) * time.Millisecond,
		SyncMode:        storageSyncMode(sc.Fsync),
		SyncInterval:    time.Duration(sc.SyncIntervalMs) * time.Millisecond,
		SyncBytes:       sc.SyncBytes,
		HWMSyncInterval: time.Duration(sc.HighWatermarkSyncIntervalMs) * time.Millisecond,
		SegmentBytes:    sc.SegmentBytes,
		Retention: storage.RetentionConfig{
			CheckInterval: time.Duration(sc.RetentionCheckIntervalMs) * time.Millisecond,
		},
	}, nil
}

// ingressWALOptions applies the storage config's ingress-WAL sync tuning
// on top of the ingress defaults.
func ingressWALOptions(sc config.StorageConfig) wal.Options {
	opts := ingress.DefaultWALOptions()
	opts.SyncInterval = time.Duration(sc.IngressWALSyncIntervalMs) * time.Millisecond
	return opts
}

func storageSyncMode(mode config.FsyncMode) storage.SyncMode {
	if mode == config.FsyncPerWrite {
		return storage.SyncPerWrite
	}
	return storage.SyncBatched
}

func buildCodec(sc config.StorageConfig) (codec.Codec, error) {
	switch strings.ToLower(sc.Codec) {
	case "none":
		return codec.NewNoopCodec(), nil
	case "zstd", "":
		level, err := parseZstdLevel(sc.CompressionLevel)
		if err != nil {
			return nil, err
		}
		return codec.NewZstdCodec(level)
	default:
		return nil, fmt.Errorf("unknown codec %q", sc.Codec)
	}
}

func parseZstdLevel(s string) (zstd.EncoderLevel, error) {
	switch strings.ToLower(s) {
	case "fastest":
		return zstd.SpeedFastest, nil
	case "default", "":
		return zstd.SpeedDefault, nil
	case "better":
		return zstd.SpeedBetterCompression, nil
	case "best":
		return zstd.SpeedBestCompression, nil
	default:
		return 0, fmt.Errorf("unknown compression level %q", s)
	}
}
