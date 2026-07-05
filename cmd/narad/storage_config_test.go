package main

import (
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/config"
)

func TestParseZstdLevel(t *testing.T) {
	cases := []struct {
		input string
		want  zstd.EncoderLevel
	}{
		{input: "fastest", want: zstd.SpeedFastest},
		{input: "default", want: zstd.SpeedDefault},
		{input: "better", want: zstd.SpeedBetterCompression},
		{input: "best", want: zstd.SpeedBestCompression},
		{input: "", want: zstd.SpeedDefault},
	}

	for _, tc := range cases {
		got, err := parseZstdLevel(tc.input)
		if err != nil {
			t.Fatalf("parseZstdLevel(%q) error = %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("parseZstdLevel(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}

	if _, err := parseZstdLevel("weird"); err == nil {
		t.Fatal("parseZstdLevel() error = nil, want error")
	}
}

func TestBuildCodec(t *testing.T) {
	c, err := buildCodec(config.StorageConfig{Codec: "none"})
	if err != nil {
		t.Fatalf("buildCodec(none) error = %v", err)
	}
	if c.Flag() != 0 {
		t.Fatalf("buildCodec(none) flag = %d, want 0", c.Flag())
	}

	c, err = buildCodec(config.StorageConfig{Codec: "zstd", CompressionLevel: "better"})
	if err != nil {
		t.Fatalf("buildCodec(zstd) error = %v", err)
	}
	if c == nil {
		t.Fatal("buildCodec(zstd) returned nil codec")
	}

	if _, err := buildCodec(config.StorageConfig{Codec: "unknown"}); err == nil {
		t.Fatal("buildCodec(unknown) error = nil, want error")
	}
}

func TestStorageOptions(t *testing.T) {
	opts, err := storageOptions(config.StorageConfig{
		Codec:                       "none",
		Fsync:                       config.FsyncBatched,
		FlushBytes:                  128,
		FlushRecords:                16,
		FlushIntervalMs:             250,
		SyncIntervalMs:              750,
		SyncBytes:                   4096,
		HighWatermarkSyncIntervalMs: 3000,
		SegmentBytes:                4096,
		RetentionCheckIntervalMs:    500,
	})
	if err != nil {
		t.Fatalf("storageOptions() error = %v", err)
	}
	if opts.FlushBytes != 128 || opts.FlushRecords != 16 || opts.SegmentBytes != 4096 {
		t.Fatalf("storageOptions() opts = %+v", opts)
	}
	if opts.FlushInterval != 250*time.Millisecond {
		t.Fatalf("FlushInterval = %v, want %v", opts.FlushInterval, 250*time.Millisecond)
	}
	if opts.SyncMode != storage.SyncBatched || opts.SyncInterval != 750*time.Millisecond || opts.SyncBytes != 4096 {
		t.Fatalf("sync options = %+v", opts)
	}
	if opts.HWMSyncInterval != 3*time.Second {
		t.Fatalf("HWMSyncInterval = %v, want 3s", opts.HWMSyncInterval)
	}
	if opts.Retention.CheckInterval != 500*time.Millisecond {
		t.Fatalf("Retention.CheckInterval = %v, want %v", opts.Retention.CheckInterval, 500*time.Millisecond)
	}

	opts, err = storageOptions(config.StorageConfig{
		Codec:                       "none",
		Fsync:                       config.FsyncPerWrite,
		FlushBytes:                  128,
		FlushRecords:                16,
		FlushIntervalMs:             250,
		SyncIntervalMs:              750,
		HighWatermarkSyncIntervalMs: 3000,
		SegmentBytes:                4096,
		RetentionCheckIntervalMs:    500,
	})
	if err != nil {
		t.Fatalf("storageOptions(per_write) error = %v", err)
	}
	if opts.SyncMode != storage.SyncPerWrite {
		t.Fatalf("SyncMode = %q, want %q", opts.SyncMode, storage.SyncPerWrite)
	}
}

func TestIngressWALOptions(t *testing.T) {
	opts := ingressWALOptions(config.StorageConfig{
		IngressWALSyncIntervalMs: 5,
	})

	if opts.SyncInterval != 5*time.Millisecond {
		t.Fatalf("SyncInterval = %v, want 5ms", opts.SyncInterval)
	}
}

func TestStorageOptionsReturnsCodecError(t *testing.T) {
	_, err := storageOptions(config.StorageConfig{Codec: "wat"})
	if err == nil {
		t.Fatal("storageOptions() error = nil, want error")
	}
}
