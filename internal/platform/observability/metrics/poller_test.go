package metrics

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func writeBytes(dir, name string, size int) error {
	return os.WriteFile(filepath.Join(dir, name), bytes.Repeat([]byte("x"), size), 0o600)
}

// TestPollerDataDirScanIntervalIsConfigurable verifies the data-dir
// walk is rate-limited by the field (defaulting to the historical 30s),
// and that a zero interval walks on every tick.
func TestPollerDataDirScanIntervalIsConfigurable(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	dataDir := t.TempDir()
	writeFile := func(name string, size int) {
		t.Helper()
		if err := writeBytes(dataDir, name, size); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeFile("a.log", 11)

	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger(), dataDir)
	if p.DataDirScanInterval != defaultDataDirScanInterval {
		t.Fatalf("default DataDirScanInterval = %v, want %v", p.DataDirScanInterval, defaultDataDirScanInterval)
	}
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_data_dir_size_bytes", nil); got != 11 {
		t.Fatalf("data_dir_size_bytes = %v, want 11", got)
	}

	// Within the interval the gauge is not refreshed.
	writeFile("b.log", 5)
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_data_dir_size_bytes", nil); got != 11 {
		t.Fatalf("data_dir_size_bytes rescanned inside the interval: %v", got)
	}

	// A zero interval walks on every tick.
	p.DataDirScanInterval = 0
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_data_dir_size_bytes", nil); got != 16 {
		t.Fatalf("data_dir_size_bytes with zero interval = %v, want 16", got)
	}
}
