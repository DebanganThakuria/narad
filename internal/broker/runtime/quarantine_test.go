package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// writeCopy makes dir holding one file of n bytes.
func writeCopy(t *testing.T, dir string, n int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.log"), make([]byte, n), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Every way this node sets a copy aside instead of deleting it shows in
// the inventory with its kind, topic, partition and size: a reclaim or
// install set-aside inside a topic directory (plain and timestamped
// names), a deleted incarnation's topic directory, and a set-aside move
// staging copy. Live partitions and live staging are not copies.
func TestQuarantinedCopiesListsEveryKindOfSetAsideCopy(t *testing.T) {
	dataDir := t.TempDir()
	topicDir := storage.TopicDir(dataDir, "orders")
	writeCopy(t, filepath.Join(topicDir, "p00000.quarantine"), 10)
	writeCopy(t, filepath.Join(topicDir, "p00001.quarantine.1759600000000000000"), 5)
	writeCopy(t, filepath.Join(topicDir, "p00002"), 100)
	writeCopy(t, filepath.Join(topicDir, "p00003.quarantine.x"), 100)
	stale := storage.StaleTopicDir(dataDir, "orders", "1111111111111111")
	writeCopy(t, filepath.Join(stale, "p00000"), 7)
	if err := storage.WriteTopicIncarnation(stale, "1111111111111111"); err != nil {
		t.Fatal(err)
	}
	writeCopy(t, filepath.Join(dataDir, ".moves", "pay-ments-3.quarantine"), 4)
	writeCopy(t, filepath.Join(dataDir, ".moves", "orders-0"), 100)

	logs := NewLogs(dataDir, storage.Options{}, nil, nil)
	if _, ok := logs.LastQuarantinedCopies(); ok {
		t.Fatal("an inventory before the first scan")
	}
	sum, err := logs.QuarantinedCopies()
	if err != nil {
		t.Fatalf("QuarantinedCopies: %v", err)
	}
	type key struct {
		kind, topic string
		partition   int
		bytes       int64
	}
	want := map[string]key{
		"p00000.quarantine":                     {QuarantineKindPartition, "orders", 0, 10},
		"p00001.quarantine.1759600000000000000": {QuarantineKindPartition, "orders", 1, 5},
		"orders.stale-1111111111111111":         {QuarantineKindTopicIncarnation, "orders", -1, 7 + 16},
		"pay-ments-3.quarantine":                {QuarantineKindStaging, "pay-ments", 3, 4},
	}
	if sum.Count != len(want) || len(sum.Copies) != len(want) {
		t.Fatalf("inventory = %+v, want %d copies", sum, len(want))
	}
	var total int64
	for _, q := range sum.Copies {
		w, ok := want[filepath.Base(q.Dir)]
		if !ok {
			t.Fatalf("unexpected copy %+v", q)
		}
		if got := (key{q.Kind, q.Topic, q.Partition, q.Bytes}); got != w {
			t.Fatalf("copy %s = %+v, want %+v", filepath.Base(q.Dir), got, w)
		}
		if q.ModTime.IsZero() {
			t.Fatalf("copy %s has no modification time", q.Dir)
		}
		total += w.bytes
	}
	if sum.Bytes != total {
		t.Fatalf("inventory bytes = %d, want %d", sum.Bytes, total)
	}
	if last, ok := logs.LastQuarantinedCopies(); !ok || last.Count != sum.Count || last.Bytes != sum.Bytes {
		t.Fatalf("kept inventory = %+v (%v), want the scan's", last, ok)
	}
}

// The listing is capped; the totals keep counting past it.
func TestQuarantinedCopiesCapsTheListButCountsEveryCopy(t *testing.T) {
	dataDir := t.TempDir()
	n := quarantineListCap + 3
	for i := range n {
		writeCopy(t, filepath.Join(storage.TopicDir(dataDir, "orders"), fmt.Sprintf("p%05d.quarantine", i)), 2)
	}
	sum, err := NewLogs(dataDir, storage.Options{}, nil, nil).QuarantinedCopies()
	if err != nil {
		t.Fatalf("QuarantinedCopies: %v", err)
	}
	if sum.Count != n || sum.Bytes != int64(2*n) || len(sum.Copies) != quarantineListCap {
		t.Fatalf("count %d, bytes %d, listed %d; want %d, %d, %d", sum.Count, sum.Bytes, len(sum.Copies), n, 2*n, quarantineListCap)
	}
}

// The gauges report the kept inventory: nothing before the first scan,
// and a scrape never walks the disk (a copy removed since the last scan
// is still counted until the next one).
func TestQuarantineGaugesReportCopiesAndBytes(t *testing.T) {
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{}, nil, nil)
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewQuarantineCollector(logs))
	if n, err := testutil.GatherAndCount(reg); err != nil || n != 0 {
		t.Fatalf("series before the first inventory = %d (%v), want none", n, err)
	}

	first := filepath.Join(storage.TopicDir(dataDir, "orders"), "p00000.quarantine")
	writeCopy(t, first, 30)
	writeCopy(t, filepath.Join(storage.TopicDir(dataDir, "orders"), "p00001.quarantine"), 12)
	if _, err := logs.QuarantinedCopies(); err != nil {
		t.Fatal(err)
	}
	expect := func(copies, bytes int) {
		t.Helper()
		want := fmt.Sprintf(`
# HELP narad_quarantined_bytes Bytes held by the partition copies this node set aside instead of deleting.
# TYPE narad_quarantined_bytes gauge
narad_quarantined_bytes %d
# HELP narad_quarantined_copies Partition copies this node set aside instead of deleting (reclaim and install set-asides, set-aside move staging copies, directories of deleted topic incarnations). Each may hold the only instance of some records.
# TYPE narad_quarantined_copies gauge
narad_quarantined_copies %d
`, bytes, copies)
		if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
			t.Fatal(err)
		}
	}
	expect(2, 42)

	if err := os.RemoveAll(first); err != nil {
		t.Fatal(err)
	}
	expect(2, 42)
	if _, err := logs.QuarantinedCopies(); err != nil {
		t.Fatal(err)
	}
	expect(1, 12)
}
