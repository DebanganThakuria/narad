//go:build dumplog

package cluster

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestDumpLog prints the keyed records of one partition directory
// (NARAD_DUMP_DIR), from offset NARAD_DUMP_FROM on, as "offset key
// payload". Investigation aid for a preserved data directory:
//
//	go test -tags dumplog -run TestDumpLog ./tests/cluster/ -v
func TestDumpLog(t *testing.T) {
	dir := os.Getenv("NARAD_DUMP_DIR")
	if dir == "" {
		t.Skip("NARAD_DUMP_DIR not set")
	}
	from, _ := strconv.ParseInt(os.Getenv("NARAD_DUMP_FROM"), 10, 64)
	log, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	fmt.Printf("dir=%s next_offset=%d hwm=%d\n", dir, log.NextOffset(), log.HighWatermark())
	for off := from; off < log.NextOffset(); off++ {
		key, ts, payload, err := log.ReadKeyed(off)
		if err != nil {
			fmt.Printf("%d: read error: %v\n", off, err)
			continue
		}
		fmt.Printf("%d key=%s ts=%d payload=%s\n", off, key, ts, clip(payload, 160))
	}
}

func clip(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
