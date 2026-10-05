package runtime

// Quarantined copies: partition data this node set aside instead of
// deleting, because it may hold the only instance of some records.
//
//   - partition: a stale copy a reclaim found ahead of the position its
//     new owner vouches for, or one the owner cannot vouch for, and an
//     earlier copy a move's install found at the partition's path:
//     topics/<topic>/p<NNNNN>.quarantine[.<unix-nanos>];
//   - topic_incarnation: a deleted topic incarnation's directory an open
//     (or the move runner's sweep) found under a live topic's name:
//     topics/<topic>.stale-<id>[.<n>]. The sweeps remove it once the
//     leader confirms the incarnation gone;
//   - staging: a move's staging copy set aside when it may hold records
//     the partition's path lacks: .moves/<topic>-<partition>.quarantine[.<unix-nanos>].
//
// Each set-aside logs one error line when it happens. QuarantinedCopies
// keeps them in view afterwards: the move runner's sweep and startup
// refresh the inventory, and the gauges and the startup listing read it.
// Nothing here removes a copy.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Kinds of quarantined copy (QuarantinedCopy.Kind).
const (
	QuarantineKindPartition        = "partition"
	QuarantineKindTopicIncarnation = "topic_incarnation"
	QuarantineKindStaging          = "staging"
)

// partitionQuarantineSuffix is messaging.QuarantineSuffix, repeated
// here because messaging imports this package.
const partitionQuarantineSuffix = ".quarantine"

// moveStagingDir is the move runner's staging root under the data
// directory (cluster.MoveRunner stages a partition copy in
// .moves/<topic>-<partition>).
const moveStagingDir = ".moves"

// quarantineListCap bounds how many copies a QuarantineSummary lists;
// its totals count every copy.
const quarantineListCap = 1000

// QuarantinedCopy is one set-aside copy on this node.
type QuarantinedCopy struct {
	Kind  string `json:"kind"`
	Topic string `json:"topic"`
	// Partition is the partition index, or -1 for a whole topic
	// directory (or a staging name that does not parse).
	Partition int       `json:"partition"`
	Dir       string    `json:"dir"`
	Bytes     int64     `json:"bytes"`
	ModTime   time.Time `json:"mod_time"`
}

// QuarantineSummary is one inventory of this node's quarantined copies.
type QuarantineSummary struct {
	// Copies lists the first quarantineListCap copies found.
	Copies []QuarantinedCopy `json:"copies"`
	// Count and Bytes total every copy found, listed or not.
	Count int   `json:"count"`
	Bytes int64 `json:"bytes"`
	// ScannedAt is when the inventory was taken.
	ScannedAt time.Time `json:"scanned_at"`
}

// QuarantinedCopies inventories this node's quarantined copies and
// keeps the result for LastQuarantinedCopies and the quarantine gauges.
// It walks the copies' files to size them, so callers run it on the
// sweep cadence and at startup, never per scrape. A directory that
// cannot be read is skipped and the first such error returned with
// everything else that was found (which is still kept).
func (g *Logs) QuarantinedCopies() (QuarantineSummary, error) {
	var (
		sum      QuarantineSummary
		firstErr error
	)
	note := func(err error) {
		if err != nil && !errors.Is(err, fs.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	add := func(kind, topicName string, partition int, dir string) {
		q, err := describeQuarantine(kind, topicName, partition, dir)
		if err != nil {
			note(err)
			return
		}
		sum.Count++
		sum.Bytes += q.Bytes
		if len(sum.Copies) < quarantineListCap {
			sum.Copies = append(sum.Copies, q)
		}
	}

	topicsRoot := filepath.Join(g.dataDir, "topics")
	entries, err := os.ReadDir(topicsRoot)
	note(err)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		c, err := classifyTopicDir(topicsRoot, e.Name())
		if err != nil {
			note(err)
			continue
		}
		if c.Quarantined {
			add(QuarantineKindTopicIncarnation, c.Topic, -1, c.Dir)
			continue
		}
		parts, err := os.ReadDir(c.Dir)
		if err != nil {
			note(err)
			continue
		}
		for _, p := range parts {
			if idx, ok := parsePartitionQuarantine(p.Name()); ok && p.IsDir() {
				add(QuarantineKindPartition, c.Topic, idx, filepath.Join(c.Dir, p.Name()))
			}
		}
	}

	stagingRoot := filepath.Join(g.dataDir, moveStagingDir)
	staged, err := os.ReadDir(stagingRoot)
	note(err)
	for _, e := range staged {
		base, ok := quarantineBase(e.Name())
		if !ok || !e.IsDir() {
			continue
		}
		topicName, partition := base, -1
		if i := strings.LastIndexByte(base, '-'); i > 0 {
			if n, err := strconv.Atoi(base[i+1:]); err == nil && n >= 0 {
				topicName, partition = base[:i], n
			}
		}
		add(QuarantineKindStaging, topicName, partition, filepath.Join(stagingRoot, e.Name()))
	}

	sum.ScannedAt = time.Now()
	kept := sum
	g.quarantine.Store(&kept)
	return sum, firstErr
}

// LastQuarantinedCopies returns the inventory QuarantinedCopies last
// took; ok=false before the first one.
func (g *Logs) LastQuarantinedCopies() (sum QuarantineSummary, ok bool) {
	p := g.quarantine.Load()
	if p == nil {
		return QuarantineSummary{}, false
	}
	return *p, true
}

// parsePartitionQuarantine parses p<NNNNN>.quarantine and
// p<NNNNN>.quarantine.<digits> into the partition index.
func parsePartitionQuarantine(name string) (int, bool) {
	base, ok := quarantineBase(name)
	if !ok {
		return 0, false
	}
	return storage.ParsePartitionDirName(base)
}

// quarantineBase strips a set-aside suffix (.quarantine, or
// .quarantine.<digits> when that name was taken) and returns what it was
// appended to. ok=false when name carries no such suffix.
func quarantineBase(name string) (string, bool) {
	i := strings.LastIndex(name, partitionQuarantineSuffix)
	if i <= 0 {
		return "", false
	}
	rest := name[i+len(partitionQuarantineSuffix):]
	if rest != "" {
		digits, ok := strings.CutPrefix(rest, ".")
		if !ok || digits == "" {
			return "", false
		}
		if _, err := strconv.ParseUint(digits, 10, 64); err != nil {
			return "", false
		}
	}
	return name[:i], true
}

// describeQuarantine stats a set-aside directory: its modification time
// and the bytes of every regular file under it. Symlinks are not
// followed.
func describeQuarantine(kind, topicName string, partition int, dir string) (QuarantinedCopy, error) {
	st, err := os.Lstat(dir)
	if err != nil {
		return QuarantinedCopy{}, err
	}
	var bytes int64
	err = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		bytes += info.Size()
		return nil
	})
	if err != nil {
		return QuarantinedCopy{}, fmt.Errorf("runtime: size quarantined copy %s: %w", dir, err)
	}
	return QuarantinedCopy{Kind: kind, Topic: topicName, Partition: partition, Dir: dir, Bytes: bytes, ModTime: st.ModTime()}, nil
}

// quarantineCollector exports the last inventory (see
// NewQuarantineCollector).
type quarantineCollector struct {
	logs   *Logs
	copies *prometheus.Desc
	bytes  *prometheus.Desc
}

// NewQuarantineCollector returns a collector over the inventory
// QuarantinedCopies last took:
//
//   - narad_quarantined_copies: partition copies this node set aside
//     instead of deleting (every kind QuarantinedCopies lists);
//   - narad_quarantined_bytes: the bytes those copies hold.
//
// A scrape reads the kept inventory and never walks the disk; before
// the first inventory it exports nothing.
func NewQuarantineCollector(logs *Logs) prometheus.Collector {
	return &quarantineCollector{
		logs: logs,
		copies: prometheus.NewDesc("narad_quarantined_copies",
			"Partition copies this node set aside instead of deleting (reclaim and install set-asides, set-aside move staging copies, directories of deleted topic incarnations). Each may hold the only instance of some records.",
			nil, nil),
		bytes: prometheus.NewDesc("narad_quarantined_bytes",
			"Bytes held by the partition copies this node set aside instead of deleting.",
			nil, nil),
	}
}

func (c *quarantineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.copies
	ch <- c.bytes
}

func (c *quarantineCollector) Collect(ch chan<- prometheus.Metric) {
	if c.logs == nil {
		return
	}
	sum, ok := c.logs.LastQuarantinedCopies()
	if !ok {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.copies, prometheus.GaugeValue, float64(sum.Count))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(sum.Bytes))
}
