package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP23Disk models stable storage under the committer's files: the
// bytes each one holds after a power loss, given the syncs the
// committer issued. A writeout captures the file's current bytes; on
// the darwin model they reach stable storage only at the next device
// flush, on the linux model at once. A lying disk promotes nothing. A
// file the committer created exists after a power loss only once its
// directory was synced.
type zzWP23Disk struct {
	mu sync.Mutex
	// darwin: writeouts wait for a device flush.
	darwin bool
	// lie: every sync reports success and makes nothing durable.
	lie bool
	// failWriteOut and failFlush, when set, fail the matching call.
	failWriteOut func(path string) error
	failFlush    func() error

	durable map[string][]byte
	staged  map[string][]byte
	// named: files whose directory entry is durable.
	named map[string]bool
	// ops logs every call in order: "writeout <path>", "flush", "dirsync <dir>".
	ops       []string
	writeOuts int
	flushes   int
}

// newZZWP23Disk starts the model with every file already under dataDir
// durable as it is: fixtures and files of an earlier process.
func newZZWP23Disk(t testing.TB, dataDir string, darwin bool) *zzWP23Disk {
	t.Helper()
	d := &zzWP23Disk{
		darwin:  darwin,
		durable: make(map[string][]byte),
		staged:  make(map[string][]byte),
		named:   make(map[string]bool),
	}
	err := filepath.WalkDir(dataDir, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		d.durable[path] = b
		d.named[path] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (d *zzWP23Disk) io() offsetIO {
	return offsetIO{writeOut: d.writeOut, flushDevice: d.flushDevice, syncDir: d.syncDir}
}

func (d *zzWP23Disk) writeOut(f *os.File) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ops = append(d.ops, "writeout "+f.Name())
	d.writeOuts++
	if d.failWriteOut != nil {
		if err := d.failWriteOut(f.Name()); err != nil {
			return err
		}
	}
	if d.lie {
		return nil
	}
	b, err := os.ReadFile(f.Name())
	if err != nil {
		return err
	}
	if d.darwin {
		d.staged[f.Name()] = b
	} else {
		d.durable[f.Name()] = b
	}
	return nil
}

func (d *zzWP23Disk) flushDevice(*os.File) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ops = append(d.ops, "flush")
	d.flushes++
	if d.failFlush != nil {
		if err := d.failFlush(); err != nil {
			return err
		}
	}
	if d.lie {
		return nil
	}
	for p, b := range d.staged {
		d.durable[p] = b
	}
	clear(d.staged)
	return nil
}

func (d *zzWP23Disk) syncDir(dir *os.File) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ops = append(d.ops, "dirsync "+dir.Name())
	if d.lie {
		return nil
	}
	entries, err := os.ReadDir(dir.Name())
	if err != nil {
		return err
	}
	for _, e := range entries {
		d.named[filepath.Join(dir.Name(), e.Name())] = true
	}
	return nil
}

// zzWP23Image is one file's possible content after a power loss;
// present=false is a file that is not there.
type zzWP23Image struct {
	data    []byte
	present bool
}

// images lists what path can hold after a power loss: the durable
// bytes, the current bytes, and sector-torn mixes of the two over each
// slot where they differ (every prefix and every suffix of 512-byte
// sectors from the newer bytes). A file whose name is not durable may
// also be missing.
func (d *zzWP23Disk) images(t testing.TB, path string) []zzWP23Image {
	t.Helper()
	d.mu.Lock()
	durable, hasDurable := d.durable[path]
	named := d.named[path]
	d.mu.Unlock()
	cur, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		cur = nil
	} else if err != nil {
		t.Fatal(err)
	}
	var out []zzWP23Image
	add := func(b []byte) {
		for _, im := range out {
			if im.present && bytes.Equal(im.data, b) {
				return
			}
		}
		out = append(out, zzWP23Image{data: slices.Clone(b), present: true})
	}
	if !named || !hasDurable {
		out = append(out, zzWP23Image{})
	}
	if hasDurable {
		add(durable)
	}
	if cur == nil {
		return out
	}
	add(cur)
	base := durable
	if !hasDurable {
		base = nil
	}
	const sector = 512
	size := max(len(base), len(cur))
	for slot := 0; slot*storage.ConsumerAheadSlotSize < size; slot++ {
		lo := slot * storage.ConsumerAheadSlotSize
		hi := min(lo+storage.ConsumerAheadSlotSize, size)
		if bytes.Equal(zzWP23Range(base, lo, hi), zzWP23Range(cur, lo, hi)) {
			continue
		}
		for cut := lo + sector; cut < hi; cut += sector {
			prefix := zzWP23Pad(base, size)
			copy(prefix[lo:cut], zzWP23Range(cur, lo, cut))
			add(prefix)
			suffix := zzWP23Pad(base, size)
			copy(suffix[cut:hi], zzWP23Range(cur, cut, hi))
			add(suffix)
		}
	}
	return out
}

func zzWP23Range(b []byte, lo, hi int) []byte {
	out := make([]byte, hi-lo)
	if lo < len(b) {
		copy(out, b[lo:min(hi, len(b))])
	}
	return out
}

func zzWP23Pad(b []byte, size int) []byte {
	out := make([]byte, size)
	copy(out, b)
	return out
}

// zzWP23Acked is the acked state of one partition: every offset up to
// frontier and every offset in ahead.
type zzWP23Acked struct {
	frontier int64
	ahead    map[int64]bool
}

func (a zzWP23Acked) has(off int64) bool { return off <= a.frontier || a.ahead[off] }

// zzWP23Shard is a fake shard for one partition: acks move its frontier
// and its acked-ahead set, and its snapshot is what the committer
// writes.
type zzWP23Shard struct {
	mu      sync.Mutex
	acked   zzWP23Acked
	version uint64
	gone    bool
}

func newZZWP23Shard(frontier int64) *zzWP23Shard {
	return &zzWP23Shard{acked: zzWP23Acked{frontier: frontier, ahead: map[int64]bool{}}}
}

// ack records off as acked and collapses the frontier over any run of
// acked-ahead offsets right above it.
func (s *zzWP23Shard) ack(off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acked.has(off) {
		return
	}
	s.acked.ahead[off] = true
	for s.acked.ahead[s.acked.frontier+1] {
		delete(s.acked.ahead, s.acked.frontier+1)
		s.acked.frontier++
	}
	s.version++
}

func (s *zzWP23Shard) snapshot() (int64, []int64, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gone {
		return 0, nil, 0, false
	}
	offsets := make([]int64, 0, len(s.acked.ahead))
	for off := range s.acked.ahead {
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return s.acked.frontier, offsets, s.version, true
}

func (s *zzWP23Shard) state() zzWP23Acked {
	s.mu.Lock()
	defer s.mu.Unlock()
	ahead := make(map[int64]bool, len(s.acked.ahead))
	for off := range s.acked.ahead {
		ahead[off] = true
	}
	return zzWP23Acked{frontier: s.acked.frontier, ahead: ahead}
}

// zzWP23Shards is an AheadSource over fake shards of topic "t".
type zzWP23Shards struct {
	mu     sync.Mutex
	shards map[int]*zzWP23Shard
}

func (s *zzWP23Shards) source(topic string, p int) (int64, []int64, uint64, bool) {
	s.mu.Lock()
	sh := s.shards[p]
	s.mu.Unlock()
	if sh == nil || topic != "t" {
		return 0, nil, 0, false
	}
	return sh.snapshot()
}

func (s *zzWP23Shards) set(p int, sh *zzWP23Shard) {
	s.mu.Lock()
	if s.shards == nil {
		s.shards = make(map[int]*zzWP23Shard)
	}
	s.shards[p] = sh
	s.mu.Unlock()
}

// zzWP23Rig is a manual committer over real partition directories of
// topic "t" and a disk model.
type zzWP23Rig struct {
	t       testing.TB
	dataDir string
	disk    *zzWP23Disk
	c       *ConsumerOffsetCommitter
	shards  *zzWP23Shards
	now     time.Time
	// crashAt, when set, ends a tick at the first point it returns
	// true for.
	crashAt func(point offsetPoint, key offsetCommitKey) bool
	crashed bool
}

type zzWP23RigOpts struct {
	parts    int
	interval time.Duration
	darwin   bool
	maxFDs   int
	burst    bool
	// fixture runs before the disk model starts, to lay down files an
	// earlier process left.
	fixture func(dataDir string)
}

func newZZWP23Rig(t testing.TB, o zzWP23RigOpts) *zzWP23Rig {
	t.Helper()
	dataDir := t.TempDir()
	for p := range o.parts {
		if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "t", p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if o.fixture != nil {
		o.fixture(dataDir)
	}
	r := &zzWP23Rig{
		t:       t,
		dataDir: dataDir,
		disk:    newZZWP23Disk(t, dataDir, o.darwin),
		shards:  &zzWP23Shards{},
		now:     time.Unix(1_900_000_000, 0),
	}
	if o.interval == 0 {
		o.interval = time.Second
	}
	r.c = newConsumerOffsetCommitter(dataDir, o.interval, nil, committerOptions{
		io:     r.disk.io(),
		manual: true,
		maxFDs: o.maxFDs,
		burst:  o.burst,
		crash: func(point offsetPoint, key offsetCommitKey) bool {
			if r.crashAt != nil && r.crashAt(point, key) {
				r.crashed = true
				return true
			}
			return false
		},
	})
	r.c.SetAheadSource(r.shards.source)
	t.Cleanup(r.abandon)
	return r
}

// tick advances the rig's clock by one T and runs a tick.
func (r *zzWP23Rig) tick() error {
	r.now = r.now.Add(r.c.tick)
	return r.c.tickAt(r.now, offsetTickNormal)
}

// abandon closes the committer's descriptors without writing anything:
// the process died.
func (r *zzWP23Rig) abandon() {
	r.c.ioMu.Lock()
	for _, st := range r.c.parts {
		if st.f != nil {
			_ = st.f.Close()
			st.f = nil
		}
	}
	r.c.ioMu.Unlock()
}

func (r *zzWP23Rig) dir(p int) string { return storage.TopicPartitionDir(r.dataDir, "t", p) }

// durableFrontier is the frontier of the record the committer last
// made the anchor for partition p (-1: none).
func (r *zzWP23Rig) durableFrontier(p int) int64 {
	r.c.ioMu.Lock()
	defer r.c.ioMu.Unlock()
	if st := r.c.parts[offsetCommitKey{"t", p}]; st != nil {
		return st.durable
	}
	return -1
}

// dirty reports whether partition p's window holds a record not yet
// written out.
func (r *zzWP23Rig) dirty(p int) bool {
	r.c.ioMu.Lock()
	defer r.c.ioMu.Unlock()
	st := r.c.parts[offsetCommitKey{"t", p}]
	return st != nil && !st.dirtySince.IsZero()
}

// zzWP23Recovered is what one recovery made of a partition directory.
type zzWP23Recovered struct {
	frontier int64
	ahead    []int64
}

// recoverNew recovers dir the way the broker does: the serve wiring's
// two readers and the shard seeding of consumer.InFlight.
func zzWP23RecoverNew(t testing.TB, dir string) zzWP23Recovered {
	t.Helper()
	f := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1 << 20, MaxAckedAhead: 1 << 20}, nil
	}, nil)
	f.SetCommittedRecovery(func(string, int) (int64, bool) {
		v, ok, err := storage.ReadConsumerOffset(dir)
		if err != nil {
			return 0, false
		}
		return v, ok
	})
	f.SetAheadRecovery(func(string, int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(dir)
		if err != nil {
			return 0, nil, false
		}
		return rec.Committed, rec.Offsets, ok
	})
	if _, err := f.ReserveNext(context.Background(), "t", 0, time.Minute, 0); err != nil {
		t.Fatal(err)
	}
	committed, offsets, _, _ := f.AheadSnapshot("t", 0)
	return zzWP23Recovered{frontier: committed, ahead: offsets}
}

// checkImages builds every power-loss image of partition p's two files
// and recovers each twice, with the broker's recovery and with
// v3.0.1's reader. Every recovered offset must have been acked; unless
// the disk lied, the frontier must be at or above the last anchor.
func (r *zzWP23Rig) checkImages(p int, acked zzWP23Acked, floor int64) int {
	r.t.Helper()
	dir := r.dir(p)
	aheadPath := filepath.Join(dir, storage.ConsumerAheadFileName)
	offsetPath := filepath.Join(dir, storage.ConsumerOffsetFileName)
	aheads := r.disk.images(r.t, aheadPath)
	offs := r.disk.images(r.t, offsetPath)
	scratch := r.t.TempDir()
	n := 0
	for _, a := range aheads {
		for _, o := range offs {
			zzWP23Put(r.t, filepath.Join(scratch, storage.ConsumerAheadFileName), a)
			zzWP23Put(r.t, filepath.Join(scratch, storage.ConsumerOffsetFileName), o)
			got := zzWP23RecoverNew(r.t, scratch)
			old := zzWP23RecoverV301(a, o)
			for name, rec := range map[string]zzWP23Recovered{"broker": got, "v3.0.1": old} {
				desc := fmt.Sprintf("partition %d image (ahead %d bytes present %v, offset %d bytes present %v) %s recovery %+v",
					p, len(a.data), a.present, len(o.data), o.present, name, rec)
				for off := int64(0); off <= rec.frontier; off++ {
					if !acked.has(off) {
						r.t.Fatalf("%s: frontier passes unacked offset %d (acked frontier %d)", desc, off, acked.frontier)
					}
				}
				for _, off := range rec.ahead {
					if !acked.has(off) {
						r.t.Fatalf("%s: recovered unacked offset %d as acked", desc, off)
					}
				}
				if !r.disk.lie && rec.frontier < floor {
					r.t.Fatalf("%s: frontier %d below the last durable anchor %d", desc, rec.frontier, floor)
				}
			}
			n++
		}
	}
	return n
}

func zzWP23Put(t testing.TB, path string, im zzWP23Image) {
	t.Helper()
	if !im.present {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		return
	}
	if err := os.WriteFile(path, im.data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// zzWP23ManualCommitter is a committer with no loop, on the default
// durability interval: ticks happen only through flush, tickAt and
// Close.
func zzWP23ManualCommitter(dataDir string) *ConsumerOffsetCommitter {
	return newConsumerOffsetCommitter(dataDir, 0, nil, committerOptions{manual: true})
}
