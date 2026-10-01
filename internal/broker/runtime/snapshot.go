package runtime

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// partitionAssignmentLister is the preferred metastore capability for
// resolving partition ownership: one local read transaction per topic
// (implemented by *metastore.Store).
type partitionAssignmentLister interface {
	ListAssignments(topicName string) ([]metastore.Assignment, error)
}

// partitionAssignmentReader is the per-partition fallback used when the
// metastore only offers point lookups.
type partitionAssignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
}

// Snapshotter produces the read-only inventory of every topic and
// partition consumed by the metrics poller. Constructed once at
// broker startup and embedded into the broker facade so its Snapshot
// method satisfies the Broker interface.
type Snapshotter struct {
	metastore metastore.Metastore
	offsets   *consumer.InFlight
	logs      *Logs
	logger    *slog.Logger
	selfID    string

	// cold holds what the files of owned partitions said, for the ones
	// whose log is closed or that have no consumer shard; see
	// coldPartition. polls counts Snapshot calls so entries of
	// partitions no longer visited are dropped. Both guarded by coldMu,
	// which a Snapshot holds throughout.
	coldMu sync.Mutex
	cold   map[coldKey]*coldPartition
	polls  uint64

	now func() time.Time // the clock a poll reads; time.Now outside tests
}

// readTopicIncarnation reads a topic directory's incarnation marker. A
// variable so tests can count the reads a poll makes.
var readTopicIncarnation = storage.ReadTopicIncarnation

// coldRefresh bounds how long a reading of a partition's files is
// reused. A closed log's files change only while it is open, which the
// snapshot sees and answers by dropping the reading; the one change it
// can miss is the cold-retention walk opening, sweeping and closing a
// log between two polls, so this is how stale that can leave a closed
// partition's gauges. It keeps an idle partition to a map lookup per
// poll rather than a directory listing and four file reads.
//
// Readings taken in the same poll (all of them after a restart, or a
// batch whose logs were open together) would all expire in the same
// later poll, rereading every closed partition at once. A reading taken
// while its read time is zero is therefore dated back by the partition's
// coldPhase, which is less than coldRefresh: its first refresh comes
// sooner, never later, and from then on the refreshes stay spread over
// the interval, about a sixth of the entries per 5 s poll.
const coldRefresh = 30 * time.Second

// coldPhase is how far a partition's first reading is dated back: an
// FNV-1a hash of the topic name and the partition number, modulo
// coldRefresh. Fixed per partition, so the spread holds across polls.
func coldPhase(topicName string, idx int) time.Duration {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := range len(topicName) {
		h ^= uint64(topicName[i])
		h *= prime64
	}
	v := uint64(idx)
	for range 8 {
		h ^= v & 0xff
		h *= prime64
		v >>= 8
	}
	return time.Duration(h % uint64(coldRefresh))
}

type coldKey struct {
	topic     string
	partition int
}

// coldPartition is what one partition's files said when last read. The
// log part (hwm, segments) is read for a closed log, the frontier part
// for a partition with no consumer shard; each has its own read time,
// zero when not loaded, and dated back by phase when loaded from zero
// (see coldRefresh).
type coldPartition struct {
	seen  uint64        // the poll that last visited it
	dir   string        // the partition directory
	phase time.Duration // coldPhase of the partition

	logAt     time.Time
	logOK     bool // the files describe this incarnation's log
	hwm       int64
	segs      []coldSegment // ascending base offset
	sizeBytes int64

	nextAt time.Time
	next   int64
}

// coldSegment is one segment file of a closed partition.
type coldSegment struct {
	base  int64
	mtime int64 // unix seconds
}

// NewSnapshotter wires a Snapshotter.
func NewSnapshotter(ms metastore.Metastore, offsets *consumer.InFlight, logs *Logs, logger *slog.Logger, selfID string) *Snapshotter {
	return &Snapshotter{
		metastore: ms,
		offsets:   offsets,
		logs:      logs,
		logger:    logger,
		selfID:    selfID,
		now:       time.Now,
	}
}

// Snapshot returns the current runtime state of every topic and
// partition. It is the data source for the metrics package's lag
// poller; the call is intentionally read-only and does not advance
// or mutate any state.
//
// Errors from individual partition lookups are NOT surfaced — a
// missing or transient partition is omitted from the result rather
// than failing the whole snapshot, so a single broken partition
// can't blind operators to the rest of the system.
func (s *Snapshotter) Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error) {
	// Limit=0 is the unpaginated, cached path — appropriate for the
	// poller, which always wants every topic in one shot.
	topics, _, err := s.metastore.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return nil, err
	}

	s.coldMu.Lock()
	defer s.coldMu.Unlock()
	s.polls++
	now := s.now()
	out := make([]metrics.TopicSnapshot, 0, len(topics))
	for _, t := range topics {
		ts := metrics.TopicSnapshot{
			Topic:      t.Name,
			Partitions: make([]metrics.PartitionSnapshot, 0, t.Partitions),
		}
		owned := s.ownedPartitions(t.Name, t.Partitions)
		marker := topicMarker{dataDir: s.logs.DataDir(), topic: t.Name, id: t.ID}
		for i := 0; i < t.Partitions; i++ {
			if !owned(i) {
				continue
			}
			ps, ok := s.topicPartitionSnapshot(t.Name, &marker, i, now)
			if !ok {
				continue
			}
			ts.Partitions = append(ts.Partitions, ps)
		}
		out = append(out, ts)
	}
	for k, e := range s.cold {
		if e.seen != s.polls {
			delete(s.cold, k)
		}
	}
	return out, nil
}

// ownedPartitions returns a predicate reporting whether this node owns
// a partition of the topic. Runtime metrics are intentionally local: in
// the WAL-first design a pod only opens and reports partition logs it
// currently owns, and cross-node aggregation belongs in Prometheus.
// Without a node identity, or a metastore that cannot answer, every
// partition is considered owned.
//
// The assignment set is read in ONE local transaction per topic (a
// prefix scan under the metastore's read lock) rather than one per
// partition cluster-wide, which is what the poller's 5s tick used to
// cost; an unassigned partition, or a listing error, means "not owned"
// exactly as a failed per-partition lookup did.
func (s *Snapshotter) ownedPartitions(topicName string, partitions int) func(int) bool {
	if s.selfID == "" {
		return func(int) bool { return true }
	}
	if lister, ok := s.metastore.(partitionAssignmentLister); ok {
		assignments, err := lister.ListAssignments(topicName)
		if err != nil {
			return func(int) bool { return false }
		}
		owned := make([]bool, partitions)
		for _, a := range assignments {
			if a.OwnerID == s.selfID && a.Partition >= 0 && a.Partition < partitions {
				owned[a.Partition] = true
			}
		}
		return func(i int) bool { return owned[i] }
	}
	if reader, ok := s.metastore.(partitionAssignmentReader); ok {
		return func(i int) bool {
			assignment, err := reader.GetAssignment(topicName, i)
			return err == nil && assignment.OwnerID == s.selfID
		}
	}
	return func(int) bool { return true }
}

// topicMarker is one poll's reading of a topic directory's incarnation
// marker. The first cold load of the topic's partitions that needs it
// reads the file and the topic's other partitions reuse that reading for
// the rest of the poll: at most one read per topic per poll rather than
// one per reloaded partition, and none when no partition reloads.
type topicMarker struct {
	dataDir string
	topic   string
	id      string // the topic's incarnation ID; "" when it has none

	read   bool
	marker string
	marked bool
	err    error
}

// matches reports whether the topic directory may hold this
// incarnation's partitions: always for a topic without an ID, otherwise
// when its marker reads and names no other incarnation. A marker that
// cannot be read counts as a mismatch, so the partitions that needed it
// this poll are omitted.
func (m *topicMarker) matches() bool {
	if m.id == "" {
		return true
	}
	if !m.read {
		m.marker, m.marked, m.err = readTopicIncarnation(storage.TopicDir(m.dataDir, m.topic))
		m.read = true
	}
	return m.err == nil && (!m.marked || m.marker == m.id)
}

// partitionSnapshot builds the snapshot for one owned partition. An
// open log reports live; a closed one (idle-evicted, or not opened since
// a restart) reports what its files say, because a backlog nobody reads
// is exactly what the lag and age gauges exist for, and omitting it made
// the poller delete those series. ok=false when neither exists: the
// partition has no persisted log here, or its directory belongs to
// another incarnation of the name.
//
// The frontier is the consumer shard's when there is one, else the
// persisted one; a shardless partition used to report 0, which counted
// its whole log as lag after every restart. The in-flight and
// acked-ahead sizes are always the shard's (0 without one): idle
// eviction closes the log but keeps the shard, and its leases and
// out-of-order acks are what shows a stalled consumer.
//
// This form reads the topic marker on its own; a poll uses
// topicPartitionSnapshot so the topic's partitions share one reading.
func (s *Snapshotter) partitionSnapshot(topicName, topicID string, idx int, now time.Time) (metrics.PartitionSnapshot, bool) {
	marker := topicMarker{dataDir: s.logs.DataDir(), topic: topicName, id: topicID}
	return s.topicPartitionSnapshot(topicName, &marker, idx, now)
}

// topicPartitionSnapshot is partitionSnapshot with the poll's reading of
// the topic marker, shared by the topic's partitions.
func (s *Snapshotter) topicPartitionSnapshot(topicName string, marker *topicMarker, idx int, now time.Time) (metrics.PartitionSnapshot, bool) {
	next, inFlight, ackedAhead, hasShard := s.offsets.Reservable(topicName, idx)
	// Peek, never Get: a metrics poll must not lazily open (and mkdir) a
	// partition log. Opening here would resurrect directories for a topic
	// being deleted and report partitions this node has never served.
	log, open := s.logs.Peek(topicName, idx)
	if open && hasShard {
		return liveSnapshot(log, idx, next, inFlight, ackedAhead), true
	}

	e := s.coldEntry(topicName, idx)
	if open {
		// The files change while the log is open: read them afresh once
		// it closes.
		e.logAt = time.Time{}
	}
	if hasShard {
		// Acks move the files through the committer: re-read if the
		// shard goes away.
		e.nextAt = time.Time{}
	} else {
		next = e.persistedNext(now)
	}
	if open {
		return liveSnapshot(log, idx, next, inFlight, ackedAhead), true
	}
	if ps, ok := e.snapshot(marker, idx, next, inFlight, ackedAhead, now); ok {
		return ps, true
	}
	// The log may have opened since the Peek (its first advance empties
	// the hwm file): report it live rather than drop its series.
	if log, open = s.logs.Peek(topicName, idx); open {
		e.logAt = time.Time{}
		return liveSnapshot(log, idx, next, inFlight, ackedAhead), true
	}
	return metrics.PartitionSnapshot{}, false
}

// coldEntry returns the partition's cached reading, creating an empty
// one, and marks it visited by this poll. Caller holds coldMu.
func (s *Snapshotter) coldEntry(topicName string, idx int) *coldPartition {
	key := coldKey{topic: topicName, partition: idx}
	e := s.cold[key]
	if e == nil {
		if s.cold == nil {
			s.cold = make(map[coldKey]*coldPartition)
		}
		e = &coldPartition{
			dir:   storage.TopicPartitionDir(s.logs.DataDir(), topicName, idx),
			phase: coldPhase(topicName, idx),
		}
		s.cold[key] = e
	}
	e.seen = s.polls
	return e
}

// persistedNext returns the next offset to deliver according to the
// partition's files, as a restart would recover it: the larger of the
// consumer.offset and consumer.ahead frontiers, collapsed over the
// acked-ahead offsets that continue it. 0 when neither file exists.
func (e *coldPartition) persistedNext(now time.Time) int64 {
	if !e.nextAt.IsZero() && now.Sub(e.nextAt) < coldRefresh {
		return e.next
	}
	next := int64(0)
	if committed, ok, err := storage.ReadConsumerOffset(e.dir); err == nil && ok {
		next = committed + 1
	}
	if rec, ok, err := storage.ReadConsumerAhead(e.dir); err == nil && ok {
		next = max(next, rec.Committed+1)
		for _, off := range rec.Offsets {
			if off == next {
				next++
			} else if off > next {
				break
			}
		}
	}
	e.next, e.nextAt = next, e.readAt(e.nextAt, now)
	return next
}

// readAt is the read time to record for a reading taken now whose
// previous read time was prev: now, or now dated back by the phase when
// prev is zero (a first reading, or one dropped while the files could
// change), so readings taken together do not expire together.
func (e *coldPartition) readAt(prev, now time.Time) time.Time {
	if prev.IsZero() {
		return now.Add(-e.phase)
	}
	return now
}

// snapshot builds a closed partition's snapshot from its files, read
// at most once per coldRefresh, with the given frontier and shard
// sizes. ok=false when there is no persisted log to report.
func (e *coldPartition) snapshot(marker *topicMarker, idx int, next int64, inFlight, ackedAhead int, now time.Time) (metrics.PartitionSnapshot, bool) {
	if e.logAt.IsZero() || now.Sub(e.logAt) >= coldRefresh {
		e.load(marker)
		e.logAt = e.readAt(e.logAt, now)
	}
	if !e.logOK {
		return metrics.PartitionSnapshot{}, false
	}
	logStart := e.hwm
	if len(e.segs) > 0 {
		logStart = e.segs[0].base
	}
	ps := metrics.PartitionSnapshot{
		Partition:       idx,
		LogStartOffset:  logStart,
		LogEndOffset:    e.hwm,
		HighWatermark:   e.hwm,
		SegmentCount:    len(e.segs),
		SizeBytes:       e.sizeBytes,
		CommittedOffset: next,
		InFlightSize:    inFlight,
		AckedAheadSize:  ackedAhead,
	}
	if next < logStart {
		ps.Dropped = logStart - next
	}
	if next < e.hwm && next >= logStart {
		// The segment holding next is the last one starting at or
		// below it.
		i, _ := slices.BinarySearchFunc(e.segs, next, func(s coldSegment, off int64) int {
			if s.base <= off {
				return -1
			}
			return 1
		})
		if i > 0 {
			ps.OldestUnconsumedAt = e.segs[i-1].mtime
		}
	}
	return ps, true
}

// load reads the closed log's persisted high-watermark (exact for a
// cleanly closed log) and one listing of its segment files. A directory
// whose topic marker names another incarnation is a deleted namesake's
// leftover, not this topic's backlog; the marker is the topic's, read
// once per poll by the first partition that needs it.
func (e *coldPartition) load(marker *topicMarker) {
	e.logOK, e.hwm, e.segs, e.sizeBytes = false, 0, e.segs[:0], 0
	if !marker.matches() {
		return
	}
	hwm, ok, err := storage.ReadPersistedHighWatermark(e.dir)
	if err != nil || !ok {
		return
	}
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return
	}
	for _, de := range entries {
		base, ok := storage.ParseSegmentFileName(de.Name())
		if !ok || de.IsDir() {
			continue
		}
		info, err := de.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return
		}
		e.segs = append(e.segs, coldSegment{base: base, mtime: info.ModTime().Unix()})
		e.sizeBytes += info.Size()
	}
	// Names are zero-padded, so the listing is already in offset order.
	e.hwm, e.logOK = hwm, true
}

// liveSnapshot builds an open log's snapshot with the given frontier.
func liveSnapshot(log *storage.Log, idx int, committed int64, inFlight, ackedAhead int) metrics.PartitionSnapshot {
	logStart := log.OldestOffset()
	logEnd := log.NextOffset()

	ps := metrics.PartitionSnapshot{
		Partition:       idx,
		LogStartOffset:  logStart,
		LogEndOffset:    logEnd,
		HighWatermark:   log.HighWatermark(),
		SegmentCount:    log.SegmentCount(),
		SizeBytes:       log.SizeBytes(),
		CommittedOffset: committed,
		InFlightSize:    inFlight,
		AckedAheadSize:  ackedAhead,
	}

	if committed < logStart {
		ps.Dropped = logStart - committed
	}

	if committed < logEnd && committed >= logStart {
		if mt, ok := log.SegmentMTimeForOffset(committed); ok {
			ps.OldestUnconsumedAt = mt
		}
	}

	return ps
}
