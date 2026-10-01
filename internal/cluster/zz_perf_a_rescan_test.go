package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzPerfASink is a local committer that takes delay per commit (the
// owner's fsync) and records every WAL seq it committed, per partition,
// in commit order.
type zzPerfASink struct {
	delay     time.Duration
	committed atomic.Int64
	mu        sync.Mutex
	seqs      map[int][]uint64
}

func (s *zzPerfASink) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := s.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (s *zzPerfASink) CommitAcceptedProduceBatch(_ context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.mu.Lock()
	if s.seqs == nil {
		s.seqs = map[int][]uint64{}
	}
	for _, r := range recs {
		s.seqs[r.TargetPartition] = append(s.seqs[r.TargetPartition], r.WAL.Seq)
	}
	s.mu.Unlock()
	s.committed.Add(int64(len(recs)))
	return make([]int64, len(recs)), nil
}

// zzPerfACheckOnceInOrder fails unless s committed n records in all,
// each partition's in strictly increasing seq order: so every record
// exactly once, in WAL order per partition.
func zzPerfACheckOnceInOrder(t *testing.T, s *zzPerfASink, n int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for part, seqs := range s.seqs {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("partition %d: seq %d committed after seq %d (commit %d of %d): a duplicate or out of order",
					part, seqs[i], seqs[i-1], i, len(seqs))
			}
		}
		total += len(seqs)
	}
	if total != n {
		t.Fatalf("committed %d records, want each of %d exactly once", total, n)
	}
}

// zzPerfARunUntil runs d's Run loop until done reports true or limit
// passes, then stops it and waits for it to return, so the test may read
// d.state afterwards.
func zzPerfARunUntil(t *testing.T, d *ProduceDispatcher, done func() bool, limit time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { d.Run(ctx); close(stopped) }()
	deadline := time.Now().Add(limit)
	for !done() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-stopped
	if !done() {
		t.Fatalf("the Run loop did not finish within %v", limit)
	}
}

// Item 1 of the PCA perf follow-up. A lone hot destination fills its
// queue (perDestCap) at once, so the rest of its backlog up to the
// lookahead horizon is skipped; after each commit, the rescan read every
// one of those skipped records again (a decode each), placed a queue's
// worth and skipped the rest again: about 11 decodes per record. A
// rescan now stops once no destination it could place records for is
// left, so each skipped record is read again about once. Every record
// still commits exactly once, in WAL order. The test counts reads, not
// time, so a slow runner does not change its outcome.
func TestPerfARescanReadsEachSkippedRecordAboutOnce(t *testing.T) {
	const n = 131072
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 1)
	m := zzWP6Manager(t)
	zzPerfAFill(t, m, n, 1)
	sink := &zzPerfASink{delay: time.Millisecond}
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	zzPerfARunUntil(t, d, func() bool { return sink.committed.Load() >= n }, 3*time.Minute)

	zzPerfACheckOnceInOrder(t, sink, n)
	st := d.state
	if st.skipped != 0 || st.held != 0 || st.nextSeq != m.DurableProduceNext() {
		t.Fatalf("after the drain: skipped=%d held=%d checkpoint=%d durable=%d; want 0, 0 and the checkpoint at the durable frontier",
			st.skipped, st.held, st.nextSeq, m.DurableProduceNext())
	}
	t.Logf("%d skipped records read again for %d records (%.2f per record)", st.rereads, n, float64(st.rereads)/n)
	if limit := uint64(n) * 11 / 10; st.rereads > limit {
		t.Fatalf("the rescans read %d skipped records again for %d records (%.2f per record), want at most %d: each skipped record read again about once",
			st.rereads, n, float64(st.rereads)/n, limit)
	}
}

// zzPerfAPayload is record number i's payload: i, then padding.
func zzPerfAPayload(i int) []byte {
	p := make([]byte, 32)
	binary.BigEndian.PutUint64(p, uint64(i))
	copy(p[8:], "xxxxxxxxxxxxxxxxxxxxxxxx")
	return p
}

// zzPerfANumber is the record number a zzPerfAPayload carries.
func zzPerfANumber(p []byte) int { return int(binary.BigEndian.Uint64(p)) }

// zzPerfAAccept accepts one record per entry of parts, on that partition
// of "orders", numbered from first on, in AcceptProduceBatch calls of up
// to 1024 records (one WAL group commit each). It returns the next free
// number.
func zzPerfAAccept(t *testing.T, m *ingress.Manager, first int, parts []int) int {
	t.Helper()
	batch := make([]ingress.BatchRecord, 0, 1024)
	for i, part := range parts {
		batch = append(batch, ingress.BatchRecord{Key: "k", TargetPartition: part, Payload: zzPerfAPayload(first + i)})
		if len(batch) == cap(batch) || i == len(parts)-1 {
			if _, err := m.AcceptProduceBatch(context.Background(), "orders", "", batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	return first + len(parts)
}

// zzPerfARepeat is n copies of part.
func zzPerfARepeat(part, n int) []int {
	parts := make([]int, n)
	for i := range parts {
		parts[i] = part
	}
	return parts
}

// zzPerfAWant is, per partition, the record numbers parts puts on it, in
// order.
func zzPerfAWant(parts []int) map[int][]int {
	want := map[int][]int{}
	for i, part := range parts {
		want[part] = append(want[part], i)
	}
	return want
}

// zzPerfAGateSink is a local committer for "orders". Partition 0's
// commits each wait for a send on release, or for open to be closed;
// partition 1's first commit after failNext is set fails. It records the
// record numbers it committed, per partition, in commit order, and the
// partition of every commit that starts on calls.
type zzPerfAGateSink struct {
	calls    chan int
	release  chan struct{}
	open     chan struct{}
	failNext atomic.Bool
	mu       sync.Mutex
	got      map[int][]int
}

func newZZPerfAGateSink() *zzPerfAGateSink {
	return &zzPerfAGateSink{calls: make(chan int, 64), release: make(chan struct{}), open: make(chan struct{}), got: map[int][]int{}}
}

func (s *zzPerfAGateSink) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := s.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (s *zzPerfAGateSink) CommitAcceptedProduceBatch(ctx context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	part := recs[0].TargetPartition
	s.calls <- part
	switch part {
	case 0:
		select {
		case <-s.release:
		case <-s.open:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case 1:
		if s.failNext.CompareAndSwap(true, false) {
			return nil, errZZPerfAInjected
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range recs {
		s.got[r.TargetPartition] = append(s.got[r.TargetPartition], zzPerfANumber(r.Payload))
	}
	return make([]int64, len(recs)), nil
}

func (s *zzPerfAGateSink) committed() map[int][]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	got := map[int][]int{}
	for part, nums := range s.got {
		got[part] = slices.Clone(nums)
	}
	return got
}

// waitCalls waits for one commit to start per entry of parts, in any
// order.
func (s *zzPerfAGateSink) waitCalls(t *testing.T, parts ...int) {
	t.Helper()
	var got []int
	for range parts {
		select {
		case part := <-s.calls:
			got = append(got, part)
		case <-time.After(5 * time.Second):
			t.Fatalf("commits started for partitions %v, want %v", got, parts)
		}
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(parts))
	if !slices.Equal(got, want) {
		t.Fatalf("commits started for partitions %v, want %v", got, want)
	}
}

var errZZPerfAInjected = errors.New("injected commit failure")

// zzPerfADone reports whether d has nothing left to do: nothing in
// flight, held or skipped, and the checkpoint at the durable frontier.
func zzPerfADone(d *ProduceDispatcher) bool {
	st := d.state
	return st.outstanding == 0 && st.held == 0 && st.skipped == 0 && st.nextSeq == d.ingress.DurableProduceNext()
}

// zzPerfAStepUntilDone drives d the way Run does, one round at a time on
// clock, until zzPerfADone: after each round, the next finished commit
// is merged, or, with none in flight, the clock moves on to what falls
// due next.
func zzPerfAStepUntilDone(t *testing.T, d *ProduceDispatcher, clock *zzWP6Clock) {
	t.Helper()
	ctx := context.Background()
	st := d.state
	for range 100000 {
		st.pass++
		st.err, st.stalled = nil, false
		d.step(ctx, st)
		if zzPerfADone(d) {
			return
		}
		if st.outstanding > 0 {
			zzShipSlowMerge(t, d)
			continue
		}
		clock.Advance(max(d.nextWake(st), time.Millisecond))
	}
	t.Fatalf("not drained: outstanding=%d held=%d skipped=%d checkpoint=%d durable=%d",
		st.outstanding, st.held, st.skipped, st.nextSeq, d.ingress.DurableProduceNext())
}

// zzPerfAStuckBacklog accepts, for "orders": three queues' worth of
// partition 0 (A) and 50 more, then ten records of partition 1 (B), then
// 100 more of A. It returns the partitions, indexed by record number.
func zzPerfAStuckBacklog(t *testing.T, m *ingress.Manager, capacity int) []int {
	t.Helper()
	parts := zzPerfARepeat(0, 3*capacity+50)
	parts = append(parts, zzPerfARepeat(1, 10)...)
	parts = append(parts, zzPerfARepeat(0, 100)...)
	zzPerfAAccept(t, m, 0, parts)
	return parts
}

// A rescan stops once no destination it may place records for is left.
// A hot destination A that the rescan fills and skips again drops out
// first, but a destination B with skipped records after A's must still
// get them all from that rescan: here B's were returned to the WAL by a
// failed commit, and the rescan starts at A's first skipped record
// because A's commit and B's recovery both asked for it. Run's steps
// are driven one at a time on a settable clock.
func TestPerfARescanStillReachesOtherDestinations(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 2)
	m := zzWP6Manager(t)
	sink := newZZPerfAGateSink()
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	clock := &zzWP6Clock{now: time.Unix(1_700_000_000, 0)}
	d.now = clock.Now
	// Short, so B's retry falls due long before any commit counts as slow.
	d.failureBackoff = 10 * time.Millisecond
	if err := d.loadCursor(); err != nil {
		t.Fatal(err)
	}
	st := d.state
	parts := zzPerfAStuckBacklog(t, m, d.perDestCap(st))

	// A's first queue and B's ten records go out; B's commit fails, so B
	// keeps its first record as the probe and returns nine to the WAL.
	sink.failNext.Store(true)
	d.step(ctx, st)
	sink.waitCalls(t, 0, 1)
	zzShipSlowMerge(t, d)
	a := st.dests[dispatchDestKey{topic: "orders", partition: 0}]
	b := st.dests[dispatchDestKey{topic: "orders", partition: 1}]
	if a == nil || b == nil || !b.failing() || b.skipped != 9 || b.held != 1 {
		t.Fatalf("after B's failed commit: want B failing with 1 probe held and 9 records skipped, got %+v", b)
	}

	// A's commit lands and asks for a rescan. B's retry falls due before
	// the next round, which rescans: it refills A's queue and blocks A,
	// and skips B's records again (B still failing). Then A's second
	// commit and B's probe go out together.
	sink.release <- struct{}{}
	zzShipSlowMerge(t, d)
	clock.Advance(d.failureBackoff)
	d.step(ctx, st)
	sink.waitCalls(t, 0, 1)
	if b.skipped != 9 {
		t.Fatalf("B skipped %d while failing, want 9", b.skipped)
	}

	// B's probe commits: B takes commits again and asks for a rescan.
	// A's second commit lands before the next round and asks for one
	// from its own first skipped record, below B's. (Merging both
	// before a round keeps this so whether or not a commit asks for a
	// rescan as it starts.)
	zzShipSlowMerge(t, d)
	recovered := clock.Now()
	if b.failing() || b.skipped != 9 || !st.rescanDue {
		t.Fatalf("after B's probe: failing=%v skipped=%d rescanDue=%v, want B healthy with 9 skipped and a rescan due", b.failing(), b.skipped, st.rescanDue)
	}
	sink.release <- struct{}{}
	zzShipSlowMerge(t, d)
	if st.rescanFrom.Seq != a.firstSkipped.Seq || a.firstSkipped.Seq >= b.firstSkipped.Seq {
		t.Fatalf("rescan from %d, A's first skipped %d, B's %d: want it to start at A's, below B's",
			st.rescanFrom.Seq, a.firstSkipped.Seq, b.firstSkipped.Seq)
	}

	// That one rescan fills A's queue and skips A's next record again,
	// so A drops out first; it must still go on to all of B's records.
	d.step(ctx, st)
	if a.blockedRead != st.readEpoch {
		t.Fatalf("A was not blocked by the rescan (blockedRead %d, read %d): the test no longer covers a destination blocked first", a.blockedRead, st.readEpoch)
	}
	if b.skipped != 0 {
		t.Fatalf("B still has %d records in the WAL after the rescan it asked for: the rescan stopped once A was blocked", b.skipped)
	}
	if waited := clock.Now().Sub(recovered); waited >= produceDispatchRescanInterval {
		t.Fatalf("B's records were placed %v after it recovered, want within one rescan interval (%v)", waited, produceDispatchRescanInterval)
	}

	// Everything drains: each partition's records exactly once, in order.
	close(sink.open)
	zzPerfAStepUntilDone(t, d, clock)
	got, want := sink.committed(), zzPerfAWant(parts)
	for part := range 2 {
		if !slices.Equal(got[part], want[part]) {
			t.Fatalf("partition %d committed %d records, want its %d exactly once in order (first mismatch near %v)",
				part, len(got[part]), len(want[part]), zzPerfAFirstDiff(got[part], want[part]))
		}
	}
}

// zzPerfAFirstDiff describes where got first differs from want.
func zzPerfAFirstDiff(got, want []int) string {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			return fmt.Sprintf("index %d: got %d, want %d", i, got[i], want[i])
		}
	}
	return fmt.Sprintf("lengths %d and %d", len(got), len(want))
}

// DispatchAvailable rescans from the lowest skipped record of every
// destination on each pass. The same stuck backlog, B failing its first
// commit, drains through repeated passes: every record exactly once, in
// order, and nothing left held or skipped.
func TestPerfARescanManualPass(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 2)
	m := zzWP6Manager(t)
	sink := newZZPerfAGateSink()
	close(sink.open)
	sink.failNext.Store(true)
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	if err := d.loadCursor(); err != nil {
		t.Fatal(err)
	}
	parts := zzPerfAStuckBacklog(t, m, d.perDestCap(d.state))
	var err error
	for pass := 0; pass < 50 && (pass == 0 || !zzPerfADone(d)); pass++ {
		_, err = d.DispatchAvailable(ctx)
	}
	if err != nil {
		t.Fatalf("last DispatchAvailable() error = %v", err)
	}
	st := d.state
	if !zzPerfADone(d) {
		t.Fatalf("after 50 passes: outstanding=%d held=%d skipped=%d checkpoint=%d durable=%d",
			st.outstanding, st.held, st.skipped, st.nextSeq, m.DurableProduceNext())
	}
	for _, dest := range st.dests {
		if dest.held != 0 || dest.skipped != 0 {
			t.Fatalf("destination %v left with held=%d skipped=%d", dest.key, dest.held, dest.skipped)
		}
	}
	got, want := sink.committed(), zzPerfAWant(parts)
	for part := range 2 {
		if !slices.Equal(got[part], want[part]) {
			t.Fatalf("partition %d committed %d records, want its %d exactly once in order (first mismatch near %v)",
				part, len(got[part]), len(want[part]), zzPerfAFirstDiff(got[part], want[part]))
		}
	}
}

// zzPerfAPropSink commits "orders" records both as the local committer
// and, through peer, as the remote owner. Each commit takes a random 0
// to 3 ms. The first batch holding record slow hangs until slowRelease
// is closed, and the first holding a record in fail fails (after
// committing, as a commit whose reply was lost, when the entry is true). It counts the commits of each record and keeps, per partition,
// the record numbers in commit order.
type zzPerfAPropSink struct {
	mu          sync.Mutex
	rng         *rand.Rand
	fail        map[int]bool
	slow        int
	slowPart    int
	slowStarted chan struct{}
	slowRelease chan struct{}
	count       []int
	got         map[int][]int
}

func (s *zzPerfAPropSink) commit(ctx context.Context, part int, nums []int) error {
	s.mu.Lock()
	delay := time.Duration(s.rng.IntN(3001)) * time.Microsecond
	hang := s.slow >= 0 && slices.Contains(nums, s.slow)
	if hang {
		s.slow, s.slowPart = -1, part
	}
	failBefore, failAfter := false, false
	for _, i := range nums {
		if after, ok := s.fail[i]; ok {
			delete(s.fail, i)
			failAfter = failAfter || after
			failBefore = failBefore || !after
		}
	}
	s.mu.Unlock()
	if hang {
		s.slowStarted <- struct{}{}
		select {
		case <-s.slowRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if failBefore {
		return errZZPerfAInjected
	}
	s.mu.Lock()
	for _, i := range nums {
		s.count[i]++
		s.got[part] = append(s.got[part], i)
	}
	s.mu.Unlock()
	if failAfter {
		return errZZPerfAInjected
	}
	return nil
}

func (s *zzPerfAPropSink) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := s.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (s *zzPerfAPropSink) CommitAcceptedProduceBatch(ctx context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	nums := make([]int, len(recs))
	for i, r := range recs {
		nums[i] = zzPerfANumber(r.Payload)
	}
	if err := s.commit(ctx, recs[0].TargetPartition, nums); err != nil {
		return nil, err
	}
	return make([]int64, len(recs)), nil
}

func (s *zzPerfAPropSink) peer() fakePeerClient {
	return fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		nums := make([]int, len(req.Records))
		for i, r := range req.Records {
			nums[i] = zzPerfANumber(r.Payload)
		}
		if err := s.commit(ctx, req.Records[0].TargetPartition, nums); err != nil {
			return nodewire.Response{}, err
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
}

// zzPerfAPropPartitions is how many partitions "orders" has in the
// property test; the even ones are owned here, the odd ones by the fake
// remote owner.
const zzPerfAPropPartitions = 40

// The dispatcher's delivery contract under a seeded random mix: 1 to 40
// destinations, some here and some on a fake remote owner, a skewed
// load that caps a hot destination, records arriving in three chunks,
// random commit delays, one commit hung past produceDispatchSlowAfter,
// and, on odd seeds, a few injected commit failures. Every record
// commits at least once, exactly once without failures, each
// partition's records first commit in WAL order, and without failures a
// record is read again at most 2.5 times on average. The seeds share
// one metadata store (a Raft election and 40 assignments cost seconds
// under -race); each has its own WAL and dispatcher.
func TestPerfADispatchProperty(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", zzPerfAPropPartitions)
	for p := 1; p < zzPerfAPropPartitions; p += 2 {
		if err := store.AssignPartition(context.Background(), "orders", p, "node-remote"); err != nil {
			t.Fatal(err)
		}
	}
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { zzPerfAProperty(t, store, seed) })
	}
}

func zzPerfAProperty(t *testing.T, store *metastore.Store, seed uint64) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	chosen := rng.Perm(zzPerfAPropPartitions)[:1+rng.IntN(zzPerfAPropPartitions)]
	local := 0
	for _, p := range chosen {
		if p%2 == 0 {
			local++
		}
	}
	n := 6000 + rng.IntN(10001)
	hot := chosen[rng.IntN(len(chosen))]
	parts := make([]int, n)
	for i := range parts {
		parts[i] = hot
		if rng.IntN(10) >= 7 {
			parts[i] = chosen[rng.IntN(len(chosen))]
		}
	}
	picks := 0
	if seed%2 == 1 {
		picks = 1 + rng.IntN(3)
	}
	sink := &zzPerfAPropSink{
		rng:         rand.New(rand.NewPCG(seed, 1)),
		fail:        map[int]bool{},
		slow:        rng.IntN(n),
		slowStarted: make(chan struct{}, 1),
		slowRelease: make(chan struct{}),
		count:       make([]int, n),
		got:         map[int][]int{},
	}
	for range picks {
		sink.fail[rng.IntN(n)] = rng.IntN(2) == 1
	}
	failures := len(sink.fail)
	t.Logf("destinations=%d (local %d) records=%d hot=%d failures=%d slow=%d", len(chosen), local, n, hot, failures, sink.slow)

	m := zzWP6Manager(t)
	d := NewProduceDispatcher(m, store, "node-self", sink, sink.peer(), nil, ProduceDispatcherConfig{})
	clock := &zzWP6Clock{now: time.Unix(1_700_000_000, 0)}
	d.now = clock.Now
	if err := d.loadCursor(); err != nil {
		t.Fatal(err)
	}
	st := d.state

	// The records arrive in three chunks: before the first round, and at
	// rounds 3 and 10.
	cuts := []int{n / 2, n * 3 / 4, n}
	acceptAt := map[int]int{0: 0, 3: 1, 10: 2}
	accepted := 0
	hung, released := false, false
	slowRounds := 0
	start := time.Now()
	for round := 0; ; round++ {
		if time.Since(start) > 8*time.Second {
			t.Fatalf("not drained after 8s (round %d): accepted=%d/%d outstanding=%d held=%d skipped=%d checkpoint=%d durable=%d",
				round, accepted, n, st.outstanding, st.held, st.skipped, st.nextSeq, m.DurableProduceNext())
		}
		if chunk, ok := acceptAt[round]; ok {
			accepted = zzPerfAAccept(t, m, accepted, parts[accepted:cuts[chunk]])
		}
		st.pass++
		st.err, st.stalled = nil, false
		d.step(ctx, st)
		if hung && !released {
			// Let the hung commit sit slow for a few rounds, or until
			// nothing else is in flight, then let it land.
			if dest := st.dests[dispatchDestKey{topic: "orders", partition: sink.slowPart}]; dest != nil && dest.slow {
				slowRounds++
				if slowRounds >= 3 || st.active == 0 {
					close(sink.slowRelease)
					released = true
				}
			}
		}
		if accepted == n && zzPerfADone(d) {
			break
		}
		if st.outstanding > 0 {
			select {
			case res := <-d.results:
				d.finish(ctx, st, res)
			case <-sink.slowStarted:
				hung = true
				clock.Advance(produceDispatchSlowAfter)
			case <-time.After(10 * time.Second):
				t.Fatalf("no commit finished in 10s (round %d, outstanding %d)", round, st.outstanding)
			}
			continue
		}
		clock.Advance(max(d.nextWake(st), time.Millisecond))
	}
	if !hung || !released {
		t.Fatalf("the slow commit never hung (hung=%v released=%v)", hung, released)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.fail) != 0 {
		t.Fatalf("%d of %d injected failures never fired", len(sink.fail), failures)
	}
	for i, c := range sink.count {
		if c == 0 {
			t.Fatalf("record %d (partition %d) never committed", i, parts[i])
		}
		if failures == 0 && c != 1 {
			t.Fatalf("record %d (partition %d) committed %d times with no failure injected, want once", i, parts[i], c)
		}
	}
	rerouted := map[int]bool{}
	for part, nums := range sink.got {
		for _, i := range nums {
			if parts[i] != part {
				rerouted[parts[i]] = true
			}
		}
	}
	for part, nums := range sink.got {
		if rerouted[part] {
			continue
		}
		seen := map[int]bool{}
		last := -1
		for _, i := range nums {
			if seen[i] || parts[i] != part {
				continue
			}
			seen[i] = true
			if i < last {
				t.Fatalf("partition %d: record %d first committed after record %d", part, i, last)
			}
			last = i
		}
	}
	t.Logf("rereads=%d (%.2f per record), rerouted partitions=%d", st.rereads, float64(st.rereads)/float64(n), len(rerouted))
	if failures == 0 && st.rereads > uint64(n)*5/2 {
		t.Fatalf("%d skipped records read again for %d records with no failure injected, want at most 2.5 per record", st.rereads, n)
	}
}
