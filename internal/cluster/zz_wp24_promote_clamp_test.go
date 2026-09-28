package cluster

// A force-promote reuses the positions of the last CatchUp listing. A
// source that reads its boundary before its consumer frontier (every
// release before this check) can list a frontier at or above that
// boundary, and the new owner then started past its own log end: the
// records it wrote below that frontier were committed, readable and
// never delivered. These tests pin the destination's clamp, which also
// covers such older sources.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// racedListingFetcher serves a partition directory with the positions
// an older source lists when acks land between its boundary read and
// its frontier read: the frontier, the acked-ahead set and the fan-out
// cursors are whatever the test sets, whatever the boundary.
type racedListingFetcher struct {
	dirFetcher
	ackedAhead []int64
	sidecars   []storage.SidecarFile
}

func (f racedListingFetcher) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	info.AckedAhead = f.ackedAhead
	info.Sidecars = f.sidecars
	return info, err
}

// cursorSidecar is the verbatim cursor file a source would list.
func cursorSidecar(t *testing.T, child string, next int64) storage.SidecarFile {
	t.Helper()
	dir := t.TempDir()
	if err := storage.WriteFanoutCursorIfPartitionDirExists(dir, child, storage.FanoutCursor{Epoch: "e1", NextOffset: next}); err != nil {
		t.Fatalf("write cursor: %v", err)
	}
	files, err := storage.ListFanoutCursorFiles(dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("list cursor files: %v %v", files, err)
	}
	return files[0]
}

func TestZZWP24ForcePromoteClampsPositionsToBoundary(t *testing.T) {
	cases := []struct {
		name          string
		records       int
		hwm           int64 // the listing's boundary
		committed     int64
		ackedAhead    []int64
		cursorNext    int64
		wantCommitted int64
		wantAhead     []int64
		wantCursor    int64
	}{
		{
			// The raced shape: a record at the boundary was committed,
			// delivered and acked between the source's reads.
			name: "frontier past the boundary", records: 10, hwm: 10,
			committed: 12, ackedAhead: []int64{14, 15}, cursorNext: 13,
			wantCommitted: 9, wantAhead: nil, wantCursor: 10,
		},
		{
			name: "acked-ahead past the boundary", records: 10, hwm: 10,
			committed: 5, ackedAhead: []int64{7, 10, 12}, cursorNext: 10,
			wantCommitted: 5, wantAhead: []int64{7}, wantCursor: 10,
		},
		{
			// Records [7, 10) are copied but hidden. A failed first
			// commit on the new owner truncates them and reuses their
			// offsets, so the frontier must stay below the boundary, not
			// below the copied tail.
			name: "frontier inside the hidden tail", records: 10, hwm: 7,
			committed: 8, ackedAhead: []int64{9}, cursorNext: 9,
			wantCommitted: 6, wantAhead: nil, wantCursor: 7,
		},
		{
			name: "consistent listing is untouched", records: 10, hwm: 10,
			committed: 4, ackedAhead: []int64{6, 8}, cursorNext: 10,
			wantCommitted: 4, wantAhead: []int64{6, 8}, wantCursor: 10,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := t.TempDir()
			buildSourcePartition(t, src, tc.records)
			fetcher := racedListingFetcher{
				dirFetcher: dirFetcher{dir: src, hwm: tc.hwm, committed: tc.committed, hasCommitted: true},
				ackedAhead: tc.ackedAhead,
				sidecars:   []storage.SidecarFile{cursorSidecar(t, "audit", tc.cursorNext)},
			}
			listedAhead := slices.Clone(tc.ackedAhead)
			sess := NewPartitionMover(fetcher, 64, slog.New(slog.NewTextHandler(io.Discard, nil))).
				Begin("src", "orders", 0, filepath.Join(t.TempDir(), "staging"))
			if _, err := sess.CatchUp(context.Background(), 1<<20, 3, 2); err != nil {
				t.Fatalf("CatchUp: %v", err)
			}
			res, err := sess.ForcePromote()
			if err != nil {
				t.Fatalf("ForcePromote: %v", err)
			}
			if res.CommittedOffset != tc.wantCommitted {
				t.Fatalf("result committed = %d, want %d", res.CommittedOffset, tc.wantCommitted)
			}
			got, ok, err := storage.ReadConsumerOffset(sess.stagingDir)
			if err != nil || !ok || got != tc.wantCommitted {
				t.Fatalf("staged consumer.offset = %d (ok %v, err %v), want %d: boundary %d", got, ok, err, tc.wantCommitted, tc.hwm)
			}
			rec, ok, err := storage.ReadConsumerAhead(sess.stagingDir)
			if err != nil || !ok {
				t.Fatalf("staged consumer.ahead: ok %v err %v", ok, err)
			}
			if rec.Committed != tc.wantCommitted || !slices.Equal(rec.Offsets, tc.wantAhead) {
				t.Fatalf("staged consumer.ahead = committed %d offsets %v, want %d %v", rec.Committed, rec.Offsets, tc.wantCommitted, tc.wantAhead)
			}
			cur, ok, err := storage.ReadFanoutCursor(sess.stagingDir, "audit")
			if err != nil || !ok || cur.NextOffset != tc.wantCursor || cur.Epoch != "e1" {
				t.Fatalf("staged cursor = %+v (ok %v, err %v), want next %d epoch e1", cur, ok, err, tc.wantCursor)
			}
			if !slices.Equal(sess.lastAhead, listedAhead) {
				t.Fatalf("the clamp modified the listing's acked-ahead set: %v, was %v", sess.lastAhead, listedAhead)
			}
		})
	}
}

const wp24Topic = "orders"

// newWP24Engine builds a single-node engine owning wp24Topic/0 as
// selfID, with consumer-offset recovery wired as cmd/narad wires it.
func newWP24Engine(t *testing.T, selfID string) (*messaging.Engine, *runtime.Logs, string) {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{
		Name: wp24Topic, Partitions: 1, RetentionMs: 7_200_000,
		VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := store.AssignPartition(ctx, wp24Topic, 0, selfID); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	offsets.SetCommittedRecovery(func(topicName string, p int) (int64, bool) {
		committed, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, topicName, p))
		return committed, ok && err == nil
	})
	offsets.SetAheadRecovery(func(topicName string, p int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, topicName, p))
		return rec.Committed, rec.Offsets, ok && err == nil
	})
	engine := messaging.NewEngine(store, schema.NewAlwaysValid(), partition.NewHashRoundRobin(),
		offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), selfID)
	return engine, logs, dataDir
}

func wp24Produce(t *testing.T, e *messaging.Engine, label string) int64 {
	t.Helper()
	offs, err := e.CommitAcceptedProduceBatch(context.Background(), []ingress.ProduceRecord{{
		Topic: wp24Topic, TargetPartition: 0, Key: "k", Payload: fmt.Appendf(nil, `{"label":%q}`, label),
	}})
	if err != nil || len(offs) != 1 {
		t.Fatalf("produce %s: offs %v err %v", label, offs, err)
	}
	return offs[0]
}

// wp24ConsumeAck takes one message from partition 0 and acks it.
func wp24ConsumeAck(t *testing.T, e *messaging.Engine, wait time.Duration) (int64, bool) {
	t.Helper()
	p := 0
	ctx := context.Background()
	msg, found, err := e.Consume(ctx, wp24Topic, messaging.ConsumeOpts{Partition: &p, Wait: wait})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if !found {
		return 0, false
	}
	h, err := consumer.DecodeHandle(msg.ReceiptHandle)
	if err != nil {
		t.Fatalf("decode handle: %v", err)
	}
	if err := e.Ack(ctx, wp24Topic, h); err != nil {
		t.Fatalf("ack %d: %v", msg.Offset, err)
	}
	return msg.Offset, true
}

// olderSourcePeer is the destination's view of a source running a
// release that reads its boundary and segments before its frontier.
// Its one listing races: a record is committed, delivered and acked on
// the real source engine between the two halves, so the boundary and
// segments predate the record while the frontier covers it. The source
// then dies, so every later call fails.
type olderSourcePeer struct {
	t      *testing.T
	src    *messaging.Engine
	lists  atomic.Int32
	dead   atomic.Bool
	listed messaging.PartitionTransferInfo
}

var errWP24SourceGone = errors.New("dial source: connection refused")

func (p *olderSourcePeer) ListPartitionSegments(ctx context.Context, _, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	if p.lists.Add(1) > 1 {
		p.dead.Store(true)
		return messaging.PartitionTransferInfo{}, errWP24SourceGone
	}
	before, err := p.src.PartitionTransferInfo(ctx, topicName, partition)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	off := wp24Produce(p.t, p.src, "during-listing")
	if got, ok := wp24ConsumeAck(p.t, p.src, time.Second); !ok || got != off {
		p.t.Fatalf("source: record %d produced during the listing not delivered (got %d, %v)", off, got, ok)
	}
	after, err := p.src.PartitionTransferInfo(ctx, topicName, partition)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	raced := before
	raced.CommittedOffset, raced.HasCommitted, raced.AckedAhead = after.CommittedOffset, after.HasCommitted, after.AckedAhead
	p.listed = raced
	return raced, nil
}

func (p *olderSourcePeer) FetchSegmentChunk(ctx context.Context, _, topicName string, partition int, base, at, length int64) ([]byte, error) {
	if p.dead.Load() {
		return nil, errWP24SourceGone
	}
	return p.src.ReadPartitionSegment(ctx, topicName, partition, base, at, min(length, storage.MaxSegmentReadBytes))
}

func (p *olderSourcePeer) PrepareHandoff(context.Context, string, string, int, time.Duration, string) (messaging.PartitionTransferInfo, error) {
	return messaging.PartitionTransferInfo{}, errWP24SourceGone
}

func (p *olderSourcePeer) CompleteMove(context.Context, string, string, int, string, string) error {
	return nil
}

func (p *olderSourcePeer) AbortMove(context.Context, string, string, int, string) error { return nil }

func (p *olderSourcePeer) GetAssignment(context.Context, string, string, int) (metastore.Assignment, error) {
	return metastore.Assignment{}, errWP24SourceGone
}

func (p *olderSourcePeer) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	return nodewire.Response{}, errWP24SourceGone
}

// TestZZWP24ForcePromotedOwnerDeliversEveryNewRecord runs a real move:
// a real source engine serves the listing and the chunks, a real
// MoveRunner force-promotes after the source dies, and a real
// destination engine then produces and consumes on the installed copy.
// Every record the new owner commits must be delivered. Before the
// clamp the installed frontier equalled the copy's end, and the first
// record produced there was committed and readable but never delivered.
func TestZZWP24ForcePromotedOwnerDeliversEveryNewRecord(t *testing.T) {
	src, _, _ := newWP24Engine(t, "node-src")
	dst, dstLogs, dstDataDir := newWP24Engine(t, "node-dst")
	for i := range 5 {
		wp24Produce(t, src, fmt.Sprintf("pre-%d", i))
	}
	for range 5 {
		if _, ok := wp24ConsumeAck(t, src, 0); !ok {
			t.Fatal("source: pre-move record not deliverable")
		}
	}

	peer := &olderSourcePeer{t: t, src: src}
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: wp24Topic, Partition: 0, OwnerID: "node-src", TargetID: "node-dst"},
		member:     metastore.Member{ID: "node-src", Addr: "src-addr", Status: metastore.MemberAlive},
		deadAfter:  2, // alive for the first attempt, dead (long ago) from the second lookup on
	}
	r := NewMoveRunner(store, "node-dst", dstDataDir, peer, dst, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		MoveConfig{RetryBackoff: 5 * time.Millisecond, ForcePromoteAfter: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.runMove(ctx, wp24Topic, 0, "node-src")
	if got := store.completeArgs; len(got) != 3 || got[2] != "node-dst" {
		t.Fatalf("move did not complete: %v", got)
	}
	if l := peer.listed; l.CommittedOffset < l.HighWatermark {
		t.Fatalf("precondition: the listing did not race (committed %d, boundary %d)", l.CommittedOffset, l.HighWatermark)
	}

	dstDir := storage.TopicPartitionDir(dstDataDir, wp24Topic, 0)
	if marker, ok, err := messaging.ReadMoveMarker(dstDir); err != nil || !ok || !marker.ForcePromoted {
		t.Fatalf("installed copy not force-promoted: %+v ok %v err %v", marker, ok, err)
	}
	log, err := dstLogs.Get(wp24Topic, 0)
	if err != nil {
		t.Fatalf("destination log: %v", err)
	}
	installedEnd := log.NextOffset()
	front, _, _ := storage.ReadConsumerOffset(dstDir)

	first := wp24Produce(t, dst, "after-move-1")
	last := wp24Produce(t, dst, "after-move-2")
	delivered := map[int64]bool{}
	for {
		off, ok := wp24ConsumeAck(t, dst, 300*time.Millisecond)
		if !ok {
			break
		}
		delivered[off] = true
	}
	for off := first; off <= last; off++ {
		if !delivered[off] {
			t.Fatalf("LOSS: record %d committed on the new owner after the force-promote was never delivered; delivered %v, installed consumer.offset %d, installed log end %d, listing boundary %d committed %d",
				off, delivered, front, installedEnd, peer.listed.HighWatermark, peer.listed.CommittedOffset)
		}
	}
	if front >= installedEnd {
		t.Fatalf("installed consumer.offset %d is at or past the installed log end %d", front, installedEnd)
	}
}
