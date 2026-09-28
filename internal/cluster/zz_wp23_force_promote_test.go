package cluster

// M4: a force-promote while the source keeps acking past the promoted
// high-watermark. The source is cut off from the destination but still
// serving: after the destination's last listing it commits more records
// and its consumers ack past the listed boundary. The destination
// promotes that listing. The source then dies, returns and is reclaimed
// (quarantined, as its copy is ahead of the promoted boundary). None of
// the source's acks may reach the destination's copy, where the offsets
// past the boundary are other records; and nothing the source's offset
// committer does after the reclaim may recreate or write the source's
// partition directory.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP23Node is one broker: a real engine over its own metastore replica
// and data directory, its consumer offsets persisted through a real
// committer the way cmd/narad wires them.
type zzWP23Node struct {
	engine    *messaging.Engine
	store     *metastore.Store
	committer *runtime.ConsumerOffsetCommitter
	dataDir   string
}

// newZZWP23Node starts a node selfID whose replica has orders/0 owned by
// owner and moving to target ("" for none).
func newZZWP23Node(t *testing.T, selfID, owner, target string) *zzWP23Node {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{
		Name: "orders", Partitions: 1, RetentionMs: 7_200_000,
		VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, id := range []string{"narad-src", "narad-dst"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ":7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember %s: %v", id, err)
		}
	}
	if err := store.AssignPartition(ctx, "orders", 0, owner); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if target != "" {
		if err := store.SetAssignmentTarget(ctx, "orders", 0, target); err != nil {
			t.Fatalf("SetAssignmentTarget: %v", err)
		}
	}
	dataDir := t.TempDir()
	committer := runtime.NewConsumerOffsetCommitter(dataDir, time.Second, nil)
	t.Cleanup(func() { _ = committer.Close() })
	offsets := zzWP23WireOffsets(dataDir, committer)
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	engine := messaging.NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0},
		offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), selfID)
	return &zzWP23Node{engine: engine, store: store, committer: committer, dataDir: dataDir}
}

func (n *zzWP23Node) dir() string { return storage.TopicPartitionDir(n.dataDir, "orders", 0) }

// flip records, in this node's replica, the move of orders/0 to target.
func (n *zzWP23Node) flip(t *testing.T, owner, target string) {
	t.Helper()
	ctx := context.Background()
	if err := n.store.SetAssignmentTarget(ctx, "orders", 0, target); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	if err := n.store.CompleteMove(ctx, "orders", 0, owner, target); err != nil {
		t.Fatalf("CompleteMove: %v", err)
	}
}

func (n *zzWP23Node) produce(t *testing.T, label string) int64 {
	t.Helper()
	off, _, err := n.engine.Produce(context.Background(), "orders", "", fmt.Appendf(nil, `{"label":%q}`, label))
	if err != nil {
		t.Fatalf("produce %s: %v", label, err)
	}
	return off
}

// take reserves the next record; false when there is none.
func (n *zzWP23Node) take(t *testing.T) (int64, consumer.Handle, bool) {
	t.Helper()
	msg, found, err := n.engine.Consume(context.Background(), "orders", messaging.ConsumeOpts{})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if !found {
		return 0, consumer.Handle{}, false
	}
	h, err := consumer.DecodeHandle(msg.ReceiptHandle)
	if err != nil {
		t.Fatalf("decode handle: %v", err)
	}
	return msg.Offset, h, true
}

// takeAck reserves and acks the next record.
func (n *zzWP23Node) takeAck(t *testing.T) (int64, bool) {
	t.Helper()
	off, h, ok := n.take(t)
	if ok {
		if err := n.engine.Ack(context.Background(), "orders", h); err != nil {
			t.Fatalf("ack %d: %v", off, err)
		}
	}
	return off, ok
}

// zzWP23ConsumerFiles reads a partition directory's consumer state files.
func zzWP23ConsumerFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, name := range []string{storage.ConsumerOffsetFileName, storage.ConsumerAheadFileName} {
		b, err := os.ReadFile(dir + "/" + name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		out[name] = b
	}
	return out
}

// zzWP23CutOffSource is the destination's view of a real source engine:
// listings and chunks come from it until the move asks it to freeze.
// Then the source is cut off: onCutOff runs (the source keeps serving
// its own consumers) and every later call fails.
type zzWP23CutOffSource struct {
	src      *messaging.Engine
	onCutOff func()

	mu     sync.Mutex
	cut    bool
	listed []messaging.PartitionTransferInfo
}

var errZZWP23SourceCutOff = errors.New("dial source: connection refused")

func (p *zzWP23CutOffSource) isCut() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cut
}

func (p *zzWP23CutOffSource) ListPartitionSegments(ctx context.Context, _, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	if p.isCut() {
		return messaging.PartitionTransferInfo{}, errZZWP23SourceCutOff
	}
	info, err := p.src.PartitionTransferInfo(ctx, topicName, partition)
	if err == nil {
		p.mu.Lock()
		p.listed = append(p.listed, info)
		p.mu.Unlock()
	}
	return info, err
}

func (p *zzWP23CutOffSource) FetchSegmentChunk(ctx context.Context, _, topicName string, partition int, base, at, length int64) ([]byte, error) {
	if p.isCut() {
		return nil, errZZWP23SourceCutOff
	}
	return p.src.ReadPartitionSegment(ctx, topicName, partition, base, at, min(length, storage.MaxSegmentReadBytes))
}

func (p *zzWP23CutOffSource) PrepareHandoff(context.Context, string, string, int, time.Duration, string) (messaging.PartitionTransferInfo, error) {
	p.mu.Lock()
	first := !p.cut
	p.cut = true
	p.mu.Unlock()
	if first && p.onCutOff != nil {
		p.onCutOff()
	}
	return messaging.PartitionTransferInfo{}, errZZWP23SourceCutOff
}

func (p *zzWP23CutOffSource) CompleteMove(context.Context, string, string, int, string, string) error {
	return nil
}

func (p *zzWP23CutOffSource) AbortMove(context.Context, string, string, int, string) error {
	return nil
}

func (p *zzWP23CutOffSource) GetAssignment(context.Context, string, string, int) (metastore.Assignment, error) {
	return metastore.Assignment{}, errZZWP23SourceCutOff
}

func (p *zzWP23CutOffSource) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	return nodewire.Response{}, errZZWP23SourceCutOff
}

func TestZZWP23ForcePromoteWhileSourceAcksPastHWM(t *testing.T) {
	ctx := context.Background()
	src := newZZWP23Node(t, "narad-src", "narad-src", "")
	dst := newZZWP23Node(t, "narad-dst", "narad-src", "narad-dst")
	for i := range 10 {
		src.produce(t, fmt.Sprintf("src-%d", i))
	}
	for i := range 5 {
		if off, ok := src.takeAck(t); !ok || off != int64(i) {
			t.Fatalf("source: took %d (%v), want %d", off, ok, i)
		}
	}

	// Cut off from the destination, the source keeps serving: it commits
	// five more records, acks up to 13, past the boundary of 10 the
	// destination listed, and hands out 14, acked only once it returns.
	var held consumer.Handle
	var srcAcked int64
	peer := &zzWP23CutOffSource{src: src.engine, onCutOff: func() {
		for i := 10; i < 15; i++ {
			src.produce(t, fmt.Sprintf("src-%d", i))
		}
		for i := 5; i <= 13; i++ {
			off, ok := src.takeAck(t)
			if !ok || off != int64(i) {
				t.Errorf("source: took %d (%v), want %d", off, ok, i)
				return
			}
			srcAcked = off
		}
		off, h, ok := src.take(t)
		if !ok || off != 14 {
			t.Errorf("source: took %d (%v), want 14", off, ok)
			return
		}
		held = h
	}}
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "narad-src:7942", Status: metastore.MemberAlive},
		deadAfter:  2, // reachable for the copy, dead (long ago) from the next lookup on
	}
	r := NewMoveRunner(store, "narad-dst", dst.dataDir, peer, dst.engine, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		MoveConfig{RetryBackoff: 5 * time.Millisecond, ForcePromoteAfter: time.Millisecond})
	moveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r.runMove(moveCtx, "orders", 0, "narad-src")
	if t.Failed() {
		t.FailNow()
	}
	if got := store.completeArgs; len(got) != 3 || got[2] != "narad-dst" {
		t.Fatalf("the move did not complete: %v", got)
	}
	marker, ok, err := messaging.ReadMoveMarker(dst.dir())
	if err != nil || !ok || !marker.ForcePromoted || marker.HighWatermark != 10 {
		t.Fatalf("installed copy: marker %+v ok %v err %v, want force-promoted at 10", marker, ok, err)
	}
	if srcAcked != 13 {
		t.Fatalf("precondition: the source acked up to %d, want 13 (past the promoted 10)", srcAcked)
	}
	installed := zzWP23ConsumerFiles(t, dst.dir())
	if got := zzWP23RecoveredFrontier(t, dst.dir()); got != 4 {
		t.Fatalf("installed copy recovers %d, want the listed 4", got)
	}
	dst.flip(t, "narad-src", "narad-dst")

	// The source returns. Its replica learns of the flip; a consumer acks
	// the record it held; the sweep reclaims the copy, which is ahead of
	// the promoted boundary, so it is quarantined.
	src.flip(t, "narad-src", "narad-dst")
	if err := src.engine.Ack(ctx, "orders", held); err != nil {
		t.Fatalf("source: ack of the held record 14: %v", err)
	}
	err = src.engine.ReclaimMovedPartitionGuarded(ctx, "orders", 0, messaging.ReclaimGuard{PromotedHWM: 10, Known: true})
	if !errors.Is(err, messaging.ErrPartitionQuarantined) {
		t.Fatalf("source reclaim: %v, want %v", err, messaging.ErrPartitionQuarantined)
	}
	// Late source writes: an ack's commit issued after the reclaim
	// dropped its shard, and the held record acked again.
	src.committer.Commit("orders", 0, 14)
	if err := src.engine.Ack(ctx, "orders", held); err == nil {
		t.Fatal("source: a second ack of 14 after the reclaim succeeded")
	}
	if err := src.committer.Close(); err != nil {
		t.Fatalf("source committer close: %v", err)
	}

	if _, err := os.Stat(src.dir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the source's partition directory exists after its reclaim (stat err %v)", err)
	}
	if got := zzWP23RecoveredFrontier(t, src.dir()+messaging.QuarantineSuffix); got < 4 || got > 14 {
		t.Fatalf("the quarantined source copy recovers %d, want its own acks (4 to 14)", got)
	}
	if got := zzWP23ConsumerFiles(t, dst.dir()); !maps.EqualFunc(got, installed, slices.Equal) {
		t.Fatalf("the destination's consumer state changed without a destination ack: %v, installed %v", got, installed)
	}

	// The new owner serves from its own frontier: the copied records 5
	// to 9 again (the source's later acks of them are not its), and every
	// record it commits itself at 10 and up, which are not the records
	// the source acked at those offsets.
	first := dst.produce(t, "dst-0")
	last := dst.produce(t, "dst-2")
	if first != 10 {
		t.Fatalf("the new owner's first record is at %d, want 10", first)
	}
	delivered := map[int64]bool{}
	for {
		off, ok := dst.takeAck(t)
		if !ok {
			break
		}
		delivered[off] = true
	}
	for off := int64(5); off <= last; off++ {
		if !delivered[off] {
			t.Fatalf("LOSS: the new owner never delivered %d; delivered %v", off, delivered)
		}
	}
	if err := dst.committer.Close(); err != nil {
		t.Fatalf("destination committer close: %v", err)
	}
	if got := zzWP23RecoveredFrontier(t, dst.dir()); got != last {
		t.Fatalf("the new owner's acks recover %d, want %d", got, last)
	}
}
