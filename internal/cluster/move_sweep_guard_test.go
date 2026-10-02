package cluster

// The old owner's stale-copy sweep must never delete the only copy of a
// moved partition's records. The new owner can hold less than the move
// gave it: an install rolled back after a flip that committed anyway, a
// volume lost under the same node ID, a sealed segment lost or cut short
// before writeback. The sweep compares the owner's listing with the local
// copy and sets the copy aside when the owner cannot vouch for it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// ownerReclaimGuard's decisions, on synthetic listings.
func TestStaleCopyOwnerGuardDecisions(t *testing.T) {
	now := time.Now()
	fresh := now.Add(-time.Minute)
	old := now.Add(-48 * time.Hour)
	seg := func(base, size int64, sealed bool) storage.SegmentInfo {
		return storage.SegmentInfo{BaseOffset: base, SizeBytes: size, Sealed: sealed}
	}
	// This node is narad-src. The local copy: three sealed segments of 5
	// records and a tail of 2.
	local := []localSegment{
		{base: 0, size: 500, modTime: fresh},
		{base: 5, size: 500, modTime: fresh},
		{base: 10, size: 500, modTime: fresh},
		{base: 15, size: 200, modTime: fresh},
	}
	marker := func(hwm int64) *messaging.MoveMarker {
		return &messaging.MoveMarker{Source: "narad-src", HighWatermark: hwm}
	}
	healthy := []storage.SegmentInfo{seg(0, 500, true), seg(5, 500, true), seg(10, 500, true), seg(15, 900, false)}

	for _, tc := range []struct {
		name      string
		info      messaging.PartitionTransferInfo
		local     []localSegment
		retention time.Duration
		setAside  bool
		wantHWM   int64
	}{
		{
			name:    "healthy owner past the promoted position: guard at the promoted hwm",
			info:    messaging.PartitionTransferInfo{Segments: healthy, HighWatermark: 25, MoveMarker: marker(17)},
			local:   local,
			wantHWM: 17,
		},
		{
			name:    "owner hwm below the marker's caps the guard",
			info:    messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(0, 500, true), seg(5, 500, true), seg(10, 300, false)}, HighWatermark: 13, MoveMarker: marker(17)},
			local:   local,
			wantHWM: 13,
		},
		{
			name:     "owner lists nothing (lost its volume): set aside",
			info:     messaging.PartitionTransferInfo{},
			local:    local,
			setAside: true,
		},
		{
			name:     "owner lists only an empty active segment (rolled back its install): set aside",
			info:     messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(0, 0, false)}},
			local:    local,
			setAside: true,
		},
		{
			name:     "owner holds records but no move marker: set aside",
			info:     messaging.PartitionTransferInfo{Segments: healthy, HighWatermark: 40},
			local:    local,
			setAside: true,
		},
		{
			name: "owner's marker records a move from another node: set aside",
			info: messaging.PartitionTransferInfo{
				Segments: healthy, HighWatermark: 25,
				MoveMarker: &messaging.MoveMarker{Source: "narad-other", HighWatermark: 25},
			},
			local:    local,
			setAside: true,
		},
		{
			name:     "owner lost a sealed segment below the vouched position: set aside",
			info:     messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(0, 500, true), seg(10, 500, true), seg(15, 900, false)}, HighWatermark: 25, MoveMarker: marker(17)},
			local:    local,
			setAside: true,
		},
		{
			name:     "owner's sealed segment is shorter than the local one: set aside",
			info:     messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(0, 500, true), seg(5, 120, true), seg(10, 500, true), seg(15, 900, false)}, HighWatermark: 25, MoveMarker: marker(17)},
			local:    local,
			setAside: true,
		},
		{
			name: "force-promoted below the local copy: the tail and the segments past it are the guard's",
			info: messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(0, 500, true), seg(5, 260, false)}, HighWatermark: 8, MoveMarker: marker(8)},
			// The local copy kept committing past 8, into the same
			// segment and new ones; the reclaim quarantines it (17 > 8).
			local:   local,
			wantHWM: 8,
		},
		{
			name: "owner's retention reaped segments the local copy holds expired",
			info: messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(10, 500, true), seg(15, 900, false)}, HighWatermark: 25, MoveMarker: marker(17)},
			local: []localSegment{
				{base: 0, size: 500, modTime: old},
				{base: 5, size: 500, modTime: old},
				{base: 10, size: 500, modTime: fresh},
				{base: 15, size: 200, modTime: fresh},
			},
			retention: 24 * time.Hour,
			wantHWM:   17,
		},
		{
			name: "keep-forever topic: an owner missing an old segment is a gap",
			info: messaging.PartitionTransferInfo{Segments: []storage.SegmentInfo{seg(10, 500, true), seg(15, 900, false)}, HighWatermark: 25, MoveMarker: marker(17)},
			local: []localSegment{
				{base: 0, size: 500, modTime: old},
				{base: 5, size: 500, modTime: old},
				{base: 10, size: 500, modTime: fresh},
				{base: 15, size: 200, modTime: fresh},
			},
			setAside: true,
		},
		{
			name:      "local copy wholly expired: the plain guard at the owner's position",
			info:      messaging.PartitionTransferInfo{HighWatermark: 0},
			local:     []localSegment{{base: 0, size: 500, modTime: old}},
			retention: time.Hour,
			wantHWM:   0,
		},
		{
			name:    "empty local copy: the plain guard at the owner's hwm",
			info:    messaging.PartitionTransferInfo{HighWatermark: 7},
			local:   []localSegment{{base: 0, size: 0, modTime: fresh}},
			wantHWM: 7,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guard := ownerReclaimGuard(tc.info, tc.local, "narad-src", tc.retention, now)
			if !guard.Known {
				t.Fatalf("guard %+v is not KNOWN: the sweep must never reclaim unguarded", guard)
			}
			if got := guard.SetAside != ""; got != tc.setAside {
				t.Fatalf("set aside = %v (%q), want %v", got, guard.SetAside, tc.setAside)
			}
			if !tc.setAside && guard.PromotedHWM != tc.wantHWM {
				t.Fatalf("guard hwm = %d, want %d", guard.PromotedHWM, tc.wantHWM)
			}
		})
	}
}

// ---- end to end: a real move between real engines, then the sweep ----

// engineNode is one broker: a real engine over its own single-node
// metastore replica and data directory, with small segments so a
// partition spans several sealed ones.
type engineNode struct {
	engine  *messaging.Engine
	store   *metastore.Store
	dataDir string
}

// newEngineNode starts a node selfID whose replica has orders/0 owned by
// owner and moving to target ("" for none).
func newEngineNode(t *testing.T, selfID, owner, target string) *engineNode {
	t.Helper()
	return newEngineNodeWithSegments(t, selfID, owner, target, 64)
}

// newEngineNodeWithSegments is newEngineNode with a chosen segment size:
// 64 bytes puts about one record in each segment, which exercises the
// sweep's per-segment checks; the default (zero) keeps every record of
// a small partition in one tail segment, as in production.
func newEngineNodeWithSegments(t *testing.T, selfID, owner, target string, segmentBytes int64) *engineNode {
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
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond, SegmentBytes: segmentBytes}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	engine := messaging.NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0},
		offsets, logs, nil, nil, discardLogger(), selfID)
	return &engineNode{engine: engine, store: store, dataDir: dataDir}
}

func (n *engineNode) dir() string { return storage.TopicPartitionDir(n.dataDir, "orders", 0) }

// flip records, in this node's replica, the move of orders/0 to target.
func (n *engineNode) flip(t *testing.T, owner, target string) {
	t.Helper()
	ctx := context.Background()
	if err := n.store.SetAssignmentTarget(ctx, "orders", 0, target); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	if err := n.store.CompleteMove(ctx, "orders", 0, owner, target); err != nil {
		t.Fatalf("CompleteMove: %v", err)
	}
}

func (n *engineNode) produce(t *testing.T, label string) {
	t.Helper()
	if _, _, err := n.engine.Produce(context.Background(), "orders", "", fmt.Appendf(nil, `{"label":%q}`, label)); err != nil {
		t.Fatalf("produce %s: %v", label, err)
	}
}

// ownerEnginePeer answers the move and sweep RPCs from one real engine
// (whoever answers at the owner's address), JSON round-tripping each
// listing as PeerClient decodes it.
type ownerEnginePeer struct {
	owner *messaging.Engine
	seen  *[]messaging.PartitionTransferInfo
}

func onTheWire(info messaging.PartitionTransferInfo, err error) (messaging.PartitionTransferInfo, error) {
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	buf, err := json.Marshal(info)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	var out messaging.PartitionTransferInfo
	err = json.Unmarshal(buf, &out)
	return out, err
}

func (p ownerEnginePeer) ListPartitionSegments(ctx context.Context, _, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := onTheWire(p.owner.PartitionTransferInfo(ctx, topicName, partition))
	if err == nil && p.seen != nil {
		*p.seen = append(*p.seen, info)
	}
	return info, err
}

func (p ownerEnginePeer) FetchSegmentChunk(ctx context.Context, _, topicName string, partition int, base, at, length int64) ([]byte, error) {
	return p.owner.ReadPartitionSegment(ctx, topicName, partition, base, at, min(length, storage.MaxSegmentReadBytes))
}

func (p ownerEnginePeer) PrepareHandoff(ctx context.Context, _, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	if token != "" {
		return onTheWire(p.owner.ConfirmHandoff(ctx, topicName, partition, ttl, token))
	}
	return onTheWire(p.owner.PrepareHandoff(ctx, topicName, partition, ttl))
}

func (ownerEnginePeer) CompleteMove(context.Context, string, string, int, string, string) error {
	return nil
}
func (ownerEnginePeer) AbortMove(context.Context, string, string, int, string) error { return nil }
func (ownerEnginePeer) GetAssignment(context.Context, string, string, int) (metastore.Assignment, error) {
	return metastore.Assignment{}, errors.New("unused: the sweep runs as its own leader")
}

func (ownerEnginePeer) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	return nodewire.Response{}, errors.New("unused")
}

// moveThenFlip runs a real move of orders/0 (records records) from a
// fresh source into a fresh destination and flips both replicas.
func moveThenFlip(t *testing.T, records int) (src, dst *engineNode) {
	t.Helper()
	src = newEngineNode(t, "narad-src", "narad-src", "")
	dst = newEngineNode(t, "narad-dst", "narad-src", "narad-dst")
	for i := range records {
		src.produce(t, fmt.Sprintf("pre-move-%d", i))
	}
	moveInto(t, dst, src.engine, "narad-src", "narad-dst")
	dst.flip(t, "narad-src", "narad-dst")
	src.flip(t, "narad-src", "narad-dst")
	return src, dst
}

// moveInto runs a real move worker on node, copying orders/0 from the
// engine that answers for owner.
func moveInto(t *testing.T, node *engineNode, from *messaging.Engine, owner, target string) {
	t.Helper()
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: owner, TargetID: target},
		member:     metastore.Member{ID: owner, Addr: owner + ":7942", Status: metastore.MemberAlive},
	}
	r := NewMoveRunner(store, target, node.dataDir, ownerEnginePeer{owner: from}, node.engine, nil,
		discardLogger(), MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r.runMove(ctx, "orders", 0, owner)
	if got := store.completeArgs; len(got) != 3 || got[2] != target {
		t.Fatalf("the move to %s did not complete: %v", target, got)
	}
}

// sweepOnSource runs the real stale-copy sweep on the old owner, as its
// own leader, against whatever engine answers for the new owner.
func sweepOnSource(t *testing.T, src *engineNode, owner *messaging.Engine) []messaging.PartitionTransferInfo {
	t.Helper()
	return sweepAs(t, src, "narad-src", metastore.Assignment{OwnerID: "narad-dst"}, owner)
}

// sweepAs runs the real stale-copy sweep on node as self, its own
// leader, with orders/0 at a (owner and target) in its view, against
// whatever engine answers for a.OwnerID.
func sweepAs(t *testing.T, node *engineNode, self string, a metastore.Assignment, owner *messaging.Engine) []messaging.PartitionTransferInfo {
	t.Helper()
	var seen []messaging.PartitionTransferInfo
	a.Topic, a.Partition = "orders", 0
	store := &fakeMoveStore{
		assignment: a,
		member:     metastore.Member{ID: a.OwnerID, Addr: a.OwnerID + ":7942", Status: metastore.MemberAlive},
		leaderID:   self,
		topics:     []topic.Topic{{Name: "orders", Partitions: 1, RetentionMs: 7_200_000}},
	}
	r := NewMoveRunner(store, self, node.dataDir, ownerEnginePeer{owner: owner, seen: &seen}, node.engine, nil,
		discardLogger(), MoveConfig{})
	r.sweepStaleCopies(context.Background())
	return seen
}

// installWithoutFlip runs a move worker on node that copies orders/0
// from the engine answering for owner and installs it, while every flip
// it proposes returns an unknown outcome and never commits. The worker
// is cancelled once it has proposed `flips` flips, which leaves the
// install, with a move marker naming owner, at the partition's path.
func installWithoutFlip(t *testing.T, node *engineNode, from *messaging.Engine, owner, target string, flips int32) {
	t.Helper()
	store := &unknownFlipStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: owner, TargetID: target},
		member:     metastore.Member{ID: owner, Addr: owner + ":7942", Status: metastore.MemberAlive},
	}, err: fmt.Errorf("%w: peer rpc: reply timeout", errs.ErrUnavailable)}
	r := NewMoveRunner(store, target, node.dataDir, ownerEnginePeer{owner: from}, node.engine, nil,
		discardLogger(), MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.runMove(ctx, "orders", 0, owner)
	}()
	for store.flips.Load() < flips && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if got := store.flips.Load(); got < flips {
		t.Fatalf("setup: the worker proposed %d flips, want %d", got, flips)
	}
	if m, ok, err := messaging.ReadMoveMarker(node.dir()); err != nil || !ok || m.Source != owner {
		t.Fatalf("setup: no install from %s at the partition's path (marker %+v, found %v, err %v)", owner, m, ok, err)
	}
}

func nextOffsetAt(t *testing.T, dir string) int64 {
	t.Helper()
	if _, err := os.Stat(dir); err != nil {
		return -1
	}
	l, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	defer l.Close()
	return l.NextOffset()
}

func quarantinesOf(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(dir + messaging.QuarantineSuffix + "*")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// sealedSegmentFiles lists a partition's non-empty sealed segment files
// in base-offset order (every segment file but the newest).
func sealedSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := listLocalSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for i, s := range segs {
		if i < len(segs)-1 && s.size > 0 {
			out = append(out, filepath.Join(dir, fmt.Sprintf("%020d.log", s.base)))
		}
	}
	return out
}

// requireSetAside fails unless the old owner's want records are in a
// quarantine next to the partition's path and not at the path.
func requireSetAside(t *testing.T, src *engineNode, want int64) {
	t.Helper()
	if _, err := os.Stat(src.dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the stale copy is still at the partition's path (stat err %v): a later install there would delete it", err)
	}
	for _, q := range quarantinesOf(t, src.dir()) {
		if got := nextOffsetAt(t, q); got == want {
			return
		}
	}
	t.Fatalf("LOSS: the old owner's %d records are not in a quarantine (path next offset %d, quarantines %v)",
		want, nextOffsetAt(t, src.dir()), quarantinesOf(t, src.dir()))
}

func TestStaleCopySweepNeverDeletesRecordsTheNewOwnerLacks(t *testing.T) {
	t.Run("healthy owner (control)", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		if n := len(sealedSegmentFiles(t, dst.dir())); n < 3 {
			t.Fatalf("setup: the new owner holds %d sealed segments, want several", n)
		}
		seen := sweepOnSource(t, src, dst.engine)
		if len(seen) != 1 || seen[0].MoveMarker == nil {
			t.Fatalf("owner listing: %+v", seen)
		}
		if _, err := os.Stat(src.dir()); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the healthy owner's stale copy was not reclaimed (stat %v, quarantines %v)", err, quarantinesOf(t, src.dir()))
		}
		if q := quarantinesOf(t, src.dir()); len(q) != 0 {
			t.Fatalf("a copy the owner vouches for was quarantined: %v", q)
		}
		if n := nextOffsetAt(t, dst.dir()); n != 10 {
			t.Fatalf("the new owner holds next offset %d, want 10", n)
		}
	})

	t.Run("owner lost its volume", func(t *testing.T) {
		src, _ := moveThenFlip(t, 10)
		reborn := newEngineNode(t, "narad-dst", "narad-dst", "")
		seen := sweepOnSource(t, src, reborn.engine)
		if len(seen) != 1 || seen[0].MoveMarker != nil || seen[0].HighWatermark != 0 {
			t.Fatalf("setup: the reborn owner's listing is %+v, want no marker and hwm 0", seen)
		}
		requireSetAside(t, src, 10)
	})

	t.Run("owner rolled its install back", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		if err := dst.engine.InstallPartitionDir("orders", 0, func() error { return os.RemoveAll(dst.dir()) }); err != nil {
			t.Fatalf("roll back the install: %v", err)
		}
		seen := sweepOnSource(t, src, dst.engine)
		if len(seen) != 1 || seen[0].MoveMarker != nil || seen[0].HighWatermark != 0 {
			t.Fatalf("setup: listing %+v, want the rolled-back owner without a marker at hwm 0", seen)
		}
		requireSetAside(t, src, 10)
	})

	t.Run("owner rolled back and took new records", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		if err := dst.engine.InstallPartitionDir("orders", 0, func() error { return os.RemoveAll(dst.dir()) }); err != nil {
			t.Fatalf("roll back the install: %v", err)
		}
		// New produce lands at offsets 0..11 of a log the old copy never
		// fed: different records at the same offsets.
		for i := range 12 {
			dst.produce(t, fmt.Sprintf("after-rollback-%d", i))
		}
		seen := sweepOnSource(t, src, dst.engine)
		if len(seen) != 1 || seen[0].MoveMarker != nil || seen[0].HighWatermark < 10 {
			t.Fatalf("setup: listing %+v, want the owner without a marker past the old copy's length", seen)
		}
		requireSetAside(t, src, 10)
	})

	t.Run("owner lost a sealed segment", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		sealed := sealedSegmentFiles(t, dst.dir())
		if len(sealed) < 3 {
			t.Fatalf("setup: %d sealed segments on the new owner, want several", len(sealed))
		}
		if err := dst.engine.InstallPartitionDir("orders", 0, func() error { return os.Remove(sealed[1]) }); err != nil {
			t.Fatalf("drop a sealed segment: %v", err)
		}
		sweepOnSource(t, src, dst.engine)
		requireSetAside(t, src, 10)
	})

	t.Run("the old copy's own marker names the new owner, past a force-promote", func(t *testing.T) {
		// The partition reached the old owner by an earlier move from
		// the node that owns it now, so the old copy carries a marker
		// naming that node. It then moved back, the new owner promoted
		// at 10, and the old owner, cut off, committed 10..14 while the
		// new owner took 8 records of its own. Production-sized
		// segments keep every record in one tail segment on both
		// nodes, and records of about 1 KiB put the first 4 KiB of that
		// segment inside the 10 records the two copies share.
		big := func(label string) string { return label + "-" + strings.Repeat("x", 1000) }
		src := newEngineNodeWithSegments(t, "narad-src", "narad-src", "", 0)
		dst := newEngineNodeWithSegments(t, "narad-dst", "narad-src", "narad-dst", 0)
		for i := range 10 {
			src.produce(t, big(fmt.Sprintf("pre-move-%d", i)))
		}
		if err := messaging.WriteMoveMarker(src.dir(), messaging.MoveMarker{Source: "narad-dst", HighWatermark: 0}); err != nil {
			t.Fatal(err)
		}
		moveInto(t, dst, src.engine, "narad-src", "narad-dst")
		dst.flip(t, "narad-src", "narad-dst")
		src.engine.ResumeProduce("orders", 0)
		for i := range 5 {
			src.produce(t, big(fmt.Sprintf("cut-off-%d", i)))
		}
		src.flip(t, "narad-src", "narad-dst")
		for i := range 8 {
			dst.produce(t, big(fmt.Sprintf("new-owner-%d", i)))
		}
		if n := len(sealedSegmentFiles(t, src.dir())); n != 0 {
			t.Fatalf("setup: the old copy holds %d sealed segments, want every record in its tail", n)
		}
		seen := sweepOnSource(t, src, dst.engine)
		if len(seen) != 1 || seen[0].MoveMarker == nil || seen[0].MoveMarker.HighWatermark != 10 || seen[0].HighWatermark != 18 {
			t.Fatalf("setup: the owner's listing is %+v, want its marker at 10 and hwm 18", seen)
		}
		requireSetAside(t, src, 15)
	})

	t.Run("the partition moved on again after a force-promote", func(t *testing.T) {
		// The new owner promoted at 10 while the old owner, cut off,
		// committed 10..14. Before the old owner's sweep ran, the
		// partition moved on to a third node, whose marker names the
		// node it got the partition from at a position (18) past the
		// old copy's 15 records. That marker vouches for the records
		// the third node was given, not for this copy's: records
		// 10..14 exist only here. Production-sized segments keep every
		// record in one tail segment, which no size check sees.
		src := newEngineNodeWithSegments(t, "narad-src", "narad-src", "", 0)
		dst := newEngineNodeWithSegments(t, "narad-dst", "narad-src", "narad-dst", 0)
		third := newEngineNodeWithSegments(t, "narad-third", "narad-dst", "narad-third", 0)
		for i := range 10 {
			src.produce(t, fmt.Sprintf("pre-move-%d", i))
		}
		moveInto(t, dst, src.engine, "narad-src", "narad-dst")
		dst.flip(t, "narad-src", "narad-dst")
		src.engine.ResumeProduce("orders", 0)
		for i := range 5 {
			src.produce(t, fmt.Sprintf("cut-off-%d", i))
		}
		src.flip(t, "narad-src", "narad-dst")
		for i := range 8 {
			dst.produce(t, fmt.Sprintf("new-owner-%d", i))
		}
		moveInto(t, third, dst.engine, "narad-dst", "narad-third")
		third.flip(t, "narad-dst", "narad-third")
		seen := sweepAs(t, src, "narad-src", metastore.Assignment{OwnerID: "narad-third"}, third.engine)
		if len(seen) != 1 || seen[0].MoveMarker == nil || seen[0].MoveMarker.Source != "narad-dst" ||
			seen[0].MoveMarker.HighWatermark != 18 || seen[0].HighWatermark != 18 {
			t.Fatalf("setup: the owner's listing is %+v, want a marker from narad-dst at 18 and hwm 18", seen)
		}
		requireSetAside(t, src, 15)
	})

	t.Run("owner holds a sealed segment short", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		sealed := sealedSegmentFiles(t, dst.dir())
		if len(sealed) < 3 {
			t.Fatalf("setup: %d sealed segments on the new owner, want several", len(sealed))
		}
		if err := dst.engine.InstallPartitionDir("orders", 0, func() error { return os.Truncate(sealed[1], 3) }); err != nil {
			t.Fatalf("truncate a sealed segment: %v", err)
		}
		sweepOnSource(t, src, dst.engine)
		requireSetAside(t, src, 10)
	})
}

// A copy the new owner cannot vouch for used to be kept in place, where
// the next move of the partition back onto this node (the natural next
// step after the owner lost its volume) deleted it: the install clears
// the partition's path before renaming the copy in. Set aside, it
// survives the move back.
func TestStaleCopyTheOwnerCannotVouchForSurvivesAMoveBack(t *testing.T) {
	src, _ := moveThenFlip(t, 10)
	reborn := newEngineNode(t, "narad-dst", "narad-dst", "")
	sweepOnSource(t, src, reborn.engine)

	// The partition is planned back onto the old owner, which copies the
	// reborn owner's (empty) partition and installs it.
	ctx := context.Background()
	if err := src.store.SetAssignmentTarget(ctx, "orders", 0, "narad-src"); err != nil {
		t.Fatal(err)
	}
	moveInto(t, src, reborn.engine, "narad-dst", "narad-src")
	if n := nextOffsetAt(t, src.dir()); n != 0 {
		t.Fatalf("setup: the moved-back partition recovers next offset %d, want the reborn owner's 0", n)
	}
	for _, q := range quarantinesOf(t, src.dir()) {
		if nextOffsetAt(t, q) == 10 {
			return
		}
	}
	t.Fatalf("LOSS: after the move back the old owner's 10 records exist nowhere (quarantines %v)", quarantinesOf(t, src.dir()))
}

// An install that never flipped is a copy of the owner's records, made
// from the owner. When its move is abandoned (the destination died with
// its flip pending and the controller cleared the target, or the worker
// was cancelled with its flip pending and the move re-planned), the
// install stays at the destination's partition path. The sweep does not
// trust the copy's own move marker to tell such an install from a copy
// this node served (a marker survives every later move of the partition,
// so it cannot), and the owner did not receive the partition from it, so
// the owner vouches for none of it: the install is set aside, never
// deleted, whatever the owner holds.
func TestStaleCopySweepSetsAsideAnInstallThatNeverFlipped(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (src, dst *engineNode) {
		t.Helper()
		src = newEngineNode(t, "narad-src", "narad-src", "")
		dst = newEngineNode(t, "narad-dst", "narad-src", "narad-dst")
		for i := range 10 {
			src.produce(t, fmt.Sprintf("pre-move-%d", i))
		}
		return src, dst
	}
	abortOn := func(t *testing.T, n *engineNode) {
		t.Helper()
		if err := n.store.AbortMove(ctx, "orders", 0, "narad-dst"); err != nil {
			t.Fatalf("AbortMove: %v", err)
		}
	}
	cleared := metastore.Assignment{OwnerID: "narad-src"}

	t.Run("the destination died and the controller cleared the target", func(t *testing.T) {
		src, dst := setup(t)
		installWithoutFlip(t, dst, src.engine, "narad-src", "narad-dst", 1)
		abortOn(t, dst)
		sweepAs(t, dst, "narad-dst", cleared, src.engine)
		requireSetAside(t, dst, 10)
	})

	t.Run("the owner holds an older move marker of its own", func(t *testing.T) {
		src, dst := setup(t)
		// The owner got the partition by an earlier move, promoted at 3.
		if err := messaging.WriteMoveMarker(src.dir(), messaging.MoveMarker{Source: "narad-old", HighWatermark: 3}); err != nil {
			t.Fatal(err)
		}
		installWithoutFlip(t, dst, src.engine, "narad-src", "narad-dst", 1)
		abortOn(t, dst)
		seen := sweepAs(t, dst, "narad-dst", cleared, src.engine)
		if len(seen) != 1 || seen[0].MoveMarker == nil || seen[0].MoveMarker.HighWatermark != 3 {
			t.Fatalf("setup: the owner's listing is %+v, want its marker at 3", seen)
		}
		requireSetAside(t, dst, 10)
	})

	t.Run("the worker was cancelled with its flip pending and the move re-planned", func(t *testing.T) {
		src, dst := setup(t)
		installWithoutFlip(t, dst, src.engine, "narad-src", "narad-dst", 3)
		if err := dst.store.SetAssignmentTarget(ctx, "orders", 0, "narad-other"); err != nil {
			t.Fatalf("SetAssignmentTarget: %v", err)
		}
		sweepAs(t, dst, "narad-dst", metastore.Assignment{OwnerID: "narad-src", TargetID: "narad-other"}, src.engine)
		requireSetAside(t, dst, 10)
	})

	t.Run("the owner came back empty", func(t *testing.T) {
		src, dst := setup(t)
		installWithoutFlip(t, dst, src.engine, "narad-src", "narad-dst", 1)
		abortOn(t, dst)
		reborn := newEngineNode(t, "narad-src", "narad-src", "")
		sweepAs(t, dst, "narad-dst", cleared, reborn.engine)
		requireSetAside(t, dst, 10)
	})

	t.Run("the owner came back empty and took other records at the same offsets", func(t *testing.T) {
		src, dst := setup(t)
		installWithoutFlip(t, dst, src.engine, "narad-src", "narad-dst", 1)
		abortOn(t, dst)
		reborn := newEngineNode(t, "narad-src", "narad-src", "")
		for i := range 12 {
			reborn.produce(t, fmt.Sprintf("new-recs-%d", i))
		}
		seen := sweepAs(t, dst, "narad-dst", cleared, reborn.engine)
		if len(seen) != 1 || seen[0].MoveMarker != nil || seen[0].HighWatermark < 10 {
			t.Fatalf("setup: the reborn owner's listing is %+v, want records past 10 and no marker", seen)
		}
		requireSetAside(t, dst, 10)
	})
}

// A partition can be planned back onto its old owner before that node's
// stale-copy sweep has judged its old copy (the sweep runs every
// moveSweepEvery reconcile ticks; the rebalance pass, every 10 s). From
// then on the sweep skips the partition, and the move's install is what
// meets the old copy at the partition's path. Whenever it holds
// unexpired records, the install sets it aside, never deletes it: no
// comparison with the incoming copy is trusted to prove it redundant.
func TestAMoveBackBeforeTheSweepNeverDeletesTheOldCopysRecords(t *testing.T) {
	ctx := context.Background()
	moveBack := func(t *testing.T, src *engineNode, from *messaging.Engine) {
		t.Helper()
		if err := src.store.SetAssignmentTarget(ctx, "orders", 0, "narad-src"); err != nil {
			t.Fatal(err)
		}
		moveInto(t, src, from, "narad-dst", "narad-src")
	}
	requireMovedBackAndKept := func(t *testing.T, src *engineNode, incoming, kept int64) {
		t.Helper()
		if n := nextOffsetAt(t, src.dir()); n != incoming {
			t.Fatalf("setup: the moved-back partition recovers next offset %d, want the incoming copy's %d", n, incoming)
		}
		for _, q := range quarantinesOf(t, src.dir()) {
			if nextOffsetAt(t, q) == kept {
				return
			}
		}
		t.Fatalf("LOSS: the install deleted the old copy's records (quarantines %v, want one recovering to %d)", quarantinesOf(t, src.dir()), kept)
	}

	t.Run("the old owner kept committing past a force-promote", func(t *testing.T) {
		// The destination promoted at 10 while the old owner, cut off,
		// committed 10..14.
		src := newEngineNode(t, "narad-src", "narad-src", "")
		dst := newEngineNode(t, "narad-dst", "narad-src", "narad-dst")
		for i := range 10 {
			src.produce(t, fmt.Sprintf("pre-move-%d", i))
		}
		moveInto(t, dst, src.engine, "narad-src", "narad-dst")
		dst.flip(t, "narad-src", "narad-dst")
		src.engine.ResumeProduce("orders", 0)
		for i := range 5 {
			src.produce(t, fmt.Sprintf("cut-off-%d", i))
		}
		src.flip(t, "narad-src", "narad-dst")
		moveBack(t, src, dst.engine)
		requireMovedBackAndKept(t, src, 10, 15)
	})

	t.Run("the new owner came back empty", func(t *testing.T) {
		src, _ := moveThenFlip(t, 10)
		reborn := newEngineNode(t, "narad-dst", "narad-dst", "")
		moveBack(t, src, reborn.engine)
		requireMovedBackAndKept(t, src, 0, 10)
	})

	t.Run("the new owner lost a sealed segment", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		sealed := sealedSegmentFiles(t, dst.dir())
		if len(sealed) < 3 {
			t.Fatalf("setup: %d sealed segments on the new owner, want several", len(sealed))
		}
		if err := dst.engine.InstallPartitionDir("orders", 0, func() error { return os.Remove(sealed[1]) }); err != nil {
			t.Fatalf("drop a sealed segment: %v", err)
		}
		moveBack(t, src, dst.engine)
		requireMovedBackAndKept(t, src, 10, 10)
	})

	t.Run("an old copy the incoming one covers is set aside too", func(t *testing.T) {
		src, dst := moveThenFlip(t, 10)
		dst.produce(t, "after-the-move")
		moveBack(t, src, dst.engine)
		requireMovedBackAndKept(t, src, 11, 10)
	})

	t.Run("control: an old copy with no records is replaced", func(t *testing.T) {
		src, dst := moveThenFlip(t, 0)
		dst.produce(t, "after-the-move")
		moveBack(t, src, dst.engine)
		if n := nextOffsetAt(t, src.dir()); n != 1 {
			t.Fatalf("the moved-back partition recovers next offset %d, want 1", n)
		}
		if q := quarantinesOf(t, src.dir()); len(q) != 0 {
			t.Fatalf("an old copy with no records was quarantined: %v", q)
		}
	})
}
