package cluster

// A fan-out cursor with no resume point must start at the link's
// recorded attach point, never at whatever the parent's tail happens
// to be when the cursor first reads. The two scenarios that lost
// records before: a parent partition created by a partition-count
// increase (producers hash to it immediately, the cursor spawned a
// reconcile pass later and skipped everything), and the window between
// an attach committing and its cursors' first read.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

func TestAttachAnchor(t *testing.T) {
	legacy := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "e"}
	recorded := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "e", AttachOffsets: []int64{5, 0}}
	cases := []struct {
		name      string
		child     topic.Topic
		partition int
		want      int64
	}{
		{"legacy record anchors at the tail", legacy, 0, topic.FanoutTailOffset},
		{"legacy record, partition beyond any count, still tail", legacy, 7, topic.FanoutTailOffset},
		{"recorded offset", recorded, 0, 5},
		{"recorded zero offset", recorded, 1, 0},
		{"partition created after the attach starts at 0", recorded, 2, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := attachAnchor(tc.child, tc.partition); got != tc.want {
				t.Fatalf("attachAnchor(%v, %d) = %d, want %d", tc.child.AttachOffsets, tc.partition, got, tc.want)
			}
		})
	}
}

// Audit finding 4.2 / 1.5: a partition added by a partition-count
// increase gets producer traffic before the reconciler spawns its
// cursor. The cursor must anchor at 0 and deliver all of it, not
// tail-anchor past it.
func TestFanoutRunnerNewParentPartitionDeliversRecordsBeforeCursorStart(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := env.newRunner(t) // registers the attach-offset resolver
	if err := env.store.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, err := env.store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}
	if !slices.Equal(child.AttachOffsets, []int64{0, 0, 0}) {
		t.Fatalf("attach offsets = %v, want [0 0 0] for an empty parent", child.AttachOffsets)
	}
	runner.Reconcile(ctx)
	defer func() { cancel(); runner.wg.Wait() }()
	waitForCursorFiles(t, env, child.AttachEpoch, 3)

	// Operator increases the parent's partition count; the controller
	// assigns the new partition; producers use it immediately, all
	// before the reconciler's next pass.
	parent, err := env.store.GetTopic(ctx, "parent")
	if err != nil {
		t.Fatalf("GetTopic(parent): %v", err)
	}
	parent.Partitions = 4
	if err := env.store.UpdateTopic(ctx, parent); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if err := env.store.AssignPartition(ctx, "parent", 3, "node-self"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	env.produceToParent(t, 3, 5, 2, 5000)

	runner.Reconcile(ctx)
	env.waitChildTotal(t, 5)
	waitForCursorAt(t, env, 3, child.AttachEpoch, 5)
}

// waitForCursorAt polls until the cursor file for parent/p carries
// epoch and next. The file is persisted just AFTER the child commit
// becomes visible (commit-before-advance), so a read right after
// waitChildTotal can still see the pre-commit position.
func waitForCursorAt(t *testing.T, env *fanoutTestEnv, p int, epoch string, next int64) {
	t.Helper()
	dir := storage.TopicPartitionDir(env.dataDir, "parent", p)
	deadline := time.Now().Add(5 * time.Second)
	for {
		cur, ok, err := storage.ReadFanoutCursor(dir, "child")
		if err != nil {
			t.Fatalf("ReadFanoutCursor(parent/%d): %v", p, err)
		}
		if ok && cur.Epoch == epoch && cur.NextOffset == next {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent/%d cursor = %+v (ok=%v), want epoch %q next %d", p, cur, ok, epoch, next)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Audit finding 4.2: records committed between the attach and the
// cursor's first read belong to the child ("from the attach point
// forward"), while records committed before the attach do not.
func TestFanoutRunnerAttachWindowIsDelivered(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := env.newRunner(t)            // registers the attach-offset resolver
	env.produceToParent(t, 0, 5, 3, 1000) // pre-attach history: never delivered

	if err := env.store.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, err := env.store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}
	if !slices.Equal(child.AttachOffsets, []int64{5, 0, 0}) {
		t.Fatalf("attach offsets = %v, want [5 0 0]", child.AttachOffsets)
	}

	// The attach window: committed after the attach, before any cursor
	// has run. The old tail anchor skipped exactly these.
	env.produceToParent(t, 0, 3, 3, 0)
	env.produceToParent(t, 1, 2, 3, 100)

	runner.Reconcile(ctx)
	defer func() { cancel(); runner.wg.Wait() }()
	env.waitChildTotal(t, 5)
	time.Sleep(200 * time.Millisecond)
	if got := env.childTotal(t); got != 5 {
		t.Fatalf("child records = %d, want exactly 5 (3 + 2 from the attach window, none of the 5 pre-attach)", got)
	}
	waitForCursorAt(t, env, 0, child.AttachEpoch, 8)
}

// The resolver asks remote owners through the partition stats RPC and
// refuses to answer at all when any owner cannot be reached.
func TestFanoutRunnerAttachOffsetsAsksEveryOwner(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "parent", Partitions: 3, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	for p, owner := range []string{"node-self", "node-remote", "node-remote"} {
		if err := store.AssignPartition(ctx, "parent", p, owner); err != nil {
			t.Fatalf("AssignPartition(%d): %v", p, err)
		}
	}

	local := &fakeFanoutBroker{hwm: map[int]int64{0: 4}}
	peer := fakePeerClient{topicPartitionStatsFn: func(_ context.Context, addr, topicName string, p int) (topic.PartitionStats, error) {
		if addr != "remote.example:7942" || topicName != "parent" {
			t.Errorf("stats request addr=%q topic=%q, want remote.example:7942/parent", addr, topicName)
		}
		return topic.PartitionStats{Index: p, HighWatermark: int64(10 * p)}, nil
	}}
	r := NewFanoutRunner(store, "node-self", t.TempDir(), local, peer, partition.NewHashRoundRobin(), nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), FanoutConfig{})

	got, err := r.AttachOffsets(ctx, "parent")
	if err != nil {
		t.Fatalf("AttachOffsets: %v", err)
	}
	if !slices.Equal(got, []int64{4, 10, 20}) {
		t.Fatalf("AttachOffsets = %v, want [4 10 20]", got)
	}

	// Registration: the store consults this runner for attaches.
	if err := store.CreateTopic(ctx, topic.Topic{Name: "child", Partitions: 3, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic(child): %v", err)
	}
	if err := store.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, err := store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}
	if !slices.Equal(child.AttachOffsets, []int64{4, 10, 20}) {
		t.Fatalf("recorded attach offsets = %v, want [4 10 20]", child.AttachOffsets)
	}

	// One unreachable owner fails the resolution, and the attach.
	r.peer = fakePeerClient{topicPartitionStatsFn: func(_ context.Context, _, _ string, p int) (topic.PartitionStats, error) {
		if p == 2 {
			return topic.PartitionStats{}, errors.New("dial: connection refused")
		}
		return topic.PartitionStats{Index: p, HighWatermark: 1}, nil
	}}
	if _, err := r.AttachOffsets(ctx, "parent"); err == nil || !strings.Contains(err.Error(), "partition 2") {
		t.Fatalf("AttachOffsets with an unreachable owner error = %v, want a partition 2 failure", err)
	}
	if err := store.DetachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("DetachChild: %v", err)
	}
	if err := store.AttachChild(ctx, "parent", "child", 0); !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("AttachChild with an unreachable owner error = %v, want %v", err, errs.ErrUnavailable)
	}
}

// fakeFanoutBroker answers tail-anchor slab reads with a fixed HWM per
// partition; it is enough for the attach-offset resolver.
type fakeFanoutBroker struct {
	hwm map[int]int64
}

func (f *fakeFanoutBroker) ReadFanoutSlab(_ context.Context, _ string, p int, opts topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	if opts.FromOffset != topic.FanoutTailOffset {
		return topic.FanoutSlab{}, errors.New("fake broker only serves tail reads")
	}
	return topic.FanoutSlab{NextOffset: f.hwm[p], HighWatermark: f.hwm[p]}, nil
}

func (f *fakeFanoutBroker) CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error) {
	return nil, errors.New("not implemented")
}

// A recorded anchor must not depend on the parent partition's directory
// already existing: a fresh parent that has never been produced to has
// no directory, and the cursor file lives in it. The tail anchor's slab
// read created it as a side effect; the recorded anchor path must do
// the same, or the persist fails with "partition removed", the cursor
// stops, the reconciler respawns it, and the child never catches up
// (seen as a 460-iteration loop on a fresh soak topology).
func TestFanoutRunnerRecordedAnchorOnNeverProducedPartition(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := env.newRunner(t)
	if err := env.store.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	child, err := env.store.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}
	// The attach-offset resolver opened the local logs to read their
	// tails; a remote owner answers that from directory stats and never
	// opens anything. Reproduce that state: close the logs and remove
	// the parent's directories before the cursors start.
	if err := env.logs.CloseTopic("parent"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(env.dataDir, "topics", "parent")); err != nil {
		t.Fatalf("remove parent dirs: %v", err)
	}
	runner.Reconcile(ctx)
	defer func() { cancel(); runner.wg.Wait() }()

	// Every partition's cursor anchors at 0 and persists, without any
	// produce having created the partition directories.
	for p := range 3 {
		waitForCursorAt(t, env, p, child.AttachEpoch, 0)
	}
	// And the first records produced afterwards are delivered.
	env.produceToParent(t, 0, 2, 3, 0)
	env.waitChildTotal(t, 2)
}
