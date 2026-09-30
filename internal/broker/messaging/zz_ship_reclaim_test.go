package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// zzShipMovedTopic creates topic name (incarnation id, parts partitions)
// with every partition assigned to node-other, so this node (node-self)
// holds only stale copies.
func zzShipMovedTopic(t *testing.T, store *metastore.Store, name, id string, parts int) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, ID: id, Partitions: parts}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, member := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: member, Addr: member + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", member, err)
		}
	}
	for p := range parts {
		if err := store.AssignPartition(ctx, name, p, "node-other"); err != nil {
			t.Fatalf("AssignPartition(%d): %v", p, err)
		}
	}
}

// zzShipStaleCopy writes a closed partition copy holding n records.
func zzShipStaleCopy(t *testing.T, dataDir, name string, partition, n int) string {
	t.Helper()
	dir := storage.TopicPartitionDir(dataDir, name, partition)
	log, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	for i := range n {
		if _, err := log.Append(storage.EncodeKeyedRecord("k", int64(i), []byte("x"))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := log.AdvanceHighWatermark(int64(n)); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dir
}

// A reclaim proceeds when the topic directory's incarnation marker names
// the incarnation the reclaim read, which is what every copy a topic
// with an ID left behind carries: the stale copy is removed, guarded or
// not, and the topic directory keeps its marker.
func TestReclaimRemovesCopyMarkedWithItsIncarnation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	zzShipMovedTopic(t, store, "orders", "inc-live", 2)
	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	t.Cleanup(func() { _ = engine.logs.CloseAll() })
	dataDir := engine.logs.DataDir()

	// Opened through the log map, as the old owner served them: that
	// stamps the topic directory with the incarnation.
	for p := range 2 {
		log, err := engine.logs.Get("orders", p)
		if err != nil {
			t.Fatalf("Get(%d): %v", p, err)
		}
		for range 3 {
			if _, err := log.Append(storage.EncodeKeyedRecord("k", 1, []byte("x"))); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := log.CommitDurable(0, 2); err != nil {
			t.Fatalf("CommitDurable: %v", err)
		}
	}
	topicDir := storage.TopicDir(dataDir, "orders")
	if id, marked, err := storage.ReadTopicIncarnation(topicDir); err != nil || !marked || id != "inc-live" {
		t.Fatalf("marker = (%q, %v, %v), want inc-live", id, marked, err)
	}

	for p, guard := range []ReclaimGuard{{}, {Known: true, PromotedHWM: 3}} {
		if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", p, guard); err != nil {
			t.Fatalf("reclaim of p%d (guard %+v): %v", p, guard, err)
		}
		dir := storage.TopicPartitionDir(dataDir, "orders", p)
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("p%d copy still present after its reclaim (stat err %v)", p, err)
		}
		if _, err := os.Stat(dir + QuarantineSuffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("p%d copy at the promoted hwm was quarantined, want removed (stat err %v)", p, err)
		}
	}
	if id, marked, err := storage.ReadTopicIncarnation(topicDir); err != nil || !marked || id != "inc-live" {
		t.Fatalf("marker after the reclaims = (%q, %v, %v), want inc-live kept", id, marked, err)
	}
}

// zzShipTopicUnreadableMS is the metastore with topic reads failing, as
// a replica mid-restore or a broken local store would.
type zzShipTopicUnreadableMS struct {
	*metastore.Store
}

var errZZShipTopicRead = errors.New("zz ship: topic record unreadable")

func (zzShipTopicUnreadableMS) GetTopic(context.Context, string) (topic.Topic, error) {
	return topic.Topic{}, errZZShipTopicRead
}

// A reclaim that cannot read the topic record does not know which
// incarnation it is about, so it refuses before touching anything.
func TestReclaimRefusesWhenTopicUnreadable(t *testing.T) {
	store := newTestStore(t)
	zzShipMovedTopic(t, store, "orders", "inc-live", 1)
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(zzShipTopicUnreadableMS{store}, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	t.Cleanup(func() { engine.dispatch.close() })
	dir := zzShipStaleCopy(t, dataDir, "orders", 0, 3)

	err := engine.ReclaimMovedPartitionGuarded(context.Background(), "orders", 0, ReclaimGuard{})
	if !errors.Is(err, errZZShipTopicRead) || !strings.Contains(err.Error(), "reclaim refused: topic unreadable") {
		t.Fatalf("reclaim = %v, want a refusal carrying the topic read failure", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("copy touched by a refused reclaim: %v", err)
	}
}

// A reclaim that cannot read the topic directory's incarnation marker
// cannot tell the moved incarnation's copy from a successor's, so it
// refuses and leaves the directory alone.
func TestReclaimRefusesWhenIncarnationMarkerUnreadable(t *testing.T) {
	store := newTestStore(t)
	zzShipMovedTopic(t, store, "orders", "inc-live", 1)
	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	t.Cleanup(func() { _ = engine.logs.CloseAll() })
	dataDir := engine.logs.DataDir()
	dir := zzShipStaleCopy(t, dataDir, "orders", 0, 3)
	// A directory where the marker file should be: reading it fails.
	if err := os.Mkdir(filepath.Join(storage.TopicDir(dataDir, "orders"), storage.IncarnationMarkerFileName), 0o755); err != nil {
		t.Fatal(err)
	}

	err := engine.ReclaimMovedPartitionGuarded(context.Background(), "orders", 0, ReclaimGuard{})
	if err == nil || !strings.Contains(err.Error(), "reclaim refused: read topic incarnation") {
		t.Fatalf("reclaim = %v, want a refusal on the unreadable marker", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("copy touched by a refused reclaim: %v", err)
	}
}
