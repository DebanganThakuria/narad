package broker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
	"github.com/debanganthakuria/narad/internal/topic"
)

// newTestBroker constructs a broker backed by a per-test temp data dir
// and the standard set of single-node implementations.
func newTestBroker(t *testing.T, policy broker.TopicPolicy) (broker.Broker, string) {
	t.Helper()
	dir := t.TempDir()
	ms, err := metastore.NewSQLiteStore(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	br, err := broker.New(broker.Deps{
		DataDir:     dir,
		LogOptions:  storage.DefaultOptions(),
		TopicPolicy: policy,
		Metastore:   ms,
		Partitions:  partition.NewHashRoundRobin(),
		Schemas:     schema.NewAlwaysValid(),
		Offsets:     consumer.NewInFlight(consumer.NewMetastoreBacked(ms)),
		Replicator:  replication.NewLocal(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = br.Close() })
	return br, dir
}

// Default policy used by tests that aren't exercising bounds. Mirrors
// the production defaults from internal/config (RF >= 2 is enforced
// at create time).
var defaultPolicy = broker.TopicPolicy{
	DefaultPartitions:        8,
	MaxPartitions:            1024,
	DefaultReplicationFactor: 2,
	DefaultRetentionMs: topic.Retention{
		MaxAgeMs: 7 * 24 * 60 * 60 * 1000, // 7 days
	},
}

// noRetention is the test convenience for "use whatever the policy
// defaults give me", since CreateTopic now takes a retention struct.
var noRetention = topic.Retention{}

func TestCreateTopicAppliesDefaultPartitions(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	// partitions = 0 → use default (8). RF = 0 → use default (2).
	got, err := br.CreateTopic(ctx, "orders", 0, 0, noRetention)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got.Partitions != 8 {
		t.Fatalf("partitions: got %d want 8", got.Partitions)
	}
	if got.ReplicationFactor != 2 {
		t.Fatalf("replication_factor: got %d want 2", got.ReplicationFactor)
	}
	if got.Retention.MaxAgeMs != defaultPolicy.DefaultRetentionMs.MaxAgeMs {
		t.Fatalf("retention.max_age_ms: got %d want %d", got.Retention.MaxAgeMs, defaultPolicy.DefaultRetentionMs.MaxAgeMs)
	}
}

func TestCreateTopicRespectsExplicitValues(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	got, err := br.CreateTopic(ctx, "orders", 16, 2, topic.Retention{MaxAgeMs: 60_000, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got.Partitions != 16 {
		t.Fatalf("partitions: got %d want 16", got.Partitions)
	}
	if got.Retention.MaxAgeMs != 60_000 || got.Retention.MaxBytes != 1<<20 {
		t.Fatalf("retention: got %+v want {60000, 1MiB}", got.Retention)
	}
}

func TestCreateTopicRetentionFallsBackToDefault(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	// Only MaxBytes set; MaxAgeMs zero → inherit policy default for age.
	got, err := br.CreateTopic(ctx, "orders", 4, 2, topic.Retention{MaxBytes: 4096})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got.Retention.MaxAgeMs != defaultPolicy.DefaultRetentionMs.MaxAgeMs {
		t.Fatalf("MaxAgeMs: got %d want default %d", got.Retention.MaxAgeMs, defaultPolicy.DefaultRetentionMs.MaxAgeMs)
	}
	if got.Retention.MaxBytes != 4096 {
		t.Fatalf("MaxBytes: got %d want 4096", got.Retention.MaxBytes)
	}
}

func TestCreateTopicRejectsNegativeRetention(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	_, err := br.CreateTopic(ctx, "bad", 4, 2, topic.Retention{MaxAgeMs: -1})
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for negative max_age_ms, got %v", err)
	}
}

func TestCreateTopicRejectsAboveMax(t *testing.T) {
	br, _ := newTestBroker(t, broker.TopicPolicy{
		DefaultPartitions:        4,
		MaxPartitions:            8,
		DefaultReplicationFactor: 2,
	})
	ctx := context.Background()

	_, err := br.CreateTopic(ctx, "too-big", 9, 2, noRetention)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
}

func TestCreateTopicRejectsNegativePartitions(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	_, err := br.CreateTopic(ctx, "neg", -1, 2, noRetention)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
}

func TestIncreaseTopicPartitionsHappyPath(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 2, noRetention); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	updated, err := br.IncreaseTopicPartitions(ctx, "orders", 16)
	if err != nil {
		t.Fatalf("IncreaseTopicPartitions: %v", err)
	}
	if updated.Partitions != 16 {
		t.Fatalf("returned topic has %d partitions, want 16", updated.Partitions)
	}
	got, err := br.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.Partitions != 16 {
		t.Fatalf("GetTopic after increase: %d, want 16", got.Partitions)
	}
}

func TestIncreaseTopicPartitionsRejectsDecrease(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "t", 16, 2, noRetention); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := br.IncreaseTopicPartitions(ctx, "t", 8)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for decrease, got %v", err)
	}
}

func TestIncreaseTopicPartitionsRejectsEqual(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "t", 8, 2, noRetention); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := br.IncreaseTopicPartitions(ctx, "t", 8)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for equal, got %v", err)
	}
}

func TestIncreaseTopicPartitionsRejectsAboveMax(t *testing.T) {
	br, _ := newTestBroker(t, broker.TopicPolicy{
		DefaultPartitions:        4,
		MaxPartitions:            16,
		DefaultReplicationFactor: 2,
	})
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "t", 4, 2, noRetention); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := br.IncreaseTopicPartitions(ctx, "t", 32)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument above max, got %v", err)
	}
}

func TestIncreaseTopicPartitionsTopicNotFound(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	_, err := br.IncreaseTopicPartitions(ctx, "missing", 16)
	if !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("want ErrTopicNotFound, got %v", err)
	}
}

func TestUpdateTopicRetentionHappyPath(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 2, topic.Retention{MaxAgeMs: 60_000, MaxBytes: 1024}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Open a partition log via Produce so we can verify it gets reopened
	// with the new retention bounds.
	if _, _, err := br.Produce(ctx, "orders", "k1", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	updated, err := br.UpdateTopicRetention(ctx, "orders", topic.Retention{MaxAgeMs: 600_000, MaxBytes: 4096})
	if err != nil {
		t.Fatalf("UpdateTopicRetention: %v", err)
	}
	if updated.Retention.MaxAgeMs != 600_000 || updated.Retention.MaxBytes != 4096 {
		t.Fatalf("retention: got %+v want {600000, 4096}", updated.Retention)
	}

	got, err := br.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.Retention.MaxAgeMs != 600_000 {
		t.Fatalf("persisted MaxAgeMs: %d, want 600000", got.Retention.MaxAgeMs)
	}

	// A subsequent produce must succeed — the previously-cached log was
	// closed by UpdateTopicRetention, so this exercises the reopen path.
	if _, _, err := br.Produce(ctx, "orders", "k2", []byte(`{"x":2}`)); err != nil {
		t.Fatalf("Produce after retention update: %v", err)
	}
}

func TestUpdateTopicRetentionFallsBackToDefault(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 2, topic.Retention{MaxAgeMs: 60_000}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	updated, err := br.UpdateTopicRetention(ctx, "orders", topic.Retention{}) // both zero
	if err != nil {
		t.Fatalf("UpdateTopicRetention: %v", err)
	}
	if updated.Retention.MaxAgeMs != defaultPolicy.DefaultRetentionMs.MaxAgeMs {
		t.Fatalf("MaxAgeMs not defaulted: got %d want %d", updated.Retention.MaxAgeMs, defaultPolicy.DefaultRetentionMs.MaxAgeMs)
	}
}

func TestUpdateTopicRetentionTopicNotFound(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	_, err := br.UpdateTopicRetention(ctx, "missing", topic.Retention{MaxAgeMs: 1000})
	if !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("want ErrTopicNotFound, got %v", err)
	}
}

func TestDeleteTopicWipesEverything(t *testing.T) {
	br, dir := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 2, noRetention); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, _, err := br.Produce(ctx, "orders", "k1", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if err := br.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	if _, err := br.GetTopic(ctx, "orders"); !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("GetTopic after delete: want ErrTopicNotFound, got %v", err)
	}

	topicDir := filepath.Join(dir, "topics", "orders")
	if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
		t.Fatalf("topic directory still present: %v", err)
	}
}

func TestDeleteTopicNotFound(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if err := br.DeleteTopic(ctx, "missing"); !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("want ErrTopicNotFound, got %v", err)
	}
}

func TestGetTopicDetailsReportsStats(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 2, noRetention); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 3 records on the same key so they all land on the same
	// partition (HashRoundRobin keyed mode is deterministic).
	for i := range 3 {
		if _, _, err := br.Produce(ctx, "orders", "fixed-key", []byte(`{"v":1}`)); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	d, err := br.GetTopicDetails(ctx, "orders")
	if err != nil {
		t.Fatalf("GetTopicDetails: %v", err)
	}
	if d.Name != "orders" {
		t.Fatalf("topic name: %q", d.Name)
	}
	if len(d.Partitions) != 4 {
		t.Fatalf("partition stats: %d (want 4)", len(d.Partitions))
	}
	// Sum of NextOffset across partitions == total produced.
	var sum int64
	for _, p := range d.Partitions {
		sum += p.NextOffset
	}
	if sum != 3 {
		t.Fatalf("sum of NextOffset = %d, want 3", sum)
	}
}

func TestMultipleTopicsAreIndependent(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "a", 4, 2, noRetention); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := br.CreateTopic(ctx, "b", 0, 0, noRetention); err != nil {
		t.Fatalf("create b: %v", err)
	}

	a, err := br.GetTopic(ctx, "a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	b, err := br.GetTopic(ctx, "b")
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if a.Partitions != 4 || b.Partitions != 8 {
		t.Fatalf("partitions: a=%d b=%d (want 4 / 8)", a.Partitions, b.Partitions)
	}

	// Producing to one topic doesn't affect the other's offset space.
	off1, _, err := br.Produce(ctx, "a", "k1", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("produce a: %v", err)
	}
	off2, _, err := br.Produce(ctx, "b", "k1", []byte(`{"x":2}`))
	if err != nil {
		t.Fatalf("produce b: %v", err)
	}
	if off1 != 0 || off2 != 0 {
		t.Fatalf("each topic's first record should be offset 0; got a=%d b=%d", off1, off2)
	}
}

func TestListTopicsPagination(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if _, err := br.CreateTopic(ctx, name, 1, 2, noRetention); err != nil {
			t.Fatalf("CreateTopic %q: %v", name, err)
		}
	}

	// First page: limit 2.
	page1, nextToken, err := br.ListTopics(ctx, metastore.ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("ListTopics page 1: %v", err)
	}
	if got, want := len(page1), 2; got != want {
		t.Fatalf("page 1 len: got %d want %d", got, want)
	}
	if nextToken != "b" {
		t.Fatalf("page 1 next_token: got %q want %q", nextToken, "b")
	}
	if page1[0].Name != "a" || page1[1].Name != "b" {
		t.Fatalf("page 1 names: got [%s, %s]", page1[0].Name, page1[1].Name)
	}

	// Second page picks up after "b".
	page2, nextToken, err := br.ListTopics(ctx, metastore.ListOptions{Limit: 2, PageToken: nextToken})
	if err != nil {
		t.Fatalf("ListTopics page 2: %v", err)
	}
	if page2[0].Name != "c" || page2[1].Name != "d" {
		t.Fatalf("page 2 names: got [%s, %s]", page2[0].Name, page2[1].Name)
	}
	if nextToken != "d" {
		t.Fatalf("page 2 next_token: got %q want %q", nextToken, "d")
	}

	// Third page returns the final row and signals no-more via empty token.
	page3, nextToken, err := br.ListTopics(ctx, metastore.ListOptions{Limit: 2, PageToken: nextToken})
	if err != nil {
		t.Fatalf("ListTopics page 3: %v", err)
	}
	if len(page3) != 1 || page3[0].Name != "e" {
		t.Fatalf("page 3: got %d topics, want 1 (e)", len(page3))
	}
	if nextToken != "" {
		t.Fatalf("page 3 next_token: got %q want empty (no more pages)", nextToken)
	}
}

func TestListTopicsUnpaginatedReturnsAll(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		if _, err := br.CreateTopic(ctx, name, 1, 2, noRetention); err != nil {
			t.Fatalf("CreateTopic %q: %v", name, err)
		}
	}

	all, nextToken, err := br.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("len: got %d want 3", len(all))
	}
	if nextToken != "" {
		t.Fatalf("next_token: got %q want empty (limit=0 = no pagination)", nextToken)
	}
}
