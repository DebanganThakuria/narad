package messaging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// The ingress dispatcher retries a failed CommitAcceptedProduceBatch by
// calling it again with the same WAL records. A commit that fails on the
// owner side (here: the partition directory cannot take the segment the
// roll needs) must leave nothing behind, so the retry delivers the
// batch exactly once instead of exposing two copies (audit finding 2.1).
func TestCommitAcceptedProduceBatchRetryAfterFailureDeliversOnce(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dataDir := t.TempDir()
	const topicName = "orders"
	ms := newMessagingFakeMetastore()
	ms.topics[topicName] = topic.Topic{Name: topicName, Partitions: 1, VisibilityTimeoutMs: 60_000}

	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond, SegmentBytes: 64}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	record := func(id string, payload string) ingress.ProduceRecord {
		return ingress.ProduceRecord{
			Topic: topicName, Key: id, TargetPartition: 0, Payload: []byte(payload),
			CreatedAtUnixMs: time.Now().UnixMilli(),
		}
	}
	commit := func(recs ...ingress.ProduceRecord) ([]int64, error) {
		return engine.CommitAcceptedProduceBatch(context.Background(), recs)
	}

	// A first small batch creates the partition's hwm file while the
	// directory is writable.
	if _, err := commit(record("a", `{"id":"a"}`)); err != nil {
		t.Fatalf("commit a: %v", err)
	}

	// The second batch fills the segment. With the directory read-only
	// the roll it wants after the commit is deferred (the batch is
	// committed regardless), and the next batch's roll fails.
	partitionDir := storage.TopicPartitionDir(dataDir, topicName, 0)
	if err := os.Chmod(partitionDir, 0o500); err != nil {
		t.Fatal(err)
	}
	restore := func() { _ = os.Chmod(partitionDir, 0o700) }
	t.Cleanup(restore)
	if _, err := commit(record("b", `{"id":"b","pad":"`+strings.Repeat("x", 64)+`"}`)); err != nil {
		t.Fatalf("commit b: %v", err)
	}

	failed := record("c", `{"id":"c"}`)
	if got, err := commit(failed); err == nil {
		t.Fatalf("commit c succeeded (%v) although the segment roll could not create a file", got)
	}

	// The disk recovers; the dispatcher retries the same WAL record.
	restore()
	got, err := commit(failed)
	if err != nil {
		t.Fatalf("retry commit c: %v", err)
	}
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("retry offsets = %v, want [2] (the offset the failed batch had)", got)
	}

	// Consume everything: a, b, c once each, then nothing.
	partition := 0
	var seen []string
	for {
		msg, found, err := engine.Consume(context.Background(), topicName, ConsumeOpts{Partition: &partition})
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		if !found {
			break
		}
		seen = append(seen, msg.Key)
		if len(seen) > 3 {
			break
		}
	}
	if strings.Join(seen, ",") != "a,b,c" {
		t.Fatalf("consumed keys = %v, want [a b c] exactly once each", seen)
	}
}
