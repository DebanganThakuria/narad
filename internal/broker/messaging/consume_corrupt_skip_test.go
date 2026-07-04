package messaging

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// End-to-end: a queue consumer must skip past a permanently-unreadable
// (corrupt) committed frame instead of head-of-line-blocking the partition,
// deliver the healthy records around it, record the loss in the metric, and
// persist the advanced frontier so the poison offset is never re-attempted.
func TestQueueConsumeSkipsCorruptFrameEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	const topicName = "orders"

	// Phase 1: write three single-record frames (offsets 0,1,2) and make them
	// visible, then close so the data + high-watermark are durable on disk.
	payloads := [][]byte{
		[]byte(`{"id":1,"m":"AAAAAAAAAAAA"}`),
		[]byte(`{"id":2,"m":"BBBBBBBBBBBB"}`), // this frame will be corrupted
		[]byte(`{"id":3,"m":"CCCCCCCCCCCC"}`),
	}
	corruptMarker := []byte("BBBBBBBBBBBB")

	logs1 := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, nil, nil)
	log1, err := logs1.Get(topicName, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i, p := range payloads {
		if _, err := log1.Append(p); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := log1.Sync(); err != nil { // one Sync => one frame
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if err := log1.AdvanceHighWatermark(int64(len(payloads))); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := logs1.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}

	// Corrupt the middle frame in place: with the default noop codec the record
	// payload is stored verbatim, so flipping a byte of its marker breaks that
	// frame's CRC while leaving its header (and the neighbouring frames) intact.
	corruptRecordPayloadOnDisk(t, dataDir, topicName, 0, corruptMarker)

	// Phase 2: reopen and consume in queue mode.
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	ms := newMessagingFakeMetastore()
	ms.topics[topicName] = topic.Topic{Name: topicName, Partitions: 1, VisibilityTimeoutMs: 60_000}

	logs2 := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs2.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(topic string, partition int, offset int64) {
		dir := storage.TopicPartitionDir(dataDir, topic, partition)
		if err := storage.WriteConsumerOffset(dir, offset); err != nil {
			t.Errorf("WriteConsumerOffset: %v", err)
		}
	})
	engine := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs2, nil, m, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	// Drain the partition: collect delivered offsets, acking each. Bounded so a
	// bug can't loop forever.
	delivered := map[int64]string{}
	ctx := context.Background()
	for range 20 {
		msg, found, err := engine.Consume(ctx, topicName, ConsumeOpts{Partition: new(0)})
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		if !found {
			if len(delivered) == 2 { // got 0 and 2; corrupt 1 skipped
				break
			}
			continue
		}
		delivered[msg.Offset] = string(msg.Payload)
		if err := engine.Ack(ctx, topicName, decodeHandleForTest(t, msg.ReceiptHandle)); err != nil {
			t.Fatalf("Ack(offset=%d): %v", msg.Offset, err)
		}
	}

	// Offsets 0 and 2 delivered with correct payloads; the corrupt offset 1 was
	// never delivered.
	if got := delivered[0]; got != string(payloads[0]) {
		t.Fatalf("offset 0 = %q, want %q", got, payloads[0])
	}
	if got := delivered[2]; got != string(payloads[2]) {
		t.Fatalf("offset 2 = %q, want %q", got, payloads[2])
	}
	if _, ok := delivered[1]; ok {
		t.Fatalf("corrupt offset 1 was delivered (%q), want it skipped", delivered[1])
	}

	// The loss is recorded (exactly one offset skipped), not silent.
	if got := counterTotal(t, reg, "narad_consumer_corrupt_skipped_total"); got != 1 {
		t.Fatalf("corrupt_skipped_total = %v, want 1", got)
	}

	// The advanced frontier is persisted: the committed consumer offset is 2,
	// so a restart resumes past the poison record (never re-attempts it).
	dir := storage.TopicPartitionDir(dataDir, topicName, 0)
	committed, ok, err := storage.ReadConsumerOffset(dir)
	if err != nil || !ok {
		t.Fatalf("ReadConsumerOffset: ok=%v err=%v", ok, err)
	}
	if committed != 2 {
		t.Fatalf("persisted committed offset = %d, want 2", committed)
	}
}

// After skipping a permanently-unreadable frame, the SAME poll must retry
// the partition and deliver the next healthy record. Falling through to the
// next partition instead would return empty and leave a long-poll blocked in
// waitForActivity for the full Wait even though data is deliverable now.
func TestQueueConsumeCorruptSkipDeliversNextRecordInSamePoll(t *testing.T) {
	dataDir := t.TempDir()
	const topicName = "orders"

	payloads := [][]byte{
		[]byte(`{"id":1,"m":"AAAAAAAAAAAA"}`), // this frame will be corrupted
		[]byte(`{"id":2,"m":"BBBBBBBBBBBB"}`),
	}
	corruptMarker := []byte("AAAAAAAAAAAA")

	logs1 := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, nil, nil)
	log1, err := logs1.Get(topicName, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i, p := range payloads {
		if _, err := log1.Append(p); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := log1.Sync(); err != nil { // one Sync => one frame
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if err := log1.AdvanceHighWatermark(int64(len(payloads))); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := logs1.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
	corruptRecordPayloadOnDisk(t, dataDir, topicName, 0, corruptMarker)

	ms := newMessagingFakeMetastore()
	ms.topics[topicName] = topic.Topic{Name: topicName, Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newTestEngineWithDir(t, dataDir, ms, nil, nil)

	msg, found, err := engine.Consume(context.Background(), topicName, ConsumeOpts{Wait: 0})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want the healthy record right after the skipped corrupt frame in the same poll")
	}
	if msg.Offset != 1 || string(msg.Payload) != string(payloads[1]) {
		t.Fatalf("Consume() = offset %d payload %q, want offset 1 payload %q", msg.Offset, msg.Payload, payloads[1])
	}
}

// counterTotal sums all label series of a named counter from the registry.
func counterTotal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
		return sum
	}
	return 0
}

// corruptRecordPayloadOnDisk flips the first byte of marker within the
// partition's segment file, corrupting the frame that contains it.
func corruptRecordPayloadOnDisk(t *testing.T, dataDir, topicName string, partition int, marker []byte) {
	t.Helper()
	dir := storage.TopicPartitionDir(dataDir, topicName, partition)
	segs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(segs) == 0 {
		t.Fatalf("no segment file in %s", dir)
	}
	for _, seg := range segs {
		data, err := os.ReadFile(seg)
		if err != nil {
			t.Fatalf("read segment: %v", err)
		}
		idx := bytes.Index(data, marker)
		if idx < 0 {
			continue
		}
		data[idx] ^= 0xFF
		if err := os.WriteFile(seg, data, 0o644); err != nil {
			t.Fatalf("write segment: %v", err)
		}
		return
	}
	t.Fatalf("marker %q not found in any segment", marker)
}
