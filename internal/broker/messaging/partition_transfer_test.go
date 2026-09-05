package messaging

// Serve-side of partition rebalance, end to end through the Engine API:
// a node exposes an owned partition's segments + durable positions, a
// destination fetches every byte via ReadPartitionSegment, and the
// recovered copy is identical (same offsets, HWM, records). This is the
// copy-round-trips-identically proof, one layer up from the raw storage
// primitives, over the real ownership guard.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestPartitionTransferInfoRequiresLocalOwnership(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)

	_, err := e.PartitionTransferInfo(context.Background(), "orders", 99) // out of range
	if err == nil {
		t.Fatal("out-of-range partition must error")
	}
	_, err = e.PartitionTransferInfo(context.Background(), "missing", 0)
	if err == nil {
		t.Fatal("missing topic must error")
	}
}

func TestPartitionTransferServeAndCopyIsIdentical(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	// Commit several records to partition 0 (advances the HWM).
	const n = 20
	recs := make([]ingress.ProduceRecord, 0, n)
	for i := range n {
		recs = append(recs, ingress.ProduceRecord{
			Topic:           "orders",
			Key:             "k",
			TargetPartition: 0,
			Payload:         []byte{byte('a' + i%26), byte('0' + i%10)},
		})
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, recs); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Serve side: list segments + durable positions.
	info, err := e.PartitionTransferInfo(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("PartitionTransferInfo: %v", err)
	}
	if len(info.Segments) == 0 || info.HighWatermark != n {
		t.Fatalf("info = %d segments, hwm %d; want segments>0, hwm %d", len(info.Segments), info.HighWatermark, n)
	}

	// Destination: fetch every segment byte via the Engine API, install
	// into a fresh dir, copy the HWM, recover, and audit identity.
	dst := t.TempDir()
	for _, seg := range info.Segments {
		var at int64
		first := true
		for at < seg.SizeBytes || first {
			chunk, err := e.ReadPartitionSegment(ctx, "orders", 0, seg.BaseOffset, at, 8)
			if err != nil {
				t.Fatalf("ReadPartitionSegment: %v", err)
			}
			if len(chunk) == 0 {
				break
			}
			if first {
				if err := storage.WriteSegmentFile(dst, seg.BaseOffset, chunk); err != nil {
					t.Fatalf("WriteSegmentFile: %v", err)
				}
				first = false
			} else if err := storage.AppendToSegmentFile(dst, seg.BaseOffset, chunk); err != nil {
				t.Fatalf("AppendToSegmentFile: %v", err)
			}
			at += int64(len(chunk))
		}
	}

	copyLog, err := storage.NewLog(dst, storage.Options{})
	if err != nil {
		t.Fatalf("recover copy: %v", err)
	}
	defer copyLog.Close()
	if copyLog.NextOffset() != info.HighWatermark {
		t.Fatalf("copy NextOffset = %d, want %d", copyLog.NextOffset(), info.HighWatermark)
	}
	for off := range int64(n) {
		if _, _, _, err := copyLog.ReadKeyed(off); err != nil {
			t.Fatalf("copy ReadKeyed(%d): %v", off, err)
		}
	}
}

// PrepareHandoff freezes the partition: after it, commits are rejected
// so no record can land after the destination captured the final tail
// (they reroute to the new owner via the ingress dispatcher). Resume
// restores commits. This is the cutover's no-loss guarantee.
func TestPrepareHandoffFreezesCommits(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	rec := func() []ingress.ProduceRecord {
		return []ingress.ProduceRecord{{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("x")}}
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, rec()); err != nil {
		t.Fatalf("pre-freeze commit: %v", err)
	}

	info, err := e.PrepareHandoff(ctx, "orders", 0, time.Minute)
	if err != nil {
		t.Fatalf("PrepareHandoff: %v", err)
	}
	if info.HighWatermark != 1 {
		t.Fatalf("frozen HWM = %d, want 1", info.HighWatermark)
	}

	// Frozen: a commit must be rejected (would reroute to the new owner).
	if _, err := e.CommitAcceptedProduceBatch(ctx, rec()); err == nil {
		t.Fatal("commit on a frozen partition must be rejected")
	}
	// The HWM did not move — nothing landed after the freeze.
	if info2, _ := e.PartitionTransferInfo(ctx, "orders", 0); info2.HighWatermark != 1 {
		t.Fatalf("HWM advanced to %d during freeze — a record landed", info2.HighWatermark)
	}

	// Resume restores commits.
	e.ResumeProduce("orders", 0)
	if _, err := e.CommitAcceptedProduceBatch(ctx, rec()); err != nil {
		t.Fatalf("post-resume commit: %v", err)
	}
}

// The frontier a handoff reports is the one consumers reached, not the
// last flushed consumer.offset: acks that landed since the last flush
// must not be redelivered by the new owner.
func TestPrepareHandoffReportsInMemoryFrontier(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 30000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	recs := make([]ingress.ProduceRecord, 0, 3)
	for i := range 3 {
		recs = append(recs, ingress.ProduceRecord{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte{byte('a' + i)}})
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, recs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for range 3 {
		msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
		if err != nil || !found {
			t.Fatalf("consume: found=%v err=%v", found, err)
		}
		h, err := consumer.DecodeHandle(msg.ReceiptHandle)
		if err != nil {
			t.Fatalf("decode handle: %v", err)
		}
		if err := e.Ack(ctx, "orders", h); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	info, err := e.PrepareHandoff(ctx, "orders", 0, time.Minute)
	if err != nil {
		t.Fatalf("PrepareHandoff: %v", err)
	}
	if !info.HasCommitted || info.CommittedOffset != 2 {
		t.Fatalf("handoff committed = (%v, %d), want (true, 2): the acked frontier, not the flushed file", info.HasCommitted, info.CommittedOffset)
	}
}

// During a handoff the source hands out no new reservations and waits
// briefly for the leases already out to be acked, so the reported
// frontier includes them; ResumeProduce reopens consume.
func TestPrepareHandoffFreezesConsumeAndDrainsLeases(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 30000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	recs := []ingress.ProduceRecord{
		{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("a")},
		{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("b")},
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, recs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
	if err != nil || !found {
		t.Fatalf("consume: found=%v err=%v", found, err)
	}
	h, _ := consumer.DecodeHandle(msg.ReceiptHandle)

	// The lease is out; its ack arrives while PrepareHandoff is draining.
	acked := make(chan error, 1)
	go func() {
		time.Sleep(60 * time.Millisecond)
		acked <- e.Ack(ctx, "orders", h)
	}()
	info, err := e.PrepareHandoff(ctx, "orders", 0, time.Minute)
	if err != nil {
		t.Fatalf("PrepareHandoff: %v", err)
	}
	if err := <-acked; err != nil {
		t.Fatalf("ack during drain: %v", err)
	}
	if !info.HasCommitted || info.CommittedOffset != 0 {
		t.Fatalf("handoff committed = (%v, %d), want (true, 0): the drain must include the ack that landed during it", info.HasCommitted, info.CommittedOffset)
	}

	// Frozen: the second record is not handed out until the freeze lifts.
	if _, found, err := e.Consume(ctx, "orders", ConsumeOpts{}); err != nil || found {
		t.Fatalf("consume on a frozen partition: found=%v err=%v, want nothing", found, err)
	}
	e.ResumeProduce("orders", 0)
	if _, found, err := e.Consume(ctx, "orders", ConsumeOpts{}); err != nil || !found {
		t.Fatalf("consume after resume: found=%v err=%v, want the second record", found, err)
	}
}

// The fan-out cursor files living in the partition directory travel with
// the transfer info: a new owner that finds none tail-anchors and skips
// the child's backlog (for a delay child, its whole pending window).
func TestPartitionTransferInfoCarriesFanoutCursorSidecars(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	if _, err := e.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("x")}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	dir := storage.TopicPartitionDir(e.logs.DataDir(), "orders", 0)
	if err := storage.WriteFanoutCursorIfPartitionDirExists(dir, "audit-child", storage.FanoutCursor{Epoch: "abc", NextOffset: 10}); err != nil {
		t.Fatalf("write cursor: %v", err)
	}
	if err := storage.WriteFanoutCursorIfPartitionDirExists(dir, "delayed", storage.FanoutCursor{Epoch: "def", NextOffset: 3}); err != nil {
		t.Fatalf("write cursor: %v", err)
	}

	for name, get := range map[string]func() (PartitionTransferInfo, error){
		"list":   func() (PartitionTransferInfo, error) { return e.PartitionTransferInfo(ctx, "orders", 0) },
		"freeze": func() (PartitionTransferInfo, error) { return e.PrepareHandoff(ctx, "orders", 0, time.Minute) },
	} {
		info, err := get()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(info.Sidecars) != 2 {
			t.Fatalf("%s: sidecars = %d, want 2 cursor files", name, len(info.Sidecars))
		}
		// Install them into a fresh dir and read them back through the
		// cursor API: the copy resumes exactly where the source was.
		dst := t.TempDir()
		for _, f := range info.Sidecars {
			if err := storage.InstallFanoutCursorFile(dst, f); err != nil {
				t.Fatalf("%s: install %s: %v", name, f.Name, err)
			}
		}
		cur, ok, err := storage.ReadFanoutCursor(dst, "audit-child")
		if err != nil || !ok || cur.Epoch != "abc" || cur.NextOffset != 10 {
			t.Fatalf("%s: installed cursor = %+v (ok %v, err %v), want epoch abc next 10", name, cur, ok, err)
		}
		e.ResumeProduce("orders", 0)
	}
	// A transfer can never plant an arbitrary file through the sidecar list.
	for _, bad := range []string{"../evil", "hwm", "fanout-.offset", "consumer.offset", "fanout-x/../y.offset"} {
		if err := storage.InstallFanoutCursorFile(t.TempDir(), storage.SidecarFile{Name: bad, Data: []byte(`{}`)}); err == nil {
			t.Fatalf("sidecar %q was installed; must be refused", bad)
		}
	}
}

// The freeze is fenced: PrepareHandoff mints a token, ConfirmHandoff with
// it extends the freeze, and once the freeze lapsed (TTL passed with no
// re-arm) the token is refused forever, so a slow cutover can never flip
// over commits the source accepted after its freeze silently expired.
func TestConfirmHandoffRefusesLapsedFreeze(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	rec := func() []ingress.ProduceRecord {
		return []ingress.ProduceRecord{{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("x")}}
	}

	info, err := e.PrepareHandoff(ctx, "orders", 0, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("PrepareHandoff: %v", err)
	}
	if info.FreezeToken == "" {
		t.Fatal("PrepareHandoff returned no freeze token")
	}
	token := info.FreezeToken
	// A repeat PrepareHandoff is idempotent: same token, extended.
	if again, err := e.PrepareHandoff(ctx, "orders", 0, 40*time.Millisecond); err != nil || again.FreezeToken != token {
		t.Fatalf("repeat PrepareHandoff = token %q err %v, want %q", again.FreezeToken, err, token)
	}
	// Re-arming with the token keeps the freeze alive across its TTL.
	for range 4 {
		time.Sleep(20 * time.Millisecond)
		if _, err := e.ConfirmHandoff(ctx, "orders", 0, 40*time.Millisecond, token); err != nil {
			t.Fatalf("ConfirmHandoff while re-arming: %v", err)
		}
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, rec()); err == nil {
		t.Fatal("commit landed on a partition whose freeze is being re-armed")
	}
	if _, err := e.ConfirmHandoff(ctx, "orders", 0, time.Minute, "not-the-token"); !errors.Is(err, ErrHandoffFreezeLapsed) {
		t.Fatalf("ConfirmHandoff with a foreign token = %v, want ErrHandoffFreezeLapsed", err)
	}
	if !e.handoffFreezeActive("orders", 0, token) {
		t.Fatal("a refused foreign token must not disturb the active freeze")
	}

	// Let the freeze lapse: the source resumes commits (AP), and the old
	// token is dead. A fenced flip on it must be refused; a fresh
	// PrepareHandoff mints a new token instead.
	time.Sleep(60 * time.Millisecond)
	if _, err := e.CommitAcceptedProduceBatch(ctx, rec()); err != nil {
		t.Fatalf("commit after the freeze lapsed: %v (the TTL must auto-resume)", err)
	}
	if _, err := e.ConfirmHandoff(ctx, "orders", 0, time.Minute, token); !errors.Is(err, ErrHandoffFreezeLapsed) {
		t.Fatalf("ConfirmHandoff on a lapsed freeze = %v, want ErrHandoffFreezeLapsed", err)
	}
	if e.isProducePaused("orders", 0) {
		t.Fatal("a refused ConfirmHandoff must not re-freeze the partition")
	}
	fresh, err := e.PrepareHandoff(ctx, "orders", 0, time.Minute)
	if err != nil {
		t.Fatalf("PrepareHandoff after lapse: %v", err)
	}
	if fresh.FreezeToken == token || fresh.FreezeToken == "" {
		t.Fatalf("re-freeze token = %q, want a fresh token (old %q)", fresh.FreezeToken, token)
	}
	if fresh.HighWatermark != 1 {
		t.Fatalf("re-freeze hwm = %d, want 1 (the commit that landed during the lapse)", fresh.HighWatermark)
	}
}

// PrepareHandoff reads the transfer info under the partition's produce
// lock, so a commit that passed the freeze gate and is mid-fsync when the
// freeze lands is included in the reported HWM rather than landing behind
// the copy.
func TestPrepareHandoffWaitsForInFlightCommitUnderProduceLock(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	if _, err := e.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte("x")}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Simulate a commit stalled inside its critical section: hold the
	// produce lock, and only advance the HWM right before releasing it.
	held := make(chan struct{})
	released := make(chan struct{})
	go func() {
		_ = e.logs.WithProduceLock("orders", 0, func(log *storage.Log) error {
			close(held)
			time.Sleep(80 * time.Millisecond)
			off, err := log.Append(storage.EncodeKeyedRecord("k", 1, []byte("late")))
			if err != nil {
				return err
			}
			if err := log.CommitDurable(off, off); err != nil {
				return err
			}
			close(released)
			return nil
		})
	}()
	<-held
	start := time.Now()
	info, err := e.PrepareHandoff(ctx, "orders", 0, time.Minute)
	if err != nil {
		t.Fatalf("PrepareHandoff: %v", err)
	}
	select {
	case <-released:
	default:
		t.Fatal("PrepareHandoff returned while a commit still held the produce lock")
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("PrepareHandoff returned after %v; it must have waited for the produce lock", time.Since(start))
	}
	if info.HighWatermark != 2 {
		t.Fatalf("frozen hwm = %d, want 2 (the in-flight commit must be part of the final tail)", info.HighWatermark)
	}
}
