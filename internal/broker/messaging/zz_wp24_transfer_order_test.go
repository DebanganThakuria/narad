//go:build darwin || linux

package messaging

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestZZWP24ListingFrontierStaysBelowBoundary pins the read order of a
// transfer listing on an unfrozen source. consumer.offset is turned
// into a FIFO so the listing parks inside its frontier read, and while
// it is parked one record is committed, delivered and acked. The
// listing must still ship a frontier below its boundary, and segments
// that hold that boundary. Read boundary-first (as before), it shipped
// boundary 5 with frontier 5, and a force-promote from it started the
// new owner past its log end.
func TestZZWP24ListingFrontierStaysBelowBoundary(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 30000}
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	// No commit callback: nothing but the listing opens consumer.offset.
	tracker := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, tracker, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	ctx := context.Background()

	produce := func(label string) (int64, error) {
		offs, err := e.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte(label)}})
		if err != nil || len(offs) != 1 {
			return 0, fmt.Errorf("produce %s: %v %v", label, offs, err)
		}
		return offs[0], nil
	}
	consumeAck := func() (int64, error) {
		msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{Wait: time.Second})
		if err != nil || !found {
			return 0, fmt.Errorf("consume: found=%v err=%v", found, err)
		}
		h, err := consumer.DecodeHandle(msg.ReceiptHandle)
		if err != nil {
			return 0, err
		}
		return msg.Offset, e.Ack(ctx, "orders", h)
	}
	for i := range 5 {
		if _, err := produce(fmt.Sprintf("pre-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		if _, err := consumeAck(); err != nil {
			t.Fatal(err)
		}
	}

	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	fifo := filepath.Join(dir, "consumer.offset")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(fifo) })
	interleaved := make(chan error, 1)
	go func() {
		// Opens once the listing has opened consumer.offset for reading.
		f, err := os.OpenFile(fifo, os.O_WRONLY, 0)
		if err != nil {
			interleaved <- err
			return
		}
		defer func() { _ = f.Close() }()
		off, err := produce("during-listing")
		if err == nil {
			var got int64
			if got, err = consumeAck(); err == nil && got != off {
				err = fmt.Errorf("delivered %d, want the record produced during the listing (%d)", got, off)
			}
		}
		// What the offset committer last flushed: offset 4.
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], 4)
		if _, werr := f.Write(buf[:]); err == nil {
			err = werr
		}
		interleaved <- err
	}()

	info, err := e.PartitionTransferInfo(ctx, "orders", 0)
	if ierr := <-interleaved; ierr != nil {
		t.Fatalf("interleaving: %v", ierr)
	}
	if err != nil {
		t.Fatalf("PartitionTransferInfo: %v", err)
	}
	if !info.HasCommitted || info.CommittedOffset >= info.HighWatermark {
		t.Fatalf("listing frontier %d (has %v) is not below its boundary %d", info.CommittedOffset, info.HasCommitted, info.HighWatermark)
	}
	for _, off := range info.AckedAhead {
		if off >= info.HighWatermark {
			t.Fatalf("listing acked-ahead offset %d is not below its boundary %d", off, info.HighWatermark)
		}
	}
	// The listed bytes, copied as a destination copies them, hold the
	// boundary.
	staged := t.TempDir()
	for _, seg := range info.Segments {
		data, err := storage.ReadSegmentRange(dir, seg.BaseOffset, 0, seg.SizeBytes)
		if err != nil {
			t.Fatalf("read segment %d: %v", seg.BaseOffset, err)
		}
		if err := storage.WriteSegmentFile(staged, seg.BaseOffset, data); err != nil {
			t.Fatalf("stage segment %d: %v", seg.BaseOffset, err)
		}
	}
	log, err := storage.NewLog(staged, storage.Options{})
	if err != nil {
		t.Fatalf("recover listed bytes: %v", err)
	}
	defer func() { _ = log.Close() }()
	if next := log.NextOffset(); next < info.HighWatermark {
		t.Fatalf("listed segments end at %d, below the listed boundary %d", next, info.HighWatermark)
	}
}
