package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

func TestQUICAppendMultiReplicatesToFollowerLogs(t *testing.T) {
	orders, err := storage.NewLog(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog(orders) error = %v", err)
	}
	t.Cleanup(func() {
		if err := orders.Close(); err != nil {
			t.Fatalf("Close(orders) error = %v", err)
		}
	})
	payments, err := storage.NewLog(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog(payments) error = %v", err)
	}
	t.Cleanup(func() {
		if err := payments.Close(); err != nil {
			t.Fatalf("Close(payments) error = %v", err)
		}
	})

	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket() error = %v", err)
	}
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quicServerTLSConfig() error = %v", err)
	}
	listener, err := quic.Listen(packetConn, tlsConf, quicConfig())
	if err != nil {
		t.Fatalf("quic.Listen() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs := streamTestLogs{logs: map[string]*storage.Log{
		streamTestLogKey("orders", 0):   orders,
		streamTestLogKey("payments", 1): payments,
	}}
	done := make(chan error, 1)
	go func() {
		done <- serveQUICListener(ctx, listener, logs, nil)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("serveQUICListener() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("serveQUICListener() did not stop")
		}
	})

	client := newQUICClientPool(time.Second)
	reqCtx, reqCancel := context.WithTimeout(context.Background(), time.Second)
	defer reqCancel()
	results, err := client.appendMulti(reqCtx, listener.Addr().String(), []replicationwire.StreamAppendGroup{
		{
			Topic:      "orders",
			Partition:  0,
			BaseOffset: 0,
			Payloads: [][]byte{
				[]byte(`{"id":1}`),
				[]byte(`not-json-but-replica-should-not-care`),
			},
		},
		{
			Topic:      "payments",
			Partition:  1,
			BaseOffset: 0,
			Payloads: [][]byte{
				[]byte(`{"id":3}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("appendMulti() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[0].NextOffset != 2 || results[0].Message != "" {
		t.Fatalf("orders result = %+v", results[0])
	}
	if results[1].NextOffset != 1 || results[1].Message != "" {
		t.Fatalf("payments result = %+v", results[1])
	}
	waitForStreamTestHighWatermark(t, orders, 2)
	waitForStreamTestHighWatermark(t, payments, 1)
}
