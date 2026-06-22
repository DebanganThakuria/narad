package replication

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

type streamTestLogs struct {
	log  *storage.Log
	logs map[string]*storage.Log
}

func (l streamTestLogs) Get(topic string, partition int) (*storage.Log, error) {
	if l.logs != nil {
		if log := l.logs[streamTestLogKey(topic, partition)]; log != nil {
			return log, nil
		}
	}
	return l.log, nil
}

func streamTestLogKey(topic string, partition int) string {
	return topic + "/" + strconv.Itoa(partition)
}

func waitForStreamTestHighWatermark(t *testing.T, log *storage.Log, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for log.HighWatermark() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := log.HighWatermark(); got != want {
		t.Fatalf("HighWatermark() = %d, want %d", got, want)
	}
}

func TestStreamClientAppendBatchReplicatesToFollowerLog(t *testing.T) {
	log, err := storage.NewLog(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go serveStreamConn(serverConn, bufio.NewReader(serverConn), streamTestLogs{log: log}, nil)

	client := &streamClient{
		conn:    clientConn,
		reader:  bufio.NewReader(clientConn),
		timeout: time.Second,
		pending: make(map[uint64]chan streamResult),
	}
	go client.readLoop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	next, err := client.appendBatch(ctx, "orders", 0, []Record{
		{Offset: 0, Payload: []byte(`{"id":1}`)},
		{Offset: 1, Payload: []byte(`{"id":2}`)},
	})
	if err != nil {
		t.Fatalf("appendBatch() error = %v", err)
	}
	if next != 2 {
		t.Fatalf("appendBatch() next offset = %d, want 2", next)
	}
	waitForStreamTestHighWatermark(t, log, 2)
	first, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0) error = %v", err)
	}
	if string(first) != `{"id":1}` {
		t.Fatalf("Read(0) = %s", first)
	}
	second, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read(1) error = %v", err)
	}
	if string(second) != `{"id":2}` {
		t.Fatalf("Read(1) = %s", second)
	}
}

func TestStreamClientAppendMultiReplicatesToFollowerLogs(t *testing.T) {
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

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	logs := streamTestLogs{logs: map[string]*storage.Log{
		streamTestLogKey("orders", 0):   orders,
		streamTestLogKey("payments", 1): payments,
	}}
	go serveStreamConn(serverConn, bufio.NewReader(serverConn), logs, nil)

	client := &streamClient{
		conn:    clientConn,
		reader:  bufio.NewReader(clientConn),
		timeout: time.Second,
		pending: make(map[uint64]chan streamResult),
	}
	go client.readLoop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, err := client.appendMulti(ctx, []replicationwire.StreamAppendGroup{
		{
			Topic:      "orders",
			Partition:  0,
			BaseOffset: 0,
			Payloads: [][]byte{
				[]byte(`{"id":1}`),
				[]byte(`{"id":2}`),
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
		t.Fatalf("appendMulti() results = %d, want 2", len(results))
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

func TestStreamHTTPUpgradeAppendBatchReplicatesToFollowerLog(t *testing.T) {
	log, err := storage.NewLog(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeStream(w, r, streamTestLogs{log: log}, nil)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := dialStreamClient(ctx, streamEndpoint(server.URL), "node-a", time.Second)
	if err != nil {
		t.Fatalf("dialStreamClient() error = %v", err)
	}
	t.Cleanup(func() {
		client.closeWithError(errors.New("test done"))
	})

	next, err := client.appendBatch(ctx, "orders", 0, []Record{
		{Offset: 0, Payload: []byte(`{"id":1}`)},
	})
	if err != nil {
		t.Fatalf("appendBatch() error = %v", err)
	}
	if next != 1 {
		t.Fatalf("appendBatch() next offset = %d, want 1", next)
	}
	if got := log.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
}

func TestStreamingClusterReusesBoundedQUICReplicationLanes(t *testing.T) {
	const totalRequests = quicReplicationLanes + 3

	captured := make(chan []replicationwire.StreamAppendGroup, totalRequests)
	acceptedStreams := make(chan struct{}, totalRequests)
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quic tls config: %v", err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", tlsConf, quicConfig())
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	go func() {
		conn, err := listener.Accept(serverCtx)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "test done")
		defer func() { <-serverCtx.Done() }()
		for {
			stream, err := conn.AcceptStream(serverCtx)
			if err != nil {
				return
			}
			acceptedStreams <- struct{}{}
			go serveTestAppendMultiStream(stream, captured)
		}
	}()

	cluster := NewStreamingCluster("node-a", fakeClusterStore{followerAddr: listener.Addr().String()}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := range totalRequests {
		if err := cluster.ReplicateBatch(ctx, "orders", 0, []Record{{Offset: int64(i), Payload: []byte(`{"id":1}`)}}); err != nil {
			t.Fatalf("ReplicateBatch(%d) error = %v", i, err)
		}
	}

	for i := range totalRequests {
		select {
		case groups := <-captured:
			if len(groups) != 1 {
				t.Fatalf("captured request %d groups = %+v, want 1 append group", i, groups)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for append-multi frame")
		}
	}

	accepted := len(acceptedStreams)
	if accepted > quicReplicationLanes {
		t.Fatalf("accepted streams = %d, want at most %d", accepted, quicReplicationLanes)
	}
	if accepted >= totalRequests {
		t.Fatalf("accepted streams = %d, expected bounded lane reuse below request count %d", accepted, totalRequests)
	}
}

func TestStreamingClusterBatchesConcurrentReplicationAcrossPartitions(t *testing.T) {
	captured := make(chan []replicationwire.StreamAppendGroup, 2)
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quic tls config: %v", err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", tlsConf, quicConfig())
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	go func() {
		conn, err := listener.Accept(serverCtx)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "test done")
		defer func() { <-serverCtx.Done() }()
		stream, err := conn.AcceptStream(serverCtx)
		if err != nil {
			return
		}
		serveTestAppendMultiStream(stream, captured)
	}()

	cluster := NewStreamingCluster("node-a", fakeClusterStore{followerAddr: listener.Addr().String()}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errs := make(chan error, 2)
	go func() {
		errs <- cluster.ReplicateBatch(ctx, "orders", 0, []Record{{Offset: 0, Payload: []byte(`{"id":1}`)}})
	}()
	go func() {
		errs <- cluster.ReplicateBatch(ctx, "orders", 1, []Record{{Offset: 0, Payload: []byte(`{"id":2}`)}})
	}()

	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("ReplicateBatch() error = %v", err)
		}
	}

	select {
	case groups := <-captured:
		if len(groups) != 2 {
			t.Fatalf("appendMulti groups = %+v, want 2 groups", groups)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for append-multi frame")
	}
	select {
	case groups := <-captured:
		t.Fatalf("unexpected second appendMulti frame: %+v", groups)
	default:
	}
}

func serveTestAppendMultiStream(stream *quic.Stream, captured chan<- []replicationwire.StreamAppendGroup) {
	for {
		frame, err := replicationwire.ReadStreamFrame(stream, replicationwire.MaxStreamFramePayloadBytes)
		if err != nil {
			_ = stream.Close()
			return
		}
		if frame.Type != replicationwire.StreamFrameAppendMulti {
			captured <- nil
			_ = stream.Close()
			return
		}
		multi, err := replicationwire.DecodeStreamAppendMulti(frame.Payload)
		if err != nil {
			captured <- nil
			_ = stream.Close()
			return
		}
		results := make([]replicationwire.StreamAppendResult, len(multi.Groups))
		for i, group := range multi.Groups {
			results[i] = replicationwire.StreamAppendResult{
				NextOffset:        group.BaseOffset + int64(len(group.Payloads)),
				ReplicaNextOffset: -1,
			}
		}
		payload, err := replicationwire.EncodeStreamAppendResults(results)
		if err != nil {
			captured <- nil
			_ = stream.Close()
			return
		}
		if err := replicationwire.WriteStreamFrame(stream, replicationwire.StreamFrame{
			Type:      replicationwire.StreamFrameMultiAck,
			RequestID: frame.RequestID,
			Payload:   payload,
		}); err != nil {
			captured <- nil
			_ = stream.Close()
			return
		}
		captured <- multi.Groups
	}
}

func TestStreamingClusterCanOpenMultipleQUICReplicationLanes(t *testing.T) {
	captured := make(chan []replicationwire.StreamAppendGroup, 2)
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quic tls config: %v", err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", tlsConf, quicConfig())
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverCtx, stopServer := context.WithCancel(context.Background())
	t.Cleanup(stopServer)
	go func() {
		conn, err := listener.Accept(serverCtx)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "test done")
		defer func() { <-serverCtx.Done() }()
		for range 2 {
			stream, err := conn.AcceptStream(serverCtx)
			if err != nil {
				return
			}
			go serveTestAppendMultiStream(stream, captured)
		}
	}()

	cluster := NewStreamingCluster("node-a", fakeClusterStore{followerAddr: listener.Addr().String()}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := cluster.ReplicateBatch(ctx, "orders", 0, []Record{{Offset: 0, Payload: []byte(`{"id":1}`)}}); err != nil {
		t.Fatalf("first ReplicateBatch() error = %v", err)
	}
	if err := cluster.ReplicateBatch(ctx, "orders", 0, []Record{{Offset: 1, Payload: []byte(`{"id":2}`)}}); err != nil {
		t.Fatalf("second ReplicateBatch() error = %v", err)
	}

	for i := range 2 {
		select {
		case groups := <-captured:
			if len(groups) != 1 {
				t.Fatalf("captured stream %d groups = %+v, want 1 append group", i, groups)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for append-multi frame")
		}
	}
}

func TestAppendReplicaBatchAcceptsDuplicatePrefix(t *testing.T) {
	log, err := storage.NewLog(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if _, _, err := log.AppendBatch([][]byte{[]byte(`{"id":1}`)}); err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}

	next, err := appendReplicaBatch(log, mustStreamAppendBatch(t, "orders", 0, 0, [][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
	}))
	if err != nil {
		t.Fatalf("appendReplicaBatch() error = %v", err)
	}
	if next != 2 {
		t.Fatalf("appendReplicaBatch() next = %d, want 2", next)
	}
	if got := log.NextOffset(); got != 2 {
		t.Fatalf("NextOffset() = %d, want 2", got)
	}
}

func TestReplicaAppendCoordinatorStagesOutOfOrderBatch(t *testing.T) {
	log, err := storage.NewLog(t.TempDir(), storage.DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	coordinator := newReplicaAppendCoordinator(streamTestLogs{log: log}, nil)
	next, err := coordinator.append(mustStreamAppendBatch(t, "orders", 0, 1, [][]byte{
		[]byte(`{"id":2}`),
	}))
	if err != nil {
		t.Fatalf("append(offset=1) error = %v", err)
	}
	if next != 2 {
		t.Fatalf("append(offset=1) next = %d, want 2", next)
	}
	if got := log.NextOffset(); got != 0 {
		t.Fatalf("NextOffset() after staged append = %d, want 0", got)
	}
	if got := log.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() after staged append = %d, want 0", got)
	}

	next, err = coordinator.append(mustStreamAppendBatch(t, "orders", 0, 0, [][]byte{
		[]byte(`{"id":1}`),
	}))
	if err != nil {
		t.Fatalf("append(offset=0) error = %v", err)
	}
	if next != 1 {
		t.Fatalf("append(offset=0) next = %d, want 1", next)
	}
	deadline := time.Now().Add(time.Second)
	for log.NextOffset() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := log.NextOffset(); got != 2 {
		t.Fatalf("NextOffset() after background drain = %d, want 2", got)
	}
	if got := log.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() after background drain = %d, want 2", got)
	}
	first, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0) error = %v", err)
	}
	if string(first) != `{"id":1}` {
		t.Fatalf("Read(0) = %s", first)
	}
	second, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read(1) error = %v", err)
	}
	if string(second) != `{"id":2}` {
		t.Fatalf("Read(1) = %s", second)
	}
}

func mustStreamAppendBatch(t *testing.T, topic string, partition int, baseOffset int64, payloads [][]byte) replicationwire.StreamAppendBatch {
	t.Helper()
	return replicationwire.StreamAppendBatch{
		Topic:      topic,
		Partition:  partition,
		BaseOffset: baseOffset,
		Payloads:   payloads,
	}
}
