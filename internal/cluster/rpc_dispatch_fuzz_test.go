package cluster

// FuzzRPCDispatch drives the node-RPC dispatcher with arbitrary request
// payloads. The dispatcher runs on a goroutine with no recover, so a
// panic anywhere between the wire decoder and the handler is a node
// crash for the price of one authenticated frame. The server is wired
// to a stub broker (every operation fails cleanly) and a real single-
// node metastore, so the control-plane handlers run their own body
// decoders and store calls. Invariants: no panic, every reply encodes,
// a payload the wire decoder rejects is answered 400, and no request
// runs past a bound.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	obsmetrics "github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

var errStub = errors.New("stub broker")

// stubBroker fails every operation with a clean error, so the fuzzer
// exercises the RPC handlers' own parsing and error mapping rather than
// the broker.
type stubBroker struct{}

func (stubBroker) CreateTopic(context.Context, brokertopics.CreateOpts) (topic.Topic, error) {
	return topic.Topic{}, errStub
}

func (stubBroker) IncreaseTopicPartitions(context.Context, string, int) (topic.Topic, error) {
	return topic.Topic{}, errStub
}

func (stubBroker) UpdateTopicRetention(context.Context, string, int64) (topic.Topic, error) {
	return topic.Topic{}, errStub
}

func (stubBroker) UpdateTopicCaps(context.Context, string, int64, int64) (topic.Topic, error) {
	return topic.Topic{}, errStub
}

func (stubBroker) UpdateTopicSchema(context.Context, string, []byte, int) (topic.Topic, error) {
	return topic.Topic{}, errStub
}

func (stubBroker) TopicSchemaHistory(context.Context, string) (topic.SchemaHistory, error) {
	return topic.SchemaHistory{}, errStub
}
func (stubBroker) DeleteTopic(context.Context, string) error        { return errs.ErrTopicNotFound }
func (stubBroker) PurgeTopic(context.Context, string, string) error { return errs.ErrTopicNotFound }
func (stubBroker) GetTopic(context.Context, string) (topic.Topic, error) {
	return topic.Topic{}, errs.ErrTopicNotFound
}

func (stubBroker) GetTopicDetails(context.Context, string) (topic.Details, error) {
	return topic.Details{}, errs.ErrTopicNotFound
}

func (stubBroker) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}
func (stubBroker) AttachChild(context.Context, string, string, int64) error { return errStub }
func (stubBroker) DetachChild(context.Context, string, string) error        { return errStub }
func (stubBroker) ReadFanoutSlab(context.Context, string, int, topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	return topic.FanoutSlab{}, errStub
}

func (stubBroker) PartitionTransferInfo(context.Context, string, int) (brokermsg.PartitionTransferInfo, error) {
	return brokermsg.PartitionTransferInfo{}, errStub
}

func (stubBroker) ReadPartitionSegment(context.Context, string, int, int64, int64, int64) ([]byte, error) {
	return nil, errStub
}
func (stubBroker) PauseProduceForHandoff(string, int, time.Duration) {}
func (stubBroker) ResumeProduce(string, int)                         {}
func (stubBroker) PrepareHandoff(context.Context, string, int, time.Duration) (brokermsg.PartitionTransferInfo, error) {
	return brokermsg.PartitionTransferInfo{}, errStub
}
func (stubBroker) ReclaimMovedPartition(context.Context, string, int) error { return errStub }
func (stubBroker) FanoutCursorStats(context.Context, string) ([]topic.FanoutCursorStat, error) {
	return nil, errStub
}

func (stubBroker) Produce(context.Context, string, string, []byte, ...int) (int64, int, error) {
	return 0, 0, errStub
}

func (stubBroker) AcceptProduce(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
	return ingress.AcceptedProduce{}, errStub
}

func (stubBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	return 0, errStub
}

func (stubBroker) CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error) {
	return nil, errStub
}

func (stubBroker) Consume(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return topic.Message{}, false, errs.ErrTopicNotFound
}

func (stubBroker) ConsumeProbe(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, *brokermsg.ConsumeWaiter, error) {
	return topic.Message{}, false, nil, errs.ErrTopicNotFound
}

func (stubBroker) ConsumeWait(context.Context, *brokermsg.ConsumeWaiter, time.Duration) (topic.Message, bool, error) {
	return topic.Message{}, false, errs.ErrTopicNotFound
}
func (stubBroker) Ack(context.Context, string, consumer.Handle) error { return errs.ErrHandleStale }
func (stubBroker) ExtendAck(context.Context, string, consumer.Handle) error {
	return errs.ErrHandleStale
}
func (stubBroker) Nack(context.Context, string, consumer.Handle) error { return errs.ErrHandleStale }
func (stubBroker) Snapshot(context.Context) ([]obsmetrics.TopicSnapshot, error) {
	return nil, nil
}
func (stubBroker) Ready(context.Context) error { return nil }
func (stubBroker) Close() error                { return nil }

// newFuzzStore boots a single-node metastore for the fuzz process.
func newFuzzStore(f *testing.F) *metastore.Store {
	f.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   "fuzz-0",
		DataDir:  filepath.Join(f.TempDir(), "metastore"),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		f.Fatalf("metastore: %v", err)
	}
	f.Cleanup(func() { store.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !store.IsLeader() {
		if time.Now().After(deadline) {
			f.Fatal("metastore never became leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return store
}

func FuzzRPCDispatch(f *testing.F) {
	must := func(b []byte, err error) []byte {
		if err != nil {
			f.Fatalf("seed: %v", err)
		}
		return b
	}
	body := []byte(`{"name":"orders","partitions":4,"schema":{"type":"object"}}`)
	seeds := [][]byte{
		{},
		{0xff},
		must(nodewire.EncodeProduceRequest(nodewire.ProduceRequest{Topic: "orders", Key: "k", Payload: []byte("p")})),
		must(nodewire.EncodeCommitProduceRequest(nodewire.CommitProduceRequest{Topic: "orders", Key: "k", Payload: []byte("p"), CreatedAtUnixMs: 1})),
		must(nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{{Topic: "orders", Payload: []byte("p")}}})),
		must(nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", WaitNanos: 1e9})),
		must(nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", Partition: 1, HasPartition: true, Offset: 3, HasOffset: true})),
		must(nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2})),
		must(nodewire.EncodeExtendAckRequest(nodewire.AckRequest{Topic: "orders", Offset: 1, Nonce: 2})),
		must(nodewire.EncodeNackRequest(nodewire.AckRequest{Topic: "orders", Offset: 1, Nonce: 2})),
		must(nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: body})),
		must(nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: []byte(`{"name":"orders","unknown":1}`)})),
		must(nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: []byte(`{"partitions":8,"retention_ms":1000}`)})),
		must(nodewire.EncodeTopicNameRequest(nodewire.OpDeleteTopic, nodewire.TopicNameRequest{Topic: "orders"})),
		must(nodewire.EncodeTopicNameRequest(nodewire.OpPurgeTopic, nodewire.TopicNameRequest{Topic: "orders", ID: "01H"})),
		must(nodewire.EncodeTopicNameRequest(nodewire.OpGetTopic, nodewire.TopicNameRequest{Topic: "orders"})),
		must(nodewire.EncodeTopicNameRequest(nodewire.OpFanoutCursors, nodewire.TopicNameRequest{Topic: "orders"})),
		must(nodewire.EncodeTopicPartitionStatsRequest(nodewire.TopicPartitionStatsRequest{Topic: "orders", Partition: 1})),
		must(nodewire.EncodeUserRequest(nodewire.OpCreateUser, nodewire.UserRequest{Username: "alice", Body: []byte(`{"username":"alice","password_hash":"eA==","grants":[{"action":"admin"}]}`)})),
		must(nodewire.EncodeUserRequest(nodewire.OpUpdateUser, nodewire.UserRequest{Username: "alice", Body: []byte(`{"username":"alice"}`)})),
		must(nodewire.EncodeUserRequest(nodewire.OpDeleteUser, nodewire.UserRequest{Username: "alice"})),
		must(nodewire.EncodeMemberRequest(nodewire.MemberRequest{ID: "fuzz-1", Addr: "127.0.0.1:1", ClusterAddr: "127.0.0.1:2", Status: "alive", LastHeartbeat: 1})),
		must(nodewire.EncodeChildLinkRequest(nodewire.OpAttachChild, nodewire.ChildLinkRequest{Parent: "orders", Child: "audit", DelayMs: 10})),
		must(nodewire.EncodeChildLinkRequest(nodewire.OpDetachChild, nodewire.ChildLinkRequest{Parent: "orders", Child: "audit"})),
		must(nodewire.EncodePartitionSegmentsRequest(nodewire.PartitionSegmentsRequest{Topic: "orders", Partition: 1})),
		must(nodewire.EncodeFetchSegmentChunkRequest(nodewire.FetchSegmentChunkRequest{Topic: "orders", Partition: 1, BaseOffset: 0, At: 0, Length: 4096})),
		must(nodewire.EncodePrepareHandoffRequest(nodewire.PrepareHandoffRequest{Topic: "orders", Partition: 1, FreezeTTLNanos: 1e9})),
		must(nodewire.EncodeDecommissionRequest(nodewire.DecommissionRequest{ID: "fuzz-1"})),
		must(nodewire.EncodeDecommissionRequest(nodewire.DecommissionRequest{ID: "fuzz-1", Cancel: true})),
		must(nodewire.EncodeCompleteMoveRequest(nodewire.CompleteMoveRequest{Topic: "orders", Partition: 1, ExpectedOwner: "fuzz-0", TargetID: "fuzz-1"})),
		must(nodewire.EncodeAbortMoveRequest(nodewire.AbortMoveRequest{Topic: "orders", Partition: 1, ExpectedTarget: "fuzz-1"})),
		must(nodewire.EncodeGetAssignmentRequest(nodewire.GetAssignmentRequest{Topic: "orders", Partition: 1})),
		nodewire.EncodeAppliedIndexRequest(),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	store := newFuzzStore(f)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewRPCServer(stubBroker{}, store, logger)
	server.SetMaxConsumeWait(50 * time.Millisecond)

	f.Fuzz(func(t *testing.T, payload []byte) {
		// A join would add the payload's node to the Raft voter set and
		// take the single-node store's quorum with it; every other op is
		// safe against a stub broker and a scratch store.
		if op, err := nodewire.OperationOf(payload); err == nil && op == nodewire.OpJoinCluster {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		done := make(chan nodewire.Response, 1)
		go func() {
			done <- server.dispatch(ctx, requestKey{stream: 1, request: 1}, payload)
		}()
		var res nodewire.Response
		select {
		case res = <-done:
		case <-time.After(15 * time.Second):
			t.Fatalf("dispatch hung on payload %x", payload)
		}
		if _, err := nodewire.EncodeResponse(res); err != nil {
			t.Fatalf("reply does not encode: %v (status %d)", err, res.Status)
		}
		if res.Status < 100 || res.Status > 599 {
			t.Fatalf("reply status %d out of range", res.Status)
		}
		if rejected := wireRejects(payload); rejected && res.Status != http.StatusBadRequest {
			t.Fatalf("payload the wire decoder rejects was answered %d, want 400: %x (%s)", res.Status, payload, res.Body)
		}
	})
}

// wireRejects reports whether the node wire decoder for the payload's
// operation rejects it (so the dispatcher must answer 400). Operations
// the dispatcher does not know are its own 400.
func wireRejects(payload []byte) bool {
	op, err := nodewire.OperationOf(payload)
	if err != nil {
		return true
	}
	switch op {
	case nodewire.OpProduce:
		_, err = nodewire.DecodeProduceRequest(payload)
	case nodewire.OpCommitProduce:
		_, err = nodewire.DecodeCommitProduceRequest(payload)
	case nodewire.OpCommitProduceBatch:
		_, err = nodewire.DecodeCommitProduceBatchRequest(payload)
	case nodewire.OpConsume:
		_, err = nodewire.DecodeConsumeRequest(payload)
	case nodewire.OpAck:
		_, err = nodewire.DecodeAckRequest(payload)
	case nodewire.OpExtendAck:
		_, err = nodewire.DecodeExtendAckRequest(payload)
	case nodewire.OpNack:
		_, err = nodewire.DecodeNackRequest(payload)
	case nodewire.OpCreateTopic, nodewire.OpAlterTopic:
		_, err = nodewire.DecodeTopicBodyRequest(payload, op)
	case nodewire.OpDeleteTopic, nodewire.OpPurgeTopic, nodewire.OpGetTopic, nodewire.OpFanoutCursors:
		_, err = nodewire.DecodeTopicNameRequest(payload, op)
	case nodewire.OpTopicPartitionStats:
		_, err = nodewire.DecodeTopicPartitionStatsRequest(payload)
	case nodewire.OpCreateUser, nodewire.OpUpdateUser, nodewire.OpDeleteUser:
		_, err = nodewire.DecodeUserRequest(payload, op)
	case nodewire.OpRegisterMember:
		_, err = nodewire.DecodeMemberRequest(payload)
	case nodewire.OpAttachChild, nodewire.OpDetachChild:
		_, err = nodewire.DecodeChildLinkRequest(payload, op)
	case nodewire.OpListPartitionSegments:
		_, err = nodewire.DecodePartitionSegmentsRequest(payload)
	case nodewire.OpFetchSegmentChunk:
		_, err = nodewire.DecodeFetchSegmentChunkRequest(payload)
	case nodewire.OpPrepareHandoff:
		_, err = nodewire.DecodePrepareHandoffRequest(payload)
	case nodewire.OpDecommissionMember:
		_, err = nodewire.DecodeDecommissionRequest(payload)
	case nodewire.OpCompleteMove:
		_, err = nodewire.DecodeCompleteMoveRequest(payload)
	case nodewire.OpAbortMove:
		_, err = nodewire.DecodeAbortMoveRequest(payload)
	case nodewire.OpGetAssignment:
		_, err = nodewire.DecodeGetAssignmentRequest(payload)
	case nodewire.OpAppliedIndex:
		err = nodewire.DecodeAppliedIndexRequest(payload)
	default:
		return true
	}
	return err != nil
}
