package cluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakePeerClient struct {
	produceFn             func(context.Context, string, nodewire.ProduceRequest) (nodewire.Response, error)
	commitProduceFn       func(context.Context, string, nodewire.CommitProduceRequest) (nodewire.Response, error)
	commitProduceBatchFn  func(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error)
	consumeFn             func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error)
	ackFn                 func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	extendAckFn           func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	nackFn                func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	createTopicFn         func(context.Context, string, []byte) (nodewire.Response, error)
	alterTopicFn          func(context.Context, string, string, []byte) (nodewire.Response, error)
	deleteTopicFn         func(context.Context, string, string) (nodewire.Response, error)
	purgeTopicFn          func(context.Context, string, string) (nodewire.Response, error)
	topicPartitionStatsFn func(context.Context, string, string, int) (topic.PartitionStats, error)
	registerMemberFn      func(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
	createUserFn          func(context.Context, string, []byte) (nodewire.Response, error)
	updateUserFn          func(context.Context, string, string, []byte) (nodewire.Response, error)
	deleteUserFn          func(context.Context, string, string) (nodewire.Response, error)
	attachChildFn         func(context.Context, string, string, string) (nodewire.Response, error)
	detachChildFn         func(context.Context, string, string, string) (nodewire.Response, error)
	fanoutCursorsFn       func(context.Context, string, string) ([]topic.FanoutCursorStat, error)
}

func (f fakePeerClient) AttachChild(ctx context.Context, addr, parent, child string) (nodewire.Response, error) {
	if f.attachChildFn != nil {
		return f.attachChildFn(ctx, addr, parent, child)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) DetachChild(ctx context.Context, addr, parent, child string) (nodewire.Response, error) {
	if f.detachChildFn != nil {
		return f.detachChildFn(ctx, addr, parent, child)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) FanoutCursors(ctx context.Context, addr, parent string) ([]topic.FanoutCursorStat, error) {
	if f.fanoutCursorsFn != nil {
		return f.fanoutCursorsFn(ctx, addr, parent)
	}
	return nil, context.DeadlineExceeded
}

func (f fakePeerClient) CreateUser(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	if f.createUserFn != nil {
		return f.createUserFn(ctx, addr, body)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) UpdateUser(ctx context.Context, addr, username string, body []byte) (nodewire.Response, error) {
	if f.updateUserFn != nil {
		return f.updateUserFn(ctx, addr, username, body)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) DeleteUser(ctx context.Context, addr, username string) (nodewire.Response, error) {
	if f.deleteUserFn != nil {
		return f.deleteUserFn(ctx, addr, username)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) Produce(ctx context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
	if f.produceFn != nil {
		return f.produceFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) CommitProduce(ctx context.Context, addr string, req nodewire.CommitProduceRequest) (nodewire.Response, error) {
	if f.commitProduceFn != nil {
		return f.commitProduceFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) CommitProduceBatch(ctx context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
	if f.commitProduceBatchFn != nil {
		return f.commitProduceBatchFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	if f.consumeFn != nil {
		return f.consumeFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) Ack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	if f.ackFn != nil {
		return f.ackFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) CreateTopic(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	if f.createTopicFn != nil {
		return f.createTopicFn(ctx, addr, body)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) AlterTopic(ctx context.Context, addr, topicName string, body []byte) (nodewire.Response, error) {
	if f.alterTopicFn != nil {
		return f.alterTopicFn(ctx, addr, topicName, body)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) DeleteTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	if f.deleteTopicFn != nil {
		return f.deleteTopicFn(ctx, addr, topicName)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) PurgeTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	if f.purgeTopicFn != nil {
		return f.purgeTopicFn(ctx, addr, topicName)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) TopicPartitionStats(ctx context.Context, addr, topicName string, partition int) (topic.PartitionStats, error) {
	if f.topicPartitionStatsFn != nil {
		return f.topicPartitionStatsFn(ctx, addr, topicName, partition)
	}
	return topic.PartitionStats{}, context.DeadlineExceeded
}

func (f fakePeerClient) RegisterMember(ctx context.Context, addr string, req nodewire.MemberRequest) (nodewire.Response, error) {
	if f.registerMemberFn != nil {
		return f.registerMemberFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func newTestStore(t *testing.T) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   "node-self",
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("timed out waiting for leader")
	return nil
}

func newTestStoreCluster(t *testing.T, ids ...string) map[string]*metastore.Store {
	t.Helper()
	if len(ids) == 0 {
		t.Fatal("newTestStoreCluster requires at least one node")
	}

	addrs := make(map[string]string, len(ids))
	for _, id := range ids {
		addrs[id] = freeTCPAddr(t)
	}

	baseDir := t.TempDir()
	stores := make(map[string]*metastore.Store, len(ids))
	for _, id := range ids {
		peers := make([]metastore.Peer, 0, len(ids)-1)
		for _, peerID := range ids {
			if peerID == id {
				continue
			}
			peers = append(peers, metastore.Peer{ID: peerID, Addr: addrs[peerID]})
		}
		store, err := metastore.New(metastore.Config{
			NodeID:        id,
			DataDir:       filepath.Join(baseDir, fmt.Sprintf("metastore-%s", id)),
			BindAddr:      addrs[id],
			AdvertiseAddr: addrs[id],
			Peers:         peers,
		})
		if err != nil {
			t.Fatalf("metastore.New(%s) error = %v", id, err)
		}
		stores[id] = store
		t.Cleanup(func() { _ = store.Close() })
	}
	return stores
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return addr
}

func waitForClusterLeader(t *testing.T, stores map[string]*metastore.Store) (string, *metastore.Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, store := range stores {
			if store.IsLeader() {
				return id, store
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for cluster leader")
	return "", nil
}

func waitForMember(t *testing.T, store *metastore.Store, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.GetMember(id); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for member %q", id)
}

func seedTopicRouteState(t *testing.T, store *metastore.Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
}

func unreachableAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return addr
}

type fixedPartitionManager struct {
	picked int
}

func (f fixedPartitionManager) Pick(string, string, int) int { return f.picked }

func (f fakePeerClient) ExtendAck(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	if f.extendAckFn != nil {
		return f.extendAckFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) Nack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	if f.nackFn != nil {
		return f.nackFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}
