package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func TestRPCServerWaitTopicDeletedLocally(t *testing.T) {
	store := newTestStore(t)
	s := &RPCServer{store: store}

	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Topic still present -> must NOT report deleted (returns false after
	// the timeout). This is the guard that stops a purge from running
	// while the local replica still shows the topic live.
	if s.waitTopicDeletedLocally("orders", 200*time.Millisecond) {
		t.Fatal("waitTopicDeletedLocally(existing) = true, want false")
	}

	if err := store.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	// Once the local replica reflects the delete, returns true promptly.
	if !s.waitTopicDeletedLocally("orders", 2*time.Second) {
		t.Fatal("waitTopicDeletedLocally(deleted) = false, want true")
	}
}

// deleteOnlyBroker embeds broker.Broker so only DeleteTopic is real; any
// other method would panic on a nil interface, which is fine because the
// delete RPC handler under test calls nothing else.
type deleteOnlyBroker struct {
	broker.Broker
	deleteErr error
	deleted   []string
}

func (b *deleteOnlyBroker) DeleteTopic(_ context.Context, name string) error {
	b.deleted = append(b.deleted, name)
	return b.deleteErr
}

type recordingBroadcaster struct {
	mu     sync.Mutex
	topics []string
	err    error
}

func (b *recordingBroadcaster) BroadcastDeleteTopic(_ context.Context, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics = append(b.topics, topic)
	return b.err
}

func (b *recordingBroadcaster) seen() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.topics...)
}

func encodeDeleteReq(t *testing.T, name string) []byte {
	t.Helper()
	payload, err := nodewire.EncodeTopicNameRequest(nodewire.OpDeleteTopic, nodewire.TopicNameRequest{Topic: name})
	if err != nil {
		t.Fatalf("EncodeTopicNameRequest: %v", err)
	}
	return payload
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A delete forwarded to the leader over RPC must fan the purge out to the
// other members — otherwise the partition owners keep their directories.
func TestRPCServerForwardedDeleteFansOutPurge(t *testing.T) {
	br := &deleteOnlyBroker{}
	bc := &recordingBroadcaster{}
	s := &RPCServer{broker: br, logger: discardLogger(), broadcaster: bc}

	res := s.handleDeleteTopic(encodeDeleteReq(t, "orders"))
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Status)
	}
	if got := bc.seen(); len(got) != 1 || got[0] != "orders" {
		t.Fatalf("broadcast = %v, want [orders]", got)
	}
}

// A fan-out failure must NOT fail the delete: the metastore record is
// already gone and the startup sweep reclaims any missed member.
func TestRPCServerForwardedDeleteSucceedsDespiteBroadcastError(t *testing.T) {
	br := &deleteOnlyBroker{}
	bc := &recordingBroadcaster{err: errors.New("peer unreachable")}
	s := &RPCServer{broker: br, logger: discardLogger(), broadcaster: bc}

	res := s.handleDeleteTopic(encodeDeleteReq(t, "orders"))
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 despite broadcast error", res.Status)
	}
	if got := bc.seen(); len(got) != 1 {
		t.Fatalf("broadcast attempts = %v, want exactly one", got)
	}
}

// If the metastore delete itself fails, nothing is purged anywhere.
func TestRPCServerForwardedDeleteDoesNotBroadcastOnDeleteError(t *testing.T) {
	br := &deleteOnlyBroker{deleteErr: errs.ErrNotFound}
	bc := &recordingBroadcaster{}
	s := &RPCServer{broker: br, logger: discardLogger(), broadcaster: bc}

	res := s.handleDeleteTopic(encodeDeleteReq(t, "ghost"))
	if res.Status == http.StatusNoContent {
		t.Fatal("status = 204, want an error status when DeleteTopic fails")
	}
	if got := bc.seen(); len(got) != 0 {
		t.Fatalf("broadcast = %v, want none (delete failed)", got)
	}
}

// A nil broadcaster (not wired) must not panic — the delete still succeeds.
func TestRPCServerForwardedDeleteNilBroadcaster(t *testing.T) {
	s := &RPCServer{broker: &deleteOnlyBroker{}, logger: discardLogger()}
	if res := s.handleDeleteTopic(encodeDeleteReq(t, "orders")); res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Status)
	}
}
