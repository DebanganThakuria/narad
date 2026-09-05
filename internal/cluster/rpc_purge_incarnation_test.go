package cluster

// The purge RPC names the incarnation being deleted. These tests pin
// the two behaviours the audit found missing: a purge no longer skips
// because the NAME exists locally (it proceeds once the local record is
// a different incarnation), and an older receiver that cannot decode
// the ID field gets the name-only purge it has always understood.

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// purgeOnlyBroker records PurgeTopic calls; every other method panics
// on the nil embedded interface, which the purge handler never reaches.
type purgeOnlyBroker struct {
	broker.Broker
	mu     sync.Mutex
	purged [][2]string
}

func (b *purgeOnlyBroker) PurgeTopic(_ context.Context, name, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.purged = append(b.purged, [2]string{name, id})
	return nil
}

func (b *purgeOnlyBroker) calls() [][2]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][2]string(nil), b.purged...)
}

func encodePurgeReq(t *testing.T, name, id string) []byte {
	t.Helper()
	payload, err := nodewire.EncodeTopicNameRequest(nodewire.OpPurgeTopic, nodewire.TopicNameRequest{Topic: name, ID: id})
	if err != nil {
		t.Fatalf("EncodeTopicNameRequest: %v", err)
	}
	return payload
}

// The scenario from the audit: the member applied delete+create of
// "orders" before the purge for the OLD incarnation arrived. Judged by
// name the topic "still exists" and the purge used to be skipped for
// good, leaving the old data for the new topic to reopen. Judged by
// incarnation the old one is gone, so the purge runs, with the old ID.
func TestRPCServerPurgeProceedsWhenNameWasRecreated(t *testing.T) {
	store := newTestStore(t)
	br := &purgeOnlyBroker{}
	s := &RPCServer{store: store, broker: br, logger: discardLogger()}
	ctx := context.Background()

	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0000000000000002", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(again): %v", err)
	}

	start := time.Now()
	res := s.handlePurgeTopic(encodePurgeReq(t, "orders", "0000000000000001"))
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Status)
	}
	if elapsed := time.Since(start); elapsed > purgeApplyWaitTimeout/2 {
		t.Fatalf("purge waited %v for the recreated name; it should proceed at once", elapsed)
	}
	if got := br.calls(); len(got) != 1 || got[0] != [2]string{"orders", "0000000000000001"} {
		t.Fatalf("PurgeTopic calls = %v, want [[orders 0000000000000001]]", got)
	}
}

// A purge for the incarnation the local replica STILL shows is deferred
// (the replica has not applied the delete): purging now would let a
// concurrent open resurrect the log. And a name-only purge (older
// sender) keeps the old rule: skipped while the name exists.
func TestRPCServerPurgeWaitsForIncarnationToGo(t *testing.T) {
	store := newTestStore(t)
	s := &RPCServer{store: store}
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0000000000000003", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if s.waitIncarnationGoneLocally("orders", "0000000000000003", 200*time.Millisecond) {
		t.Fatal("live incarnation reported gone")
	}
	if s.waitIncarnationGoneLocally("orders", "", 200*time.Millisecond) {
		t.Fatal("name-only wait reported a live name gone")
	}
	if !s.waitIncarnationGoneLocally("orders", "0000000000000004", 2*time.Second) {
		t.Fatal("a different incarnation under the name should count as gone")
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if !s.waitIncarnationGoneLocally("orders", "0000000000000003", 2*time.Second) {
		t.Fatal("deleted incarnation not reported gone")
	}
}

// A record without an ID under the name (created by an older binary)
// counts as gone for a purge that names an incarnation: the purged one
// had an ID, so that record is a different incarnation.
func TestRPCServerPurgeTreatsRecordWithoutIDAsOtherIncarnation(t *testing.T) {
	store := newTestStore(t)
	s := &RPCServer{store: store}
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if !s.waitIncarnationGoneLocally("orders", "0000000000000005", time.Second) {
		t.Fatal("record without an ID should not hold up a purge by incarnation")
	}
}

// fallbackTransport answers the first request with 400 (an older
// receiver that cannot decode the trailing ID) and records every
// payload so the retry can be inspected.
type fallbackTransport struct {
	payloads [][]byte
	statuses []int
}

func (f *fallbackTransport) RequestOnLane(_ context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	f.payloads = append(f.payloads, append([]byte(nil), payload...))
	status := f.statuses[len(f.payloads)-1]
	encoded, err := nodewire.EncodeResponse(nodewire.Response{Status: status})
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: encoded}, nil
}

func TestPeerClientPurgeFallsBackToNameOnlyForOldReceiver(t *testing.T) {
	ctx := context.Background()
	tr := &fallbackTransport{statuses: []int{http.StatusBadRequest, http.StatusNoContent}}
	client := &PeerClient{frames: tr}
	res, err := client.PurgeTopic(ctx, "peer", "orders", "0000000000000006")
	if err != nil {
		t.Fatalf("PurgeTopic: %v", err)
	}
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want the retry's 204", res.Status)
	}
	if len(tr.payloads) != 2 {
		t.Fatalf("requests = %d, want 2 (with ID, then name-only)", len(tr.payloads))
	}
	first, err := nodewire.DecodeTopicNameRequest(tr.payloads[0], nodewire.OpPurgeTopic)
	if err != nil || first.ID != "0000000000000006" {
		t.Fatalf("first request = %+v (err %v), want the ID-bearing purge", first, err)
	}
	second, err := nodewire.DecodeTopicNameRequest(tr.payloads[1], nodewire.OpPurgeTopic)
	if err != nil || second.ID != "" || second.Topic != "orders" {
		t.Fatalf("retry = %+v (err %v), want the name-only purge", second, err)
	}

	// A receiver that understands the ID answers once; no retry.
	tr = &fallbackTransport{statuses: []int{http.StatusNoContent}}
	client = &PeerClient{frames: tr}
	if _, err := client.PurgeTopic(ctx, "peer", "orders", "0000000000000006"); err != nil {
		t.Fatalf("PurgeTopic: %v", err)
	}
	if len(tr.payloads) != 1 {
		t.Fatalf("requests = %d, want 1", len(tr.payloads))
	}
}

// The broadcast hands every member the deleted incarnation's ID.
func TestBroadcastDeleteTopicCarriesIncarnation(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var gotID string
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, _, _, id string) (nodewire.Response, error) {
		gotID = id
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	if err := router.BroadcastDeleteTopic(ctx, "orders", "0000000000000007"); err != nil {
		t.Fatalf("BroadcastDeleteTopic: %v", err)
	}
	if gotID != "0000000000000007" {
		t.Fatalf("purge id = %q, want 0000000000000007", gotID)
	}
}

// A forwarded delete reads the incarnation before deleting and hands
// it to the purge fan-out.
func TestRPCServerForwardedDeleteBroadcastsIncarnation(t *testing.T) {
	br := &deleteWithRecordBroker{record: topic.Topic{Name: "orders", ID: "0000000000000008"}}
	bc := &recordingIDBroadcaster{}
	s := &RPCServer{broker: br, logger: discardLogger(), broadcaster: bc}
	if res := s.handleDeleteTopic(encodeDeleteReq(t, "orders")); res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Status)
	}
	if bc.id != "0000000000000008" {
		t.Fatalf("broadcast id = %q, want 0000000000000008", bc.id)
	}
}

type deleteWithRecordBroker struct {
	broker.Broker
	record topic.Topic
}

func (b *deleteWithRecordBroker) GetTopic(context.Context, string) (topic.Topic, error) {
	return b.record, nil
}
func (b *deleteWithRecordBroker) DeleteTopic(context.Context, string) error { return nil }

type recordingIDBroadcaster struct{ id string }

func (b *recordingIDBroadcaster) BroadcastDeleteTopic(_ context.Context, _, id string) error {
	b.id = id
	return nil
}
