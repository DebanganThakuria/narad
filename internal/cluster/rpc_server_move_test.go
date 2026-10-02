package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handlerFrames is a frameTransport that answers every request with one
// RPC handler, so a real *PeerClient talks to a real handler.
type handlerFrames struct {
	handle func(payload []byte) nodewire.Response
}

func (f handlerFrames) RequestOnLane(_ context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	reply, err := nodewire.EncodeResponse(f.handle(payload))
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: reply}, nil
}

func (f handlerFrames) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLane(ctx, addr, lane, frameType, payload)
}

// handlerClient is a PeerClient whose every request is answered by
// handle.
func handlerClient(handle func([]byte) nodewire.Response) *PeerClient {
	return &PeerClient{frames: handlerFrames{handle: handle}}
}

// A flip the leader's state machine refused can never commit, so the
// destination may act on it; any other error may still reach the
// leader's log. The leader's answer has to tell the two apart: 409 for
// the compare-and-set refusing, 404 for a partition with no assignment,
// 503 for everything else.
func TestCompleteMoveRPCTellsARefusalFromAnUnknownOutcome(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "narad-src"); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(nil, store, nil)
	status := func(topicName, owner, target string) int {
		t.Helper()
		payload, err := nodewire.EncodeCompleteMoveRequest(nodewire.CompleteMoveRequest{Topic: topicName, Partition: 0, ExpectedOwner: owner, TargetID: target})
		if err != nil {
			t.Fatal(err)
		}
		return server.handleCompleteMove(payload).Status
	}
	if got := status("orders", "narad-src", "narad-dst"); got != http.StatusConflict {
		t.Errorf("a CAS refusal answered %d, want 409", got)
	}
	if got := status("gone", "narad-src", "narad-dst"); got != http.StatusNotFound {
		t.Errorf("a flip of a partition with no assignment answered %d, want 404", got)
	}

	client := handlerClient(server.handleCompleteMove)
	if err := client.CompleteMove(ctx, "leader", "orders", 0, "narad-src", "narad-dst"); err == nil || !flipSettled(err) {
		t.Errorf("forwarded CAS refusal: err %v settled %v, want a settled refusal", err, flipSettled(err))
	}
	if err := client.CompleteMove(ctx, "leader", "gone", 0, "narad-src", "narad-dst"); err == nil || !flipSettled(err) {
		t.Errorf("forwarded flip of a missing partition: err %v settled %v, want a settled refusal", err, flipSettled(err))
	}

	// An older leader answers 503 for every failure: not settled.
	older := handlerClient(func([]byte) nodewire.Response {
		return errorResponse(http.StatusServiceUnavailable, "complete move failed: leadership lost")
	})
	if err := older.CompleteMove(ctx, "leader", "orders", 0, "narad-src", "narad-dst"); err == nil || flipSettled(err) {
		t.Errorf("503: err %v settled %v, want an unsettled error", err, flipSettled(err))
	}
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("%w: leadership lost while committing log", errs.ErrUnavailable),
	} {
		if flipSettled(err) {
			t.Errorf("flipSettled(%v) = true, want the outcome unknown", err)
		}
	}
	for _, err := range []error{
		fmt.Errorf("%w: complete-move owner is %q, expected %q", errs.ErrInvalidArgument, "narad-1", "narad-0"),
		errs.ErrNotFound,
		&flipNotProposedError{cause: fmt.Errorf("no known leader")},
	} {
		if !flipSettled(err) {
			t.Errorf("flipSettled(%v) = false, want settled", err)
		}
	}

	// A valid flip still answers 204.
	if err := store.SetAssignmentTarget(ctx, "orders", 0, "narad-dst"); err != nil {
		t.Fatal(err)
	}
	if err := client.CompleteMove(ctx, "leader", "orders", 0, "narad-src", "narad-dst"); err != nil {
		t.Fatalf("valid flip: %v", err)
	}
}

// Only the leader answers GetAssignment, behind a barrier, and marks its
// answer as a leader read; a follower answers 421. Releases before it
// answered from any replica and never marked the answer, so an older
// peer's answer is found or missing but never a leader read.
func TestGetAssignmentRPCAnswersOnlyAsTheBarrieredLeader(t *testing.T) {
	ctx := context.Background()
	stores := newTestStoreCluster(t, "n1", "n2", "n3")
	leaderID, leader := waitForClusterLeader(t, stores)
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := leader.AssignPartition(ctx, "orders", 0, "narad-src"); err != nil {
		t.Fatal(err)
	}
	var follower *metastore.Store
	for id, s := range stores {
		if id != leaderID {
			follower = s
			break
		}
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if _, err := follower.GetAssignment("orders", 0); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the follower never applied the assignment")
		}
	}
	client := func(s *metastore.Store) *PeerClient {
		return handlerClient(NewRPCServer(nil, s, nil).handleGetAssignment)
	}
	get := func(s *metastore.Store, partition int) nodewire.Response {
		t.Helper()
		payload, err := nodewire.EncodeGetAssignmentRequest(nodewire.GetAssignmentRequest{Topic: "orders", Partition: partition})
		if err != nil {
			t.Fatal(err)
		}
		return NewRPCServer(nil, s, nil).handleGetAssignment(payload)
	}

	if res := get(follower, 0); res.Status != http.StatusMisdirectedRequest {
		t.Errorf("a follower answered GetAssignment with %d, want 421", res.Status)
	}
	res := get(leader, 0)
	var body struct {
		Owner      string `json:"owner_id"`
		LeaderRead bool   `json:"leader_read"`
	}
	if err := json.Unmarshal(res.Body, &body); res.Status != http.StatusOK || err != nil || !body.LeaderRead || body.Owner != "narad-src" {
		t.Errorf("leader answered %d %s, want 200 with owner narad-src and leader_read", res.Status, res.Body)
	}
	if res := get(leader, 9); res.Status != http.StatusNotFound || json.Unmarshal(res.Body, &body) != nil || !body.LeaderRead {
		t.Errorf("leader on a missing assignment answered %d %s, want a leader-read 404", res.Status, res.Body)
	}

	a, found, leaderRead, err := client(leader).leaderAssignment(ctx, "leader", "orders", 0)
	if err != nil || !found || !leaderRead || a.OwnerID != "narad-src" {
		t.Errorf("leaderAssignment from the leader = %+v found=%v leader_read=%v err=%v, want narad-src from a leader read", a, found, leaderRead, err)
	}
	if _, found, leaderRead, err := client(leader).leaderAssignment(ctx, "leader", "orders", 9); err != nil || found || !leaderRead {
		t.Errorf("leaderAssignment of a missing partition: found=%v leader_read=%v err=%v, want a leader-read miss", found, leaderRead, err)
	}
	if _, _, _, err := client(follower).leaderAssignment(ctx, "follower", "orders", 0); err == nil {
		t.Error("leaderAssignment accepted a follower's answer")
	}
	// The unchanged client still decodes the leader's answer.
	if got, err := client(leader).GetAssignment(ctx, "leader", "orders", 0); err != nil || got.OwnerID != "narad-src" {
		t.Errorf("PeerClient.GetAssignment = %+v, %v", got, err)
	}

	older := handlerClient(func([]byte) nodewire.Response {
		body, _ := json.Marshal(metastore.Assignment{Topic: "orders", OwnerID: "narad-src"})
		return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}
	})
	if _, found, leaderRead, err := older.leaderAssignment(ctx, "old", "orders", 0); err != nil || !found || leaderRead {
		t.Errorf("older peer: found=%v leader_read=%v err=%v, want found and not a leader read", found, leaderRead, err)
	}
	olderMissing := handlerClient(func([]byte) nodewire.Response {
		return errorResponse(http.StatusNotFound, "assignment not found")
	})
	if _, found, leaderRead, err := olderMissing.leaderAssignment(ctx, "old", "orders", 0); err != nil || found || leaderRead {
		t.Errorf("older peer 404: found=%v leader_read=%v err=%v, want missing and not a leader read", found, leaderRead, err)
	}
}
