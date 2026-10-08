package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A forwarded schema update must be visible on the forwarding follower
// the moment the client gets its response, without sleeping: the
// follower waits for its replica to reach the leader's applied index.
func TestForwardedAlterIsVisibleOnTheFollowerWhenItAnswers(t *testing.T) {
	stores := newTestStoreCluster(t, "n1", "n2", "n3")
	leaderID, leader := waitForClusterLeader(t, stores)
	var follower *metastore.Store
	var followerID string
	for id, s := range stores {
		if id != leaderID {
			follower, followerID = s, id
			break
		}
	}
	ctx := context.Background()
	if err := leader.RegisterMember(ctx, metastore.Member{ID: leaderID, Addr: "leader:1", ClusterAddr: leader.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	router := NewRouter(follower, followerID, partition.NewHashRoundRobin(), "")
	deadline := time.Now().Add(10 * time.Second)
	for router.leaderMemberAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("follower never learned the leader's member address")
		}
		time.Sleep(20 * time.Millisecond)
	}

	var probes atomic.Int32
	router.peer = fakePeerClient{
		alterTopicFn: func(ctx context.Context, addr, topicName string, body []byte) (nodewire.Response, error) {
			// Stand in for the leader's handler: the write lands on the
			// leader store, exactly as a real forward would.
			if err := leader.PutSchema(ctx, topicName, 1, body); err != nil {
				return nodewire.Response{}, err
			}
			return jsonResponse(http.StatusOK, map[string]any{"name": topicName}), nil
		},
		appliedIndexFn: func(context.Context, string) (uint64, error) {
			probes.Add(1)
			return leader.AppliedIndex(), nil
		},
	}

	for i := range 20 {
		// Each round registers on a fresh topic so the visibility check is
		// a strict read-after-write on the follower, not a re-read.
		name := "orders"
		if i > 0 {
			name = "orders-" + string(rune('a'+i))
			if err := leader.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 1}); err != nil {
				t.Fatalf("CreateTopic(%s): %v", name, err)
			}
		}
		rec := httptest.NewRecorder()
		handled := router.RouteAlterTopic(ctx, rec, nil, name, []byte(`{"type":"object"}`))
		if !handled {
			t.Fatalf("RouteAlterTopic() handled = false, want forwarded")
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("RouteAlterTopic() status = %d body %s", rec.Code, rec.Body.String())
		}
		history, err := schema.PersistedHistory(ctx, follower, name)
		if err != nil {
			t.Fatalf("follower history(%s): %v", name, err)
		}
		if len(history) != 1 {
			t.Fatalf("round %d: follower sees %d schema versions right after the forwarded alter answered, want 1", i, len(history))
		}
	}
	if probes.Load() != 20 {
		t.Fatalf("applied-index probes = %d, want one per successful forward", probes.Load())
	}
}

func TestSettleForwardedWriteSkipsNonSuccessAndUnsupportedLeaders(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")

	var probes atomic.Int32
	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		probes.Add(1)
		return 0, errors.New("unsupported rpc operation")
	}}
	ctx := context.Background()

	// A failed forward is answered as-is: no probe.
	router.settleForwardedWrite(ctx, "leader:1", nodewire.Response{Status: http.StatusConflict})
	if probes.Load() != 0 {
		t.Fatalf("probe sent for a 409, want none")
	}

	// A leader that predates the probe: answered immediately.
	start := time.Now()
	router.settleForwardedWrite(ctx, "leader:1", nodewire.Response{Status: http.StatusOK})
	if probes.Load() != 1 {
		t.Fatalf("probes = %d, want 1", probes.Load())
	}
	if time.Since(start) > time.Second {
		t.Fatalf("settle took %s with an unsupported leader, want immediate", time.Since(start))
	}

	// A bound the replica cannot reach: the client's context ends the wait.
	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		return store.AppliedIndex() + 1_000_000, nil
	}}
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	rec := httptest.NewRecorder()
	router.writeForwardedWrite(shortCtx, rec, "leader:1", jsonResponse(http.StatusOK, map[string]string{"ok": "yes"}), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the leader's 200 even when the local wait times out", rec.Code)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("settle took %s, want about the context deadline", elapsed)
	}
}

// The ingress's catch-up before a 404 (SyncWithLeader) fails as
// retryable when it cannot confirm with the leader, and succeeds once
// the replica has reached the leader's applied index; the leader itself
// only barriers.
func TestSyncWithLeaderFailsAsUnavailableWithoutTheLeader(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// This node leads: a barrier, nothing to ask.
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	if err := router.SyncWithLeader(ctx); err != nil {
		t.Fatalf("SyncWithLeader on the leader: %v", err)
	}

	stores := newTestStoreCluster(t, "n1", "n2", "n3")
	leaderID, leader := waitForClusterLeader(t, stores)
	var follower *metastore.Store
	var followerID string
	for id, s := range stores {
		if id != leaderID {
			follower, followerID = s, id
			break
		}
	}
	router = NewRouter(follower, followerID, partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{}
	if err := router.SyncWithLeader(ctx); !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("SyncWithLeader with no leader member on record = %v, want ErrUnavailable", err)
	}

	if err := leader.RegisterMember(ctx, metastore.Member{ID: leaderID, Addr: "leader:1", ClusterAddr: leader.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for router.leaderMemberAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("follower never learned the leader's member address")
		}
		time.Sleep(20 * time.Millisecond)
	}
	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		return 0, errors.New("connection refused")
	}}
	if err := router.SyncWithLeader(ctx); !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("SyncWithLeader with the leader unreachable = %v, want ErrUnavailable", err)
	}

	if err := leader.CreateTopic(ctx, topic.Topic{Name: "fresh", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		return leader.AppliedIndex(), nil
	}}
	if err := router.SyncWithLeader(ctx); err != nil {
		t.Fatalf("SyncWithLeader: %v", err)
	}
	if _, err := follower.GetTopic(ctx, "fresh"); err != nil {
		t.Fatalf("the topic the leader created is not on the synced replica: %v", err)
	}
}

// The strict settle the remote writes use answers nil only once this
// replica has applied what the leader had: a probe that fails and a
// bound the replica cannot reach are errs.ErrNotAppliedHere, and an
// answer that changed nothing needs no probe.
func TestStrictSettleReportsAWriteNotAppliedHere(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	ctx := context.Background()

	var probes atomic.Int32
	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		probes.Add(1)
		return 0, errors.New("not the leader")
	}}
	if err := router.settleForwardedWriteStrict(ctx, "leader:1", nodewire.Response{Status: http.StatusConflict}); err != nil || probes.Load() != 0 {
		t.Fatalf("a 409: %v after %d probes, want nil and no probe", err, probes.Load())
	}
	if err := router.settleForwardedWriteStrict(ctx, "leader:1", nodewire.Response{Status: http.StatusOK}); !errors.Is(err, errs.ErrNotAppliedHere) {
		t.Fatalf("a failed probe: %v, want ErrNotAppliedHere", err)
	}

	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		return store.AppliedIndex() + 1_000_000, nil
	}}
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := router.settleForwardedWriteStrict(shortCtx, "leader:1", nodewire.Response{Status: http.StatusOK}); !errors.Is(err, errs.ErrNotAppliedHere) {
		t.Fatalf("an index the replica never reaches: %v, want ErrNotAppliedHere", err)
	}

	router.peer = fakePeerClient{appliedIndexFn: func(context.Context, string) (uint64, error) {
		return store.AppliedIndex(), nil
	}}
	if err := router.settleForwardedWriteStrict(ctx, "leader:1", nodewire.Response{Status: http.StatusOK}); err != nil {
		t.Fatalf("a replica already caught up: %v", err)
	}
}
