package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// produce-dispatch-commit#1: each pending record of a deleted topic used
// to be confirmed deleted on its own, one leader RPC (or Raft barrier on
// the leader) per record, while the dispatcher committed nothing else.
// One confirmation covers every record of the topic.
func TestZZWP6DeletedTopicIsConfirmedOncePerTopic(t *testing.T) {
	const n = 300
	store := newTestStore(t)
	ctx := context.Background()
	// The dispatcher is node-other, a follower; the store's Raft leader is
	// node-self, reachable at a fake address.
	for _, m := range []metastore.Member{
		{ID: "node-self", Addr: "leader.example:7942", Status: metastore.MemberAlive},
		{ID: "node-other", Addr: "other.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	seedNamedProduceDispatchTopic(t, store, "orders", "node-other", 1)
	seedNamedProduceDispatchTopic(t, store, "doomed", "node-other", 2)
	manager := newDispatchIngressManagerLargeSegments(t)
	for i := range n {
		if _, err := manager.AcceptProduce(ctx, "doomed", "k", i%2, []byte(`{"d":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	live, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"o":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTopic(ctx, "doomed"); err != nil {
		t.Fatal(err)
	}
	var leaderCalls atomic.Int32
	peer := fakePeerClient{getTopicFn: func(context.Context, string, string) (nodewire.Response, error) {
		leaderCalls.Add(1)
		return nodewire.Response{Status: http.StatusNotFound}, nil
	}}
	committer := &fakeProduceCommitter{}
	d := NewProduceDispatcher(manager, store, "node-other", committer, peer, nil, ProduceDispatcherConfig{})
	if _, err := d.DispatchAvailable(ctx); err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if got := committer.committed(); len(got) != 1 || got[0].WAL.Seq != live.WAL.Seq {
		t.Fatalf("committed %d records, want only the live topic's", len(got))
	}
	if next, _ := manager.LoadProduceCheckpoint(); next != n+1 {
		t.Fatalf("checkpoint = %d, want %d (the deleted topic's records discarded)", next, n+1)
	}
	if calls := leaderCalls.Load(); calls > 2 {
		t.Fatalf("%d leader confirmations for %d records of one deleted topic, want one per topic", calls, n)
	}
}

// zzWP6SeedIncarnation creates topic "orders" as incarnation id with
// partitions owned per owners.
func zzWP6SeedIncarnation(t *testing.T, store *metastore.Store, id string, owners ...string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: id, Partitions: len(owners)}); err != nil {
		t.Fatal(err)
	}
	for p, owner := range owners {
		if err := store.AssignPartition(ctx, "orders", p, owner); err != nil {
			t.Fatal(err)
		}
	}
}

func zzWP6RegisterMembers(t *testing.T, store *metastore.Store) {
	t.Helper()
	for _, m := range []metastore.Member{
		{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
}

// produce-accept#0: a record still in the WAL when its topic is deleted
// and a topic of the same name is created must not commit into the new
// topic (its schema and owner are not the ones the record was accepted
// under). The record carries its incarnation; the dispatcher checks it
// before it resolves or reroutes anything by name, and discards the
// record once the leader confirms the incarnation is gone. A record with
// no incarnation (accepted by an older release) keeps the by-name
// behaviour.
func TestZZWP6RecreatedTopicDoesNotReceiveOldRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		// newOwners are the new incarnation's partition owners;
		// node-remote is marked dead, so a record whose partition it
		// owns would be rerouted to a live sibling.
		newOwners []string
	}{
		{"same-partition", []string{"node-self", "node-self", "node-self"}},
		{"reroute", []string{"node-self", "node-self", "node-remote"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			zzWP6RegisterMembers(t, store)
			zzWP6SeedIncarnation(t, store, "incarnation-1", "node-self", "node-self", "node-self")
			manager := newDispatchIngressManager(t)
			if _, err := manager.AcceptProduceWithTopicID(ctx, "orders", "incarnation-1", "k", 2, []byte(`{"id":42}`)); err != nil {
				t.Fatal(err)
			}
			if err := store.DeleteTopic(ctx, "orders"); err != nil {
				t.Fatal(err)
			}
			zzWP6SeedIncarnation(t, store, "incarnation-2", tc.newOwners...)
			if err := store.MarkMemberDead(ctx, "node-remote"); err != nil {
				t.Fatal(err)
			}
			// A new record of the new incarnation, and a legacy record
			// without one, both for a local partition.
			if _, err := manager.AcceptProduceWithTopicID(ctx, "orders", "incarnation-2", "k", 1, []byte(`{"sku":"a"}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.AcceptProduce(ctx, "orders", "k", 1, []byte(`{"legacy":true}`)); err != nil {
				t.Fatal(err)
			}

			committer := &fakeProduceCommitter{}
			d := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})
			for range 3 {
				if _, err := d.DispatchAvailable(ctx); err != nil {
					t.Fatalf("DispatchAvailable() error = %v", err)
				}
			}
			got := committer.committed()
			for _, r := range got {
				if string(r.Payload) == `{"id":42}` {
					t.Fatalf("the deleted incarnation's record was committed into the new topic (partition %d)", r.TargetPartition)
				}
			}
			if len(got) != 2 {
				t.Fatalf("committed %d records, want 2 (the new incarnation's and the legacy one)", len(got))
			}
			if next, _ := manager.LoadProduceCheckpoint(); next != 3 {
				t.Fatalf("checkpoint = %d, want 3 (the old record discarded, not pinned)", next)
			}
		})
	}
}

// A remote commit carries each record's incarnation so the owner can
// refuse a replaced one. An owner on an older release refuses a batch
// with the trailing topic IDs (400, trailing data): the dispatcher then
// resends it without them and keeps doing so for that owner.
func TestZZWP6RemoteCommitCarriesTopicIDsWithLegacyFallback(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			zzWP6RegisterMembers(t, store)
			zzWP6SeedIncarnation(t, store, "incarnation-1", "node-remote", "node-remote", "node-remote")
			manager := newDispatchIngressManager(t)
			var mu sync.Mutex
			var calls int
			var got []nodewire.CommitProduceRequest
			peer := fakePeerClient{commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
				mu.Lock()
				defer mu.Unlock()
				calls++
				if legacy {
					// What an older owner's decoder makes of the frame.
					payload, err := nodewire.EncodeCommitProduceBatchRequest(req)
					if err != nil {
						return nodewire.Response{}, err
					}
					withoutIDs := nodewire.CommitProduceBatchRequest{Records: append([]nodewire.CommitProduceRequest(nil), req.Records...)}
					for i := range withoutIDs.Records {
						withoutIDs.Records[i].TopicID = ""
					}
					plain, err := nodewire.EncodeCommitProduceBatchRequest(withoutIDs)
					if err != nil {
						return nodewire.Response{}, err
					}
					if len(payload) != len(plain) {
						return nodewire.Response{Status: http.StatusBadRequest, Body: []byte("invalid commit produce batch request: trailing node rpc payload data")}, nil
					}
				}
				got = append(got, req.Records...)
				return nodewire.Response{Status: http.StatusOK}, nil
			}}
			d := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, peer, nil, ProduceDispatcherConfig{})
			for i := range 2 {
				if _, err := manager.AcceptProduceWithTopicID(ctx, "orders", "incarnation-1", "k", 0, []byte(`{"id":1}`)); err != nil {
					t.Fatal(err)
				}
				if _, err := d.DispatchAvailable(ctx); err != nil {
					t.Fatalf("pass %d: DispatchAvailable() error = %v", i, err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(got) != 2 {
				t.Fatalf("owner committed %d records, want 2", len(got))
			}
			wantID, wantCalls := "incarnation-1", 2
			if legacy {
				// One refused batch, its resend, then straight without.
				wantID, wantCalls = "", 3
			}
			for _, r := range got {
				if r.TopicID != wantID {
					t.Fatalf("owner saw topic id %q, want %q", r.TopicID, wantID)
				}
			}
			if calls != wantCalls {
				t.Fatalf("%d commit RPCs, want %d", calls, wantCalls)
			}
		})
	}
}
