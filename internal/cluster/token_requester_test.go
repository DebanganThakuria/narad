package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

const demandRemote = "remote.example:7942"

// demandRouter is a router on node-self that knows one remote member,
// whose token registrations land in the returned log.
func demandRouter(t *testing.T) (*Router, *metastore.Store, *registrationLog) {
	t.Helper()
	store := newTestStore(t)
	if err := store.RegisterMember(context.Background(), metastore.Member{ID: "node-remote", Addr: demandRemote, Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	reg := newRegistrationLog()
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{registerTokensFn: reg.registerTokens}
	router.SetSelfAddr("node-self.example:7942")
	return router, store, reg
}

// splitTopic creates a topic with one partition here and one on
// node-remote, so a consumer parking on it registers a token there.
func splitTopic(t *testing.T, store *metastore.Store, name string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic(%s) error = %v", name, err)
	}
	if err := store.AssignPartition(ctx, name, 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, name, 1, "node-self"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
}

func demandCount(q *tokenRequester) int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.topics)
}

// A consumer parks on each of 20 short-lived topics, is served locally,
// and the topic is deleted. Once the tokens it left have lapsed, a
// keeper pass forgets every one of them: the keeper no longer looks the
// owners of a deleted topic up in the replica twice a second forever.
func TestTokenKeeperForgetsATopicNobodyWaitsOn(t *testing.T) {
	router, store, _ := demandRouter(t)
	ctx := context.Background()
	q := router.tokens
	const n = 20
	const wait = 150 * time.Millisecond
	for i := range n {
		name := fmt.Sprintf("job-%d", i)
		splitTopic(t, store, name)
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/"+name+"/consume?wait=150ms", nil)
		local := &fakeLocalWaiter{
			delay: 5 * time.Millisecond, found: true,
			msg: topic.Message{Topic: name, Partition: 1, Offset: 0, ReceiptHandle: "1:0:1"},
		}
		if !router.RouteConsumeWait(ctx, res, req, name, wait, local) {
			t.Fatalf("RouteConsumeWait(%s) not handled: the consumer never parked on tokens", name)
		}
		if err := store.DeleteTopic(ctx, name); err != nil {
			t.Fatalf("DeleteTopic(%s) error = %v", name, err)
		}
	}
	if got := demandCount(q); got != n {
		t.Fatalf("setup: %d demand entries, want %d", got, n)
	}
	// Every token lapses registerShareWindow after its budget.
	time.Sleep(wait + registerShareWindow + 100*time.Millisecond)
	q.keepAlive(ctx)
	if got := demandCount(q); got != 0 {
		t.Fatalf("%d demand entries left after a keeper pass over %d deleted topics nobody waits on", got, n)
	}
}

// A consumer woken off the queue, whose demand the keeper pruned before
// it went back, is queued in the topic's fresh demand: the next offer
// reaches it, and its release takes it off that queue.
func TestParkAfterAPruneUsesAFreshDemand(t *testing.T) {
	router, _, _ := demandRouter(t)
	q := router.tokens
	ctx := context.Background()
	w, unpark := q.park("orders", time.Now().Add(time.Minute))
	if !q.WakeOneWaiter("orders", "a") || takeOffer(w) != "a" {
		t.Fatal("setup: the parked consumer was not offered the record")
	}
	// Off the queue and holding no token: the keeper may prune it now.
	q.keepAlive(ctx)
	q.repark("orders", w)
	if !q.WakeOneWaiter("orders", "b") {
		t.Fatal("a consumer reparked after a keeper pass is not offered the next record")
	}
	if got := takeOffer(w); got != "b" {
		t.Fatalf("took %q, want b", got)
	}
	q.repark("orders", w)
	unpark()
	d := q.lockExistingDemand("orders")
	if d == nil {
		t.Fatal("the reparked consumer's demand is gone")
	}
	left := len(d.waiters)
	d.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d waiters left after the consumer released itself", left)
	}
}

// A demand whose token is still live is kept, so a consumer parking in
// the share window still shares the token; it goes once the token
// lapses.
func TestTokenKeeperKeepsADemandWhoseTokenIsLive(t *testing.T) {
	router, store, reg := demandRouter(t)
	splitTopic(t, store, "orders")
	q := router.tokens
	ctx := context.Background()

	_, unpark := q.park("orders", time.Now().Add(time.Second))
	q.register(ctx, "orders", time.Second)
	unpark()
	q.keepAlive(ctx)
	if got := demandCount(q); got != 1 {
		t.Fatalf("a demand with a live token was pruned (%d entries)", got)
	}
	// A consumer parking inside the share window sends nothing.
	_, unpark2 := q.park("orders", time.Now().Add(500*time.Millisecond))
	q.register(ctx, "orders", 500*time.Millisecond)
	unpark2()
	waitFor(t, 5*time.Second, "the first registration", func() bool {
		adds, _ := reg.counts(demandRemote + "/orders")
		return adds >= 1
	})
	if adds, _ := reg.counts(demandRemote + "/orders"); adds != 1 {
		t.Fatalf("%d registrations; a consumer parking in the share window should send none", adds)
	}
	if n := q.pruneIdle(time.Now().Add(time.Second + 2*registerShareWindow)); n != 1 {
		t.Fatalf("pruned %d demands once every token lapsed, want 1", n)
	}
}

// 10k demands nobody waits on go in one keeper pass.
func TestTokenKeeperPrunesManyIdleDemandsInOnePass(t *testing.T) {
	router, _, _ := demandRouter(t)
	q := router.tokens
	const n = 10000
	for i := range n {
		q.demandFor(fmt.Sprintf("gone-%d", i))
	}
	q.keepAlive(context.Background())
	if got := demandCount(q); got != 0 {
		t.Fatalf("%d of %d idle demand entries survived a keeper pass", got, n)
	}
}

// Parking, waking, reparking and releasing race the keeper: every
// parked consumer is always reachable, and nothing is left behind.
func TestTokenKeeperPruneRacesParkingConsumers(t *testing.T) {
	router, _, _ := demandRouter(t)
	q := router.tokens
	ctx, cancel := context.WithCancel(context.Background())
	var keeper sync.WaitGroup
	keeper.Go(func() {
		for ctx.Err() == nil {
			q.keepAlive(ctx)
		}
	})
	var wg sync.WaitGroup
	errc := make(chan error, 8)
	for g := range 8 {
		wg.Go(func() {
			name := fmt.Sprintf("race-%d", g)
			for i := range 2000 {
				w, unpark := q.park(name, time.Now().Add(time.Minute))
				for round := range 2 {
					if !q.WakeOneWaiter(name, "a") || takeOffer(w) != "a" {
						errc <- fmt.Errorf("%s iteration %d round %d: the only parked consumer was not reached", name, i, round)
						unpark()
						return
					}
					q.repark(name, w)
				}
				unpark()
			}
		})
	}
	wg.Wait()
	cancel()
	keeper.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	q.keepAlive(context.Background())
	if got := demandCount(q); got != 0 {
		t.Fatalf("%d demand entries left once every consumer released itself", got)
	}
}

// takeOffer receives an offer's signal the way a parked consumer does,
// then takes the owner address it carried ("" when none is pending).
func takeOffer(w *localWaiter) string {
	select {
	case <-w.ch:
	default:
		return ""
	}
	return w.take()
}
