package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The scan limit bounds the records a member reads, not the ones that
// match: a backlog of other topics' records past it stops the scan, and
// the answer is incomplete, which counts as unshipped.
func TestUnshippedScanStopsAtTheLimitOfRecordsRead(t *testing.T) {
	rg := deleteRig(t)
	ing := rg.src.ingress
	ctx := context.Background()
	for range 5 {
		if _, err := ing.AcceptProduceWithTopicID(ctx, "other", "other-id", "", 0, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ing.AcceptProduceWithTopicID(ctx, "orders", "src-orders-id", "", 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if n, complete, err := ing.PendingForTopic(context.Background(), "src-orders-id", "orders", 3); err != nil || complete {
		t.Fatalf("scan limited to 3 over 6 records: count %d complete %v err %v, want an incomplete scan", n, complete, err)
	}
	if n, complete, err := ing.PendingForTopic(context.Background(), "src-orders-id", "orders", 6); err != nil || !complete || n != 1 {
		t.Fatalf("scan limited to the backlog's size: count %d complete %v err %v, want 1, complete", n, complete, err)
	}
}

// A scan stops when the leader has stopped waiting for it, and answers
// incomplete, instead of replaying the rest of the backlog for nobody.
func TestUnshippedScanStopsWhenItsContextEnds(t *testing.T) {
	rg := deleteRig(t)
	ing := rg.src.ingress
	for range 3 {
		if _, err := ing.AcceptProduceWithTopicID(context.Background(), "orders", "src-orders-id", "", 0, []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, complete, err := ing.PendingForTopic(ctx, "src-orders-id", "orders", 100); complete || !errors.Is(err, context.Canceled) {
		t.Fatalf("scan with its context ended: count %d complete %v err %v, want an incomplete scan and the context's error", n, complete, err)
	}
}

// blockingScans stands in for the ingress backlog scan: each scan
// reports its start and holds until released.
type blockingScans struct {
	started  chan string
	release  chan struct{}
	inflight atomic.Int32
	peak     atomic.Int32
	calls    atomic.Int32
}

func (b *blockingScans) scan(ctx context.Context, _, name string, _ int) (uint64, bool, error) {
	b.calls.Add(1)
	n := b.inflight.Add(1)
	defer b.inflight.Add(-1)
	for {
		p := b.peak.Load()
		if n <= p || b.peak.CompareAndSwap(p, n) {
			break
		}
	}
	b.started <- name
	select {
	case <-b.release:
		return 0, true, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

func (b *blockingScans) expectStart(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-b.started:
		if want != "" && got != want {
			t.Fatalf("scan of %s started, want %s", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no scan of %q started", want)
	}
}

func (b *blockingScans) expectNoStart(t *testing.T) {
	t.Helper()
	select {
	case got := <-b.started:
		t.Fatalf("a scan of %s started", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// Queries that arrive while a member scans a topic do not each start a
// scan of their own once it ends: they share the next one, which starts
// after they arrived. Across topics, a member runs at most
// maxUnshippedScans at once.
func TestMemberUnshippedScansAreSharedAndCapped(t *testing.T) {
	s := linksRig(t)
	scans := &blockingScans{started: make(chan string, 8), release: make(chan struct{})}
	s.links.scanBacklog = scans.scan
	ask := func(name string) <-chan nodewire.Response {
		out := make(chan nodewire.Response, 1)
		q, _ := json.Marshal(map[string]string{"topic": name})
		go func() {
			out <- s.links.ServeUnshipped(context.Background(), nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckUnshipped, Body: q})
		}()
		return out
	}
	answered := func(ch <-chan nodewire.Response) {
		t.Helper()
		select {
		case res := <-ch:
			if res.Status != http.StatusOK || bodyOf(t, res)["complete"] != true {
				t.Fatalf("answer: %d %s", res.Status, res.Body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a query never answered")
		}
	}

	first := ask("orders")
	scans.expectStart(t, "orders")
	later := []<-chan nodewire.Response{ask("orders"), ask("orders")}
	rigWait(t, "the later queries to wait", 10*time.Second, func() bool {
		return goroutinesIn("(*RemoteLinks).memberScan") >= 3
	})
	scans.release <- struct{}{}
	answered(first)
	scans.expectStart(t, "orders")
	scans.expectNoStart(t)
	scans.release <- struct{}{}
	for _, ch := range later {
		answered(ch)
	}
	if n := scans.calls.Load(); n != 2 {
		t.Fatalf("%d scans of one topic for three queries, want 2", n)
	}

	var queries []<-chan nodewire.Response
	for _, name := range []string{"t1", "t2", "t3"} {
		queries = append(queries, ask(name))
	}
	scans.expectStart(t, "")
	scans.expectStart(t, "")
	scans.expectNoStart(t)
	for range 3 {
		scans.release <- struct{}{}
	}
	for _, ch := range queries {
		answered(ch)
	}
	if p := scans.peak.Load(); p != maxUnshippedScans {
		t.Fatalf("%d scans at once, want at most %d", p, maxUnshippedScans)
	}
}

// A member's scan that cannot finish (a slow disk, a long backlog, or a
// wait for a scan slot) answers complete=false while the leader still
// waits for it: the leader then reports it under
// backlog_over_scan_limit, not as a node that did not answer, which the
// docs tell the operator means the node is down.
func TestMemberUnshippedScanAnswersBeforeTheLeaderStopsWaiting(t *testing.T) {
	s := linksRig(t)
	s.links.askTimeout = time.Second
	scans := &blockingScans{started: make(chan string, 8), release: make(chan struct{})}
	s.links.scanBacklog = scans.scan
	q, _ := json.Marshal(map[string]string{"topic": "orders"})
	// The leader's clock starts before the query reaches the member.
	leaderDeadline := time.Now().Add(s.links.askTimeout)
	res := s.links.ServeUnshipped(context.Background(), nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckUnshipped, Body: q})
	if res.Status != http.StatusOK || bodyOf(t, res)["complete"] != false {
		t.Fatalf("answer: %d %s, want 200 and an incomplete scan", res.Status, res.Body)
	}
	if spare := time.Until(leaderDeadline); spare < s.links.askTimeout/10 {
		t.Fatalf("the member answered %s before the leader stopped waiting; want time for the answer to travel", spare)
	}
}
