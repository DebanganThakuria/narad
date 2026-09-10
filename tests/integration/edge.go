package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// edge mode: the request-level edge cases from the state-transition plan
// that need no broker restart. Each case creates its own topic, prints
// PASS/FAIL with the evidence, and the mode fails if any case failed.
//
//	delete-while-parked   a consumer parked in a long poll returns promptly
//	                      when its topic is deleted, instead of sleeping out
//	                      its wait
//	cancel-during-park    a consumer that disconnects while parked does not
//	                      strand the next record: a later consumer gets it
//	cancel-vs-deliver     the same, with the record produced the instant the
//	                      consumer gives up (the handed-then-abandoned race)
//	delete-during-produce a topic deleted under producers stays deleted: no
//	                      late produce resurrects it, and a same-name
//	                      recreate starts empty
func runEdge(cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	lb := &roundRobinClient{
		nodes:    cfg.nodes,
		client:   &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}},
		username: cfg.username,
		password: cfg.password,
	}
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	failed := 0
	report := func(name string, ok bool, detail string) {
		if ok {
			fmt.Printf("PASS  %s: %s\n", name, detail)
		} else {
			failed++
			fmt.Printf("FAIL  %s: %s\n", name, detail)
		}
	}
	e := &edgeEnv{cfg: cfg, lb: lb}
	e.deleteWhileParked(ctx, report)
	e.cancelDuringPark(ctx, report)
	e.cancelVersusDeliver(ctx, report)
	e.deleteDuringProduce(ctx, report)
	if failed > 0 {
		return fmt.Errorf("edge: %d case(s) failed", failed)
	}
	fmt.Println("PASS edge: every case passed")
	return nil
}

type edgeEnv struct {
	cfg config
	lb  *roundRobinClient
}

func (e *edgeEnv) createTopic(ctx context.Context, name string, visibility time.Duration) error {
	req := newTopicRecord(name, 3, visibility)
	if err := retry(ctx, 20, 250*time.Millisecond, func() error {
		_, _, err := e.lb.do(ctx, http.MethodPost, "/v1/topics", req, nil, http.StatusCreated, http.StatusConflict)
		return err
	}); err != nil {
		return err
	}
	// A fresh topic's partitions are assigned asynchronously; a node that
	// has not seen the assignment yet answers a long poll with an
	// immediate 204 (nothing local, no remote owners to leave tokens
	// with). Every case here parks a consumer, so wait until every
	// partition has an owner.
	return retry(ctx, 40, 250*time.Millisecond, func() error {
		owners, err := partitionOwners(ctx, e.lb, name)
		if err != nil {
			return err
		}
		if len(owners) < req.Partitions {
			return fmt.Errorf("topic %s: %d/%d partitions assigned", name, len(owners), req.Partitions)
		}
		return nil
	})
}

func (e *edgeEnv) deleteTopic(ctx context.Context, name string) (int, error) {
	var st int
	err := retry(ctx, 20, 250*time.Millisecond, func() error {
		var err error
		// Control-plane writes answer 503 from a node that is not the
		// leader during an election; the same retry the create path has.
		st, _, err = e.lb.do(ctx, http.MethodDelete, "/v1/topics/"+url.PathEscape(name), nil, nil, http.StatusNoContent, http.StatusOK, http.StatusNotFound)
		return err
	})
	return st, err
}

func (e *edgeEnv) produce(ctx context.Context, node, name string, seq int) (int, error) {
	body, _ := json.Marshal(map[string]any{"id": fmt.Sprintf("%s/%d", name, seq), "seq": seq})
	st, _, err := e.lb.doRawTo(ctx, node, http.MethodPost, "/v1/topics/"+url.PathEscape(name)+"/produce?key=k", body, nil,
		http.StatusAccepted, http.StatusNotFound, http.StatusServiceUnavailable, http.StatusConflict)
	return st, err
}

// consume returns status, the message when 200, and the wall time spent.
func (e *edgeEnv) consume(ctx context.Context, node, name string, wait time.Duration) (int, consumeResponse, time.Duration, error) {
	var m consumeResponse
	start := time.Now()
	st, _, err := e.lb.doTo(ctx, node, http.MethodGet, "/v1/topics/"+url.PathEscape(name)+"/consume?wait="+wait.String(), nil, &m,
		http.StatusOK, http.StatusNoContent, http.StatusNotFound)
	return st, m, time.Since(start), err
}

func (e *edgeEnv) ack(ctx context.Context, node, name, handle string) {
	_, _, _ = e.lb.doTo(ctx, node, http.MethodPost, "/v1/topics/"+url.PathEscape(name)+"/ack?receipt_handle="+url.QueryEscape(handle), nil, nil, http.StatusNoContent, http.StatusGone, http.StatusNotFound)
}

// deleteWhileParked: park on node 1 (so the wait spans the cross-node
// path when the owner is elsewhere), delete the topic from node 0 after
// a second, and require the poll to come back well inside its 15s wait.
func (e *edgeEnv) deleteWhileParked(ctx context.Context, report func(string, bool, string)) {
	name := e.cfg.runID + "-edge-delpark"
	if err := e.createTopic(ctx, name, 30*time.Second); err != nil {
		report("delete-while-parked", false, "create: "+err.Error())
		return
	}
	type res struct {
		st  int
		d   time.Duration
		err error
	}
	done := make(chan res, 1)
	go func() {
		st, _, d, err := e.consume(ctx, e.cfg.nodes[1%len(e.cfg.nodes)], name, 15*time.Second)
		done <- res{st: st, d: d, err: err}
	}()
	time.Sleep(time.Second)
	if _, err := e.deleteTopic(ctx, name); err != nil {
		report("delete-while-parked", false, "delete: "+err.Error())
		<-done
		return
	}
	r := <-done
	// The delete is issued one second in, so a return before that means
	// the poll never parked (nothing to wake); after 8s it slept on.
	ok := r.err == nil && (r.st == http.StatusNoContent || r.st == http.StatusNotFound) && r.d >= time.Second && r.d < 8*time.Second
	report("delete-while-parked", ok, fmt.Sprintf("parked consume returned %d after %s (want 204/404 between the 1s delete and well under the 15s wait), err=%v", r.st, r.d.Round(time.Millisecond), r.err))
}

// cancelDuringPark: a consumer parks with a 10s wait and its client goes
// away after 500ms. A record produced afterwards must reach a fresh
// consumer, not sit reserved for the one that left.
func (e *edgeEnv) cancelDuringPark(ctx context.Context, report func(string, bool, string)) {
	name := e.cfg.runID + "-edge-cancel"
	if err := e.createTopic(ctx, name, 30*time.Second); err != nil {
		report("cancel-during-park", false, "create: "+err.Error())
		return
	}
	defer func() { _, _ = e.deleteTopic(context.Background(), name) }()
	node := e.cfg.nodes[1%len(e.cfg.nodes)]
	parkCtx, cancelPark := context.WithCancel(ctx)
	gone := make(chan struct{})
	go func() {
		_, _, _, _ = e.consume(parkCtx, node, name, 10*time.Second)
		close(gone)
	}()
	time.Sleep(500 * time.Millisecond)
	cancelPark()
	<-gone
	time.Sleep(200 * time.Millisecond)
	if st, err := e.produce(ctx, e.cfg.nodes[0], name, 1); err != nil || st != http.StatusAccepted {
		report("cancel-during-park", false, fmt.Sprintf("produce: status %d err %v", st, err))
		return
	}
	st, m, d, err := e.consume(ctx, node, name, 3*time.Second)
	ok := err == nil && st == http.StatusOK
	if ok {
		e.ack(ctx, node, name, m.ReceiptHandle)
	}
	report("cancel-during-park", ok, fmt.Sprintf("fresh consumer got status %d after %s (want 200: the record was not stranded on the abandoned waiter), err=%v", st, d.Round(time.Millisecond), err))
}

// cancelVersusDeliver: the handed-then-abandoned race. Many consumers
// park and are cancelled at random moments while records stream in. Every
// record must end up delivered to a consumer that is still there, within
// one visibility timeout at the very worst (a record handed to a waiter
// that left is released at once, not held for the lease).
func (e *edgeEnv) cancelVersusDeliver(ctx context.Context, report func(string, bool, string)) {
	name := e.cfg.runID + "-edge-race"
	const records = 200
	visibility := 20 * time.Second
	if err := e.createTopic(ctx, name, visibility); err != nil {
		report("cancel-vs-deliver", false, "create: "+err.Error())
		return
	}
	defer func() { _, _ = e.deleteTopic(context.Background(), name) }()
	var seen sync.Map
	var delivered atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Churning consumers: park, get cancelled after a random short time.
	for i := range 8 {
		wg.Go(func() {
			node := e.cfg.nodes[i%len(e.cfg.nodes)]
			for iter := 0; ; iter++ {
				select {
				case <-stop:
					return
				default:
				}
				// 20-219ms, varied per worker AND per iteration so the
				// cancellations land at different phases of the delivery.
				jitter := time.Duration(20+(i*37+iter*53)%200) * time.Millisecond
				cctx, cancel := context.WithTimeout(ctx, jitter)
				st, m, _, err := e.consume(cctx, node, name, 2*time.Second)
				cancel()
				if err == nil && st == http.StatusOK {
					if _, dup := seen.LoadOrStore(m.Payload.ID, true); !dup {
						delivered.Add(1)
					}
					e.ack(ctx, node, name, m.ReceiptHandle)
				}
			}
		})
	}
	// Steady consumers that never cancel: whatever the churners drop,
	// these must pick up.
	for i := range 2 {
		wg.Go(func() {
			node := e.cfg.nodes[(i+1)%len(e.cfg.nodes)]
			for {
				select {
				case <-stop:
					return
				default:
				}
				st, m, _, err := e.consume(ctx, node, name, time.Second)
				if err == nil && st == http.StatusOK {
					if _, dup := seen.LoadOrStore(m.Payload.ID, true); !dup {
						delivered.Add(1)
					}
					e.ack(ctx, node, name, m.ReceiptHandle)
				}
			}
		})
	}
	start := time.Now()
	for seq := 1; seq <= records; seq++ {
		if st, err := e.produce(ctx, e.cfg.nodes[seq%len(e.cfg.nodes)], name, seq); err != nil || st != http.StatusAccepted {
			report("cancel-vs-deliver", false, fmt.Sprintf("produce %d: status %d err %v", seq, st, err))
			close(stop)
			wg.Wait()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline := time.Now().Add(visibility + 10*time.Second)
	for delivered.Load() < records && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	took := time.Since(start)
	close(stop)
	wg.Wait()
	got := delivered.Load()
	report("cancel-vs-deliver", got == records, fmt.Sprintf("%d/%d records delivered to a live consumer in %s under constant consumer cancellation (budget: one visibility timeout)", got, records, took.Round(time.Millisecond)))
}

// deleteDuringProduce: producers hammer a topic while it is deleted. After
// the delete no produce may succeed and the topic must stay 404; a
// same-name recreate must start empty (no resurrected records) and serve
// only what is produced after it.
func (e *edgeEnv) deleteDuringProduce(ctx context.Context, report func(string, bool, string)) {
	name := e.cfg.runID + "-edge-delprod"
	if err := e.createTopic(ctx, name, 30*time.Second); err != nil {
		report("delete-during-produce", false, "create: "+err.Error())
		return
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var acceptedAfterDelete atomic.Int64
	var deletedAt atomic.Int64
	for i := range 12 {
		wg.Go(func() {
			node := e.cfg.nodes[i%len(e.cfg.nodes)]
			seq := i * 1_000_000
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq++
				st, err := e.produce(ctx, node, name, seq)
				if err == nil && st == http.StatusAccepted {
					// The delete returns once the deleting node has applied
					// it; the other nodes learn through raft a moment later,
					// so a produce to them in that window is legitimately
					// accepted into an ingress WAL that then drops it. Only a
					// 202 well after that window means the topic came back.
					if at := deletedAt.Load(); at != 0 && time.Now().UnixNano() > at+int64(2*time.Second) {
						acceptedAfterDelete.Add(1)
					}
				}
			}
		})
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := e.deleteTopic(ctx, name); err != nil {
		close(stop)
		wg.Wait()
		report("delete-during-produce", false, "delete: "+err.Error())
		return
	}
	deletedAt.Store(time.Now().UnixNano())
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()
	st, _, _ := e.lb.do(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(name), nil, nil, http.StatusOK, http.StatusNotFound)
	stillGone := st == http.StatusNotFound
	// Recreate under the same name: it must be empty.
	if err := e.createTopic(ctx, name, 30*time.Second); err != nil {
		report("delete-during-produce", false, "recreate: "+err.Error())
		return
	}
	defer func() { _, _ = e.deleteTopic(context.Background(), name) }()
	resurrected := 0
	for range 6 {
		cst, m, _, err := e.consume(ctx, e.cfg.nodes[0], name, time.Second)
		if err == nil && cst == http.StatusOK {
			resurrected++
			e.ack(ctx, e.cfg.nodes[0], name, m.ReceiptHandle)
		}
	}
	freshOK := false
	if pst, err := e.produce(ctx, e.cfg.nodes[1%len(e.cfg.nodes)], name, 42); err == nil && pst == http.StatusAccepted {
		cst, m, _, err := e.consume(ctx, e.cfg.nodes[2%len(e.cfg.nodes)], name, 5*time.Second)
		freshOK = err == nil && cst == http.StatusOK && m.Payload.ID == fmt.Sprintf("%s/%d", name, 42)
		if cst == http.StatusOK {
			e.ack(ctx, e.cfg.nodes[2%len(e.cfg.nodes)], name, m.ReceiptHandle)
		}
	}
	ok := stillGone && acceptedAfterDelete.Load() == 0 && resurrected == 0 && freshOK
	report("delete-during-produce", ok, fmt.Sprintf("after delete under 12 producers: topic 404=%v, produces accepted >2s after delete=%d, records resurrected on same-name recreate=%d, fresh produce/consume on the recreate=%v", stillGone, acceptedAfterDelete.Load(), resurrected, freshOK))
}
