//go:build cluster

package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Cross-node consume: the token protocol.
//
// A consumer's HTTP request lands on whichever node a load balancer
// picked, which is usually not the one holding the partition its record
// lands on. Before tokens, such a consumer only ever heard about data on
// its own node, so a record produced elsewhere stayed invisible for the
// client's entire wait and came back 204.
//
// These are the properties that need a real multi-node cluster: the
// single-node dispatcher tests in tests/e2e cover the local half, but
// nothing there can tell a working token exchange from a broker that
// happens to own every partition.
//
// They assert on TIMING as much as outcome. "The record arrived
// eventually" is satisfied by a consumer that waited out its budget and
// was served by the client's next poll, which is exactly the bug this
// protocol exists to fix.

// tokenTopic creates a topic and waits for its partitions to be spread
// across the cluster, so a consumer on node 0 genuinely has remote
// owners to leave tokens with.
func tokenTopic(t *testing.T, c *cluster, name string) {
	t.Helper()
	c.apiWant(0, http.MethodPost, "/v1/topics", map[string]any{
		"name": name, "partitions": 6,
	}, 30*time.Second, http.StatusOK, http.StatusCreated, http.StatusConflict)

	// Wait for every node to agree on who owns what, rather than sleeping
	// and hoping. Assignments propagate through raft, and a consume that
	// runs before they land does NOT wait: the router picks a local
	// partition from the assignment view, ConsumeProbe recomputes local
	// ownership, disagrees, and returns ErrNotPartitionOwner with a nil
	// waiter, which the handler answers 204 on the spot. The client asked
	// for six seconds and is answered in one millisecond, so any test that
	// parks consumers before this settles is really measuring the race.
	deadline := time.Now().Add(45 * time.Second)
	for {
		settled := true
		for node := range 3 {
			_, body := c.apiWant(node, http.MethodGet, "/v1/topics/"+name, nil,
				30*time.Second, http.StatusOK)
			var got struct {
				Stats []struct {
					OwnerNode string `json:"owner_node"`
				} `json:"partition_stats"`
			}
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("decode topic %s from node %d: %v", name, node, err)
			}
			if len(got.Stats) != 6 {
				settled = false
				break
			}
			for _, s := range got.Stats {
				if s.OwnerNode == "" {
					settled = false
					break
				}
			}
			if !settled {
				break
			}
		}
		if settled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("partition ownership for %s never settled across all nodes", name)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func tokenProduce(t *testing.T, c *cluster, node int, topicName, key string) bool {
	t.Helper()
	status, _, err := c.api(node, http.MethodPost,
		"/v1/topics/"+topicName+"/produce?key="+key, map[string]any{"id": key})
	return err == nil && (status == http.StatusAccepted || status == http.StatusOK)
}

type tokenMsg struct {
	Partition     int             `json:"partition"`
	Offset        int64           `json:"offset"`
	Payload       json.RawMessage `json:"payload"`
	ReceiptHandle string          `json:"receipt_handle"`
}

// tokenConsume is deliberately error-tolerant: these run from goroutines
// where t.Fatal is not allowed, and a node killed mid-test is expected.
func tokenConsume(c *cluster, node int, topicName, wait string) (tokenMsg, bool) {
	status, body, err := c.api(node, http.MethodGet,
		"/v1/topics/"+topicName+"/consume?wait="+wait, nil)
	if err != nil || status != http.StatusOK {
		return tokenMsg{}, false
	}
	var m tokenMsg
	if json.Unmarshal(body, &m) != nil {
		return tokenMsg{}, false
	}
	return m, true
}

func tokenAck(c *cluster, node int, topicName, handle string) {
	_, _, _ = c.api(node, http.MethodPost,
		"/v1/topics/"+topicName+"/ack?receipt_handle="+handle, nil)
}

func newTokenCluster(t *testing.T) *cluster {
	t.Helper()
	c := newCluster(t, clusterOptions{})
	c.startAll()
	c.waitAllReady(90 * time.Second)
	c.waitAdmin(60 * time.Second)
	return c
}

// TestTokenConsume_WakesAConsumerOnAnotherNode is the bug this protocol
// was built for. A consumer parks on node 0; a record is produced to
// node 2 and lands on whichever partition its key hashes to, usually one
// node 0 does not own. It must be delivered promptly rather than at the
// end of the budget.
//
// Repeated, because a single round could get lucky and hash to a
// partition node 0 happens to own, which the local path would serve
// without the token protocol doing anything.
func TestTokenConsume_WakesAConsumerOnAnotherNode(t *testing.T) {
	c := newTokenCluster(t)
	defer c.teardown()
	tokenTopic(t, c, "tok-wake")

	const rounds = 6
	var delivered int
	var slowest time.Duration

	for i := range rounds {
		type result struct {
			msg     tokenMsg
			ok      bool
			elapsed time.Duration
		}
		done := make(chan result, 1)
		go func() {
			start := time.Now()
			m, ok := tokenConsume(c, 0, "tok-wake", "8s")
			done <- result{m, ok, time.Since(start)}
		}()

		// Let the consumer park and register its tokens with the owners.
		time.Sleep(700 * time.Millisecond)
		produced := time.Now()
		if !tokenProduce(t, c, 2, "tok-wake", "wake-"+string(rune('a'+i))) {
			t.Fatalf("round %d: produce failed", i)
		}

		r := <-done
		if !r.ok {
			continue
		}
		delivered++
		if lat := time.Since(produced); lat > slowest {
			slowest = lat
		}
		tokenAck(c, 0, "tok-wake", r.msg.ReceiptHandle)
	}

	if delivered != rounds {
		t.Fatalf("%d/%d rounds delivered: a consumer parked on one node is not hearing about records on another",
			delivered, rounds)
	}
	// The budget is 8s. Anything near it means the consumer was not woken
	// and merely outlasted the produce.
	if slowest > 4*time.Second {
		t.Fatalf("slowest delivery %v: consumers are waiting out their budget rather than being notified", slowest)
	}
	t.Logf("%d/%d delivered, slowest %v after the produce", delivered, rounds, slowest)
}

// TestTokenConsume_OneRecordServesOneNode pins that the cross-node path
// does not duplicate. Consumers park on all three nodes and one record
// is produced. Exactly one must be served: two would mean the record was
// claimed twice, which the atomic reservation is supposed to prevent.
func TestTokenConsume_OneRecordServesOneNode(t *testing.T) {
	c := newTokenCluster(t)
	defer c.teardown()
	tokenTopic(t, c, "tok-once")

	var mu sync.Mutex
	var got []tokenMsg
	var outcomes []string
	var wg sync.WaitGroup
	for node := range 3 {
		for range 3 { // three consumers per node
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				start := time.Now()
				status, body, err := c.api(n, http.MethodGet,
					"/v1/topics/tok-once/consume?wait=6s", nil)
				mu.Lock()
				outcomes = append(outcomes, fmt.Sprintf("node %d: status=%d err=%v after %v body=%.120s",
					n, status, err, time.Since(start).Round(time.Millisecond), string(body)))
				mu.Unlock()
				if err != nil || status != http.StatusOK {
					return
				}
				var m tokenMsg
				if json.Unmarshal(body, &m) != nil {
					return
				}
				mu.Lock()
				got = append(got, m)
				mu.Unlock()
				tokenAck(c, n, "tok-once", m.ReceiptHandle)
			}(node)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	if !tokenProduce(t, c, 1, "tok-once", "once-1") {
		t.Fatal("produce failed")
	}
	wg.Wait()

	if len(got) != 1 {
		// Post-mortem: say WHERE the record went, so a failure separates
		// "delivered twice" from "never delivered at all" without a rerun.
		for _, o := range outcomes {
			t.Logf("consumer outcome: %s", o)
		}
		for i := range 3 {
			if m, ok := tokenConsume(c, i, "tok-once", "2s"); ok {
				t.Logf("post-mortem: node %d still finds partition %d offset %d unclaimed",
					i, m.Partition, m.Offset)
			} else {
				t.Logf("post-mortem: node %d finds nothing", i)
			}
		}
		_, body := c.apiWant(0, http.MethodGet, "/v1/topics/tok-once", nil, 20*time.Second, http.StatusOK)
		t.Logf("post-mortem topic state: %s", string(body))
		t.Fatalf("one record reached %d consumers across the cluster, want exactly 1", len(got))
	}
}

// TestTokenConsume_SurvivesAnOwnerDying pins that tokens are connection
// scoped. A node holding this consumer's tokens is killed; the tokens
// die with it, and the survivors must keep delivering records on the
// partitions they still own. Nothing acknowledged may be lost.
func TestTokenConsume_SurvivesAnOwnerDying(t *testing.T) {
	c := newTokenCluster(t)
	defer c.teardown()
	tokenTopic(t, c, "tok-kill")

	var mu sync.Mutex
	got := map[string]int{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Consume from the two nodes that will survive.
	for _, node := range []int{0, 1} {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if m, ok := tokenConsume(c, n, "tok-kill", "2s"); ok {
					var p struct {
						ID string `json:"id"`
					}
					_ = json.Unmarshal(m.Payload, &p)
					mu.Lock()
					got[p.ID]++
					mu.Unlock()
					tokenAck(c, n, "tok-kill", m.ReceiptHandle)
				}
			}
		}(node)
	}
	time.Sleep(1500 * time.Millisecond)

	sent := map[string]bool{}
	for i := range 16 {
		id := "kill-" + string(rune('a'+i))
		if tokenProduce(t, c, i%2, "tok-kill", id) {
			sent[id] = true
		}
		if i == 6 {
			c.kill(2)
		}
		time.Sleep(150 * time.Millisecond)
	}

	// Records on the dead node's partitions are unavailable until
	// ownership moves, which is the rebalance story rather than this one.
	// What must hold is that the cluster keeps serving at all.
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= len(sent)/2 {
			break
		}
		time.Sleep(time.Second)
	}
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("nothing was delivered after an owner died: losing one node stopped the cluster serving")
	}
	for id, n := range got {
		if n > 1 {
			t.Logf("redelivered %s %dx (expected: a record reserved by the dying node is redelivered)", id, n)
		}
	}
	t.Logf("delivered %d of %d produced with one of three owners killed mid-stream", len(got), len(sent))
}

// pinnedOwner produces n records under one key, so they all land on one
// partition, and reports that partition's owning node. Ownership is what
// decides which process has to be killed for a test about resurrection
// to be about resurrection at all.
func pinnedOwner(t *testing.T, c *cluster, topicName, key string, via, n int) (partition, owner int) {
	t.Helper()
	for range n {
		if !tokenProduce(t, c, via, topicName, key) {
			t.Fatalf("produce to %s failed", topicName)
		}
	}
	// Describe aggregates per-partition stats from their owners over RPC,
	// so the records are not visible here the instant produce returns.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, body := c.apiWant(via, http.MethodGet, "/v1/topics/"+topicName, nil,
			30*time.Second, http.StatusOK)
		var got struct {
			Stats []struct {
				Index         int    `json:"index"`
				HighWatermark int64  `json:"high_watermark"`
				OwnerNode     string `json:"owner_node"`
			} `json:"partition_stats"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode topic %s: %v", topicName, err)
		}
		found, holding := 0, 0
		for _, s := range got.Stats {
			if s.HighWatermark == 0 {
				continue
			}
			found++
			holding += int(s.HighWatermark)
			partition = s.Index
			// Node ids are narad-1..narad-N for harness indices 0..N-1.
			if _, err := fmt.Sscanf(s.OwnerNode, "narad-%d", &owner); err != nil {
				t.Fatalf("unexpected owner_node %q", s.OwnerNode)
			}
			owner--
		}
		if found > 1 {
			t.Fatalf("%d partitions hold records, want exactly 1: key %q did not pin them", found, key)
		}
		if found == 1 && holding >= n {
			return partition, owner
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d records visible on %d partitions after producing under key %q",
				holding, n, found, key)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// consumeLongPoll is tokenConsume for a budget longer than the harness
// client allows.
//
// c.http carries a 15s timeout, which is right for ordinary calls and
// wrong for a consume deliberately parked across a process restart: the
// client gives up at 15s and the caller sees a transport error that
// looks exactly like "nothing was delivered". This uses its own client
// so the server's answer is what the test measures.
func consumeLongPoll(c *cluster, node int, topicName, wait string, timeout time.Duration) (tokenMsg, bool) {
	req, err := http.NewRequest(http.MethodGet,
		c.url(node)+"/v1/topics/"+topicName+"/consume?wait="+wait, nil)
	if err != nil {
		return tokenMsg{}, false
	}
	req.SetBasicAuth("admin", adminPassword)
	req.Header.Set("X-Narad-Client", "cluster-test")

	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return tokenMsg{}, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return tokenMsg{}, false
	}
	var m tokenMsg
	if json.Unmarshal(body, &m) != nil {
		return tokenMsg{}, false
	}
	return m, true
}

// partitionOwner reports which node owns one partition right now, so a
// test can prove ownership did not quietly move underneath it.
func partitionOwner(t *testing.T, c *cluster, via int, topicName string, partition int) int {
	t.Helper()
	_, body := c.apiWant(via, http.MethodGet, "/v1/topics/"+topicName, nil,
		30*time.Second, http.StatusOK)
	var got struct {
		Stats []struct {
			Index     int    `json:"index"`
			OwnerNode string `json:"owner_node"`
		} `json:"partition_stats"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode topic %s: %v", topicName, err)
	}
	for _, s := range got.Stats {
		if s.Index != partition {
			continue
		}
		owner := 0
		if _, err := fmt.Sscanf(s.OwnerNode, "narad-%d", &owner); err != nil {
			t.Fatalf("unexpected owner_node %q", s.OwnerNode)
		}
		return owner - 1
	}
	t.Fatalf("topic %s has no partition %d", topicName, partition)
	return -1
}

// TestTokenConsume_ResurrectedOwnerDeliversItsBacklog covers the case
// the tests above leave open, and it only means anything in this precise
// order: the owner of the records is killed FIRST, a consumer parks on a
// survivor while it is down, and only then does it come back.
//
// Order is the whole test. A consumer that parks after the owner is
// already up finds the backlog with its opening fan-out probe, which is
// an ordinary local_only consume and involves no token at all. Parking
// while the owner is dead removes that path: the probe cannot reach it,
// so the consumer parks having left tokens only with the nodes that were
// alive, and the resurrected node holds records that nobody has asked
// it for.
//
// Nothing is produced after the restart either, so the records that come
// back are the pre-crash ones and no new write can be what woke anybody.
// Tokens are soft state and die with the process, so the resurrected node
// starts with an empty table: whatever reaches this consumer has to be
// established after the node is back. If nothing is, the consumer waits
// out its whole budget and the client only sees the backlog on its next
// poll, which is a full long-poll of added latency every time a node
// returns.
//
// The delivered record must come from the pinned partition, which the
// reader does not own and which stays owned by the node that died. That
// is what rules out the two ways this could pass without meaning
// anything: ownership quietly moving during the outage, and the reader
// being served from its own partitions.
func TestTokenConsume_ResurrectedOwnerDeliversItsBacklog(t *testing.T) {
	c := newCluster(t, clusterOptions{env: map[string]string{
		// The consumer has to stay parked across a process restart, which
		// is longer than the default ten second ceiling allows. Raising
		// the ceiling drags write_timeout and shutdown_grace with it:
		// config refuses a wait a response could not outlive.
		"NARAD_HTTP_MAX_CONSUME_WAIT": "45s",
		"NARAD_HTTP_WRITE_TIMEOUT":    "90s",
		"NARAD_HTTP_SHUTDOWN_GRACE":   "50s",
	}})
	c.startAll()
	c.waitAllReady(90 * time.Second)
	c.waitAdmin(60 * time.Second)
	defer c.teardown()
	tokenTopic(t, c, "tok-resurrect")

	const backlog = 4
	partition, owner := pinnedOwner(t, c, "tok-resurrect", "pinned", 0, backlog)
	reader := (owner + 1) % 3
	t.Logf("%d records on partition %d owned by node %d; reading from node %d",
		backlog, partition, owner, reader)

	// Let the records reach disk before the process is killed: the
	// durability of the un-flushed window is a different property.
	time.Sleep(2 * time.Second)
	c.kill(owner)
	time.Sleep(2 * time.Second)

	// Park a consumer while the owner is down, so its opening probe
	// cannot see the backlog and it genuinely has to be told about it.
	type result struct {
		msg     tokenMsg
		ok      bool
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		m, ok := consumeLongPoll(c, reader, "tok-resurrect", "40s", 60*time.Second)
		done <- result{m, ok, time.Since(start)}
	}()

	// Long enough that the consumer is parked and has registered its
	// tokens with the survivors, and short enough to be sure it is still
	// parked when the owner returns.
	time.Sleep(3 * time.Second)
	select {
	case r := <-done:
		t.Fatalf("the consumer returned (ok=%v) after %v while the owner was still dead: "+
			"it should have been parked with nothing to serve it", r.ok, r.elapsed)
	default:
	}

	c.start(owner)
	c.waitReady(owner, 90*time.Second)
	back := time.Now()

	select {
	case r := <-done:
		if !r.ok {
			t.Fatalf("the parked consumer came back empty after %v: a resurrected owner is "+
				"sitting on %d records and nobody is telling the consumer they exist",
				r.elapsed, backlog)
		}
		lat := time.Since(back)
		if lat > 15*time.Second {
			t.Fatalf("delivered %v after the owner came back: the consumer is not being told "+
				"about a node that returned, it is outlasting something else", lat)
		}
		if r.msg.Partition != partition {
			t.Fatalf("served partition %d, want the pinned partition %d: this consumer was "+
				"satisfied from somewhere other than the node that came back, so the test "+
				"proves nothing about resurrection", r.msg.Partition, partition)
		}
		if now := partitionOwner(t, c, reader, "tok-resurrect", partition); now != owner {
			t.Fatalf("partition %d is owned by node %d at delivery but was owned by node %d "+
				"before the kill: ownership moved during the outage, so the record could have "+
				"come from a new owner rather than from the node that came back",
				partition, now, owner)
		}
		t.Logf("delivered %v after the owner came back (consumer had been parked %v), partition %d offset %d",
			lat, r.elapsed, r.msg.Partition, r.msg.Offset)
		tokenAck(c, reader, "tok-resurrect", r.msg.ReceiptHandle)
	case <-time.After(45 * time.Second):
		t.Fatalf("the parked consumer never returned at all")
	}
}
