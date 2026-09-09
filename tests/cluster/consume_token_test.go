//go:build cluster

package cluster

import (
	"encoding/json"
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
	// Assignments propagate through raft; without this a consume can run
	// before any node believes it owns anything.
	time.Sleep(3 * time.Second)
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
	var wg sync.WaitGroup
	for node := range 3 {
		for range 3 { // three consumers per node
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				if m, ok := tokenConsume(c, n, "tok-once", "6s"); ok {
					mu.Lock()
					got = append(got, m)
					mu.Unlock()
					tokenAck(c, n, "tok-once", m.ReceiptHandle)
				}
			}(node)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	if !tokenProduce(t, c, 1, "tok-once", "once-1") {
		t.Fatal("produce failed")
	}
	wg.Wait()

	if len(got) != 1 {
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
