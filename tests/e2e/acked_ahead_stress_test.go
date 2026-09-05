package e2e

// Stress test for the delivery-gated acked-ahead cap: with a tiny cap
// (2) and a small in-flight window (8), 64 consumers acking in random
// order with random delays, and a few consumers that hold a message
// past the visibility timeout, the broker must never reject an
// ack with 503 (ErrAckedAheadFull), every message must be acked at
// least once, the partition must drain to empty, and the drain must not
// crawl at one cap-sized batch per visibility timeout (the pre-fix
// behaviour: ~1024/3 visibility timeouts for this load).

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestAckedAheadStressNeverRejectsLiveAcks(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, withMaxConsumeWait(2*time.Second))

	const (
		topicName    = "aa-stress"
		total        = 1024
		workers      = 64
		visibilityMs = 1000
		slowHolds    = 3 // workers that hold their first message past the visibility timeout
		drainBudget  = 30 * time.Second
	)

	resp := env.post("/v1/topics", map[string]any{
		"name":                          topicName,
		"partitions":                    3,
		"max_acked_ahead_per_partition": int64(2),
		"max_in_flight_per_partition":   int64(8),
		"visibility_timeout_ms":         int64(visibilityMs),
	})
	expectStatus(t, resp, http.StatusCreated)

	// Pin every record to one partition so the whole load hits one shard.
	before := topicNextOffsets(t, env, topicName)
	for i := range total {
		r := env.rawPost("/v1/topics/"+topicName+"/produce?key=pinned", fmt.Sprintf(`{"n":%d}`, i))
		if r.StatusCode != http.StatusAccepted {
			t.Fatalf("produce %d: got %d body=%s", i, r.StatusCode, readBody(r))
		}
		r.Body.Close()
	}
	waitForVisibleDelta(t, env, topicName, before, total)
	details, err := env.Broker.GetTopicDetails(context.Background(), topicName)
	if err != nil {
		t.Fatalf("topic details: %v", err)
	}
	part := -1
	for i, p := range details.Partitions {
		if p.HighWatermark == total {
			part = i
		}
	}
	if part < 0 {
		t.Fatalf("no partition holds all %d records: %+v", total, details.Partitions)
	}

	var (
		acked      [total]atomic.Int32
		distinct   atomic.Int64 // offsets acked successfully at least once
		delivered  atomic.Int64
		ack503     atomic.Int64
		ackStale   atomic.Int64
		holds      atomic.Int64
		stop       atomic.Bool
		consumeURL = env.url(fmt.Sprintf("/v1/topics/%s/consume?partition=%d&wait=200ms", topicName, part))
		ackURL     = env.url("/v1/topics/" + topicName + "/ack?receipt_handle=")
	)

	start := time.Now()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w)+1, 0xA5))
			received := 0
			for !stop.Load() {
				resp, err := http.Get(consumeURL)
				if err != nil {
					t.Errorf("worker %d consume: %v", w, err)
					return
				}
				if resp.StatusCode == http.StatusNoContent {
					resp.Body.Close()
					continue
				}
				if resp.StatusCode != http.StatusOK {
					t.Errorf("worker %d consume: status %d body=%s", w, resp.StatusCode, readBody(resp))
					return
				}
				var m topic.Message
				err = json.NewDecoder(resp.Body).Decode(&m)
				resp.Body.Close()
				if err != nil {
					t.Errorf("worker %d decode: %v", w, err)
					return
				}
				delivered.Add(1)
				received++

				// The first slowHolds workers each sit on their first message
				// past the visibility timeout: the late ack is a legitimate
				// 410, the message is redelivered to someone else, and while
				// it is the frontier hole the cap stalls fresh deliveries for
				// at most one visibility timeout. (With 64 workers the
				// partition drains in a couple of seconds, so a single slow
				// worker could not get through three sequential holds.)
				if w < slowHolds && received == 1 {
					holds.Add(1)
					time.Sleep(visibilityMs*time.Millisecond + 300*time.Millisecond)
				} else {
					time.Sleep(time.Duration(rng.IntN(4)) * time.Millisecond)
				}

				req, _ := http.NewRequest(http.MethodPost, ackURL+url.QueryEscape(m.ReceiptHandle), nil)
				ackResp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Errorf("worker %d ack: %v", w, err)
					return
				}
				body := readBody(ackResp)
				switch ackResp.StatusCode {
				case http.StatusNoContent:
					if acked[m.Offset].Add(1) == 1 {
						distinct.Add(1)
					}
				case http.StatusServiceUnavailable:
					ack503.Add(1)
					t.Errorf("worker %d: ack of offset %d rejected with 503: %s", w, m.Offset, body)
				case http.StatusGone:
					ackStale.Add(1)
				default:
					t.Errorf("worker %d: ack of offset %d: status %d body=%s", w, m.Offset, ackResp.StatusCode, body)
				}
			}
		})
	}

	// Drain: every offset has been acked with 204 at least once. Each
	// accepted ack either advances the frontier or parks the offset in
	// the ahead set, and the parked run collapses when its hole is acked,
	// so once every offset is acked the committed frontier is the tail.
	// (topic details expose the producer's next_offset, not the
	// consumer frontier, so this is the only client-visible drain signal.)
	deadline := time.Now().Add(drainBudget)
	drained := false
	for time.Now().Before(deadline) {
		if distinct.Load() == total {
			drained = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()

	missing := 0
	for off := range total {
		if acked[off].Load() == 0 {
			missing++
		}
	}
	t.Logf("drained=%v elapsed=%s delivered=%d (redeliveries %d) never-acked=%d stale-acks=%d 503s=%d slow-holds=%d",
		drained, elapsed, delivered.Load(), delivered.Load()-total, missing, ackStale.Load(), ack503.Load(), holds.Load())

	if !drained {
		t.Fatalf("partition %d did not drain within %s: %d of %d offsets acked", part, drainBudget, distinct.Load(), total)
	}
	if n := ack503.Load(); n != 0 {
		t.Fatalf("%d acks were rejected with 503 (acked-ahead full); the cap must gate delivery, not acks", n)
	}
	if missing != 0 {
		t.Fatalf("%d of %d offsets were never acked successfully", missing, total)
	}
	if h := holds.Load(); h != slowHolds {
		t.Errorf("%d messages were held past the visibility timeout, want %d", h, slowHolds)
	}
	if r := env.get(fmt.Sprintf("/v1/topics/%s/consume?partition=%d", topicName, part)); r.StatusCode != http.StatusNoContent {
		t.Fatalf("consume after drain: got %d, want 204", r.StatusCode)
	}
	// The pre-fix crawl was one cap-sized batch per visibility timeout,
	// i.e. hundreds of seconds for 1024 records; the budget here is a few
	// holds' worth of visibility timeout plus HTTP throughput.
	if elapsed > drainBudget {
		t.Fatalf("drain took %s, expected well under %s (%d slow holds x %dms visibility)", elapsed, drainBudget, slowHolds, visibilityMs)
	}
}
