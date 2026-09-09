package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// Tests for the dispatcher that serves queue-style consumes.
//
// The shape these cover replaced one where every parked consumer was
// woken by a commit and raced for the record: N wake-ups, N scans, one
// winner. Now a consumer enqueues and blocks, and a single pump does the
// reservation once and hands it over. The properties worth protecting
// are therefore about WHO gets woken and WHEN anything is reserved, not
// just about a message arriving eventually — a broadcast-and-race
// implementation would pass a naive "did it arrive" test too.

// tryConsume is mustConsume without the t.Fatal, so it is safe to call
// from the goroutines these tests need. It reports the message, whether
// one came back, and the raw status for the cases that care.
func tryConsume(e *env, topicName, wait string) (topic.Message, bool, int) {
	u := e.url("/v1/topics/" + topicName + "/consume")
	if wait != "" {
		u += "?wait=" + wait
	}
	resp, err := http.Get(u)
	if err != nil {
		return topic.Message{}, false, 0
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return topic.Message{}, false, resp.StatusCode
	}
	var msg topic.Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return topic.Message{}, false, resp.StatusCode
	}
	return msg, true, resp.StatusCode
}

// TestConsume_OneRecordWakesExactlyOneWaiter is the dispatcher's whole
// reason for existing. Twelve consumers park on an empty topic and one
// record is produced. Exactly one must be served: never zero, which
// would be a lost wake-up, and never two, which would mean the record
// was handed out twice.
//
// The design this replaced woke all twelve and let them race for it.
// That produced the right answer too, at eleven wasted scans per commit,
// so this test is about the cost as much as the outcome.
func TestConsume_OneRecordWakesExactlyOneWaiter(t *testing.T) {
	t.Parallel()
	const waiters = 12
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "one-waiter", Partitions: 3})

	var mu sync.Mutex
	var served []int64
	var wg sync.WaitGroup
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if msg, ok, _ := tryConsume(e, "one-waiter", "4s"); ok {
				mu.Lock()
				served = append(served, msg.Offset)
				mu.Unlock()
			}
		}()
	}
	// Let every consumer park before anything is available, so this
	// exercises the wake path rather than the inline probe.
	time.Sleep(500 * time.Millisecond)
	mustProduce(t, e, "one-waiter", "k", map[string]any{"n": 1})
	wg.Wait()

	if len(served) != 1 {
		t.Fatalf("one record reached %d consumers, want exactly 1 (offsets %v)", len(served), served)
	}
}

// TestConsume_ManyRecordsReachManyWaiters pins the other half: the pump
// keeps handing records out while both sides of its gate hold, so five
// records committed while five consumers wait reach five of them rather
// than being drip-fed one per wake-up.
func TestConsume_ManyRecordsReachManyWaiters(t *testing.T) {
	t.Parallel()
	const waiters, records = 5, 5
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "many-waiter", Partitions: 3})

	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if msg, ok, _ := tryConsume(e, "many-waiter", "6s"); ok {
				mu.Lock()
				seen[msg.Offset]++
				mu.Unlock()
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	for i := range records {
		mustProduce(t, e, "many-waiter", "k", map[string]any{"n": i})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != records {
		t.Fatalf("%d records reached %d consumers, want %d", records, len(seen), records)
	}
	for off, n := range seen {
		if n != 1 {
			t.Fatalf("offset %d delivered %d times, want once", off, n)
		}
	}
}

// TestConsume_NothingIsReservedWithoutAWaiter pins the invariant that
// removes the give-back problem: the pump reserves only once it holds a
// waiter. A record produced while nobody is parked must still be
// sitting there, unreserved, for the next consumer to take.
//
// If the pump reserved speculatively, this consume would find nothing
// and the record would be invisible until its visibility timeout.
func TestConsume_NothingIsReservedWithoutAWaiter(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "no-spec", Partitions: 3})

	mustProduce(t, e, "no-spec", "k", map[string]any{"n": 1})
	// Give the pump every chance to wake and grab it.
	time.Sleep(300 * time.Millisecond)

	if _, ok, status := tryConsume(e, "no-spec", "3s"); !ok {
		t.Fatalf("consume got status %d and no message: the record was reserved with no waiter to give it to", status)
	}
}

// TestConsume_WaitIsHonouredThenEmpty pins the budget. A consumer that
// nothing satisfies must park for its full wait and then answer 204,
// rather than returning early (which would make clients busy-poll) or
// hanging past it.
func TestConsume_WaitIsHonouredThenEmpty(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "budget", Partitions: 3})

	start := time.Now()
	_, ok, status := tryConsume(e, "budget", "700ms")
	elapsed := time.Since(start)

	if ok || status != http.StatusNoContent {
		t.Fatalf("consume on an empty topic = (found %v, status %d), want 204", ok, status)
	}
	if elapsed < 600*time.Millisecond {
		t.Fatalf("returned after %v, want the full 700ms budget honoured", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("returned after %v, want it to stop at the budget", elapsed)
	}
}

// TestConsume_LateProduceStillWakesAParkedConsumer is the lost-wake-up
// guard. The dispatcher gates on records AND a waiter, so it has to be
// woken by a change to either. A consumer that parks first and gets its
// record later exercises the "data arrived" arm; if only the "waiter
// arrived" arm worked, this would sit out its whole budget.
func TestConsume_LateProduceStillWakesAParkedConsumer(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "late", Partitions: 3})

	type result struct {
		found   bool
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		_, ok, _ := tryConsume(e, "late", "8s")
		done <- result{ok, time.Since(start)}
	}()

	time.Sleep(400 * time.Millisecond)
	mustProduce(t, e, "late", "k", map[string]any{"n": 1})

	select {
	case r := <-done:
		if !r.found {
			t.Fatal("a record produced while a consumer was parked never reached it")
		}
		if r.elapsed > 5*time.Second {
			t.Fatalf("delivered after %v: the consumer waited out its budget instead of being woken", r.elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consume never returned")
	}
}

// TestConsume_ClampedWaitIsAnnounced pins the header. A wait longer than
// the server's ceiling is truncated, and saying so is the difference
// between an explicable early 204 and a client that looks like it is
// losing messages.
func TestConsume_ClampedWaitIsAnnounced(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "clamped", Partitions: 3})

	resp := getJSON(t, e.url("/v1/topics/clamped/consume?wait=10m"))
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if got := resp.Header.Get("X-Narad-Wait-Clamped"); got == "" {
		t.Fatal("a 10m wait was truncated with no X-Narad-Wait-Clamped header: the client cannot tell it was shortened")
	}
}

// TestConsume_UnclampedWaitIsNotAnnounced keeps the signal meaningful:
// a header on every response would be ignored within a day.
func TestConsume_UnclampedWaitIsNotAnnounced(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	mustCreateTopic(t, e, createTopicReq{Name: "unclamped", Partitions: 3})

	resp := getJSON(t, e.url("/v1/topics/unclamped/consume?wait=200ms"))
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if got := resp.Header.Get("X-Narad-Wait-Clamped"); got != "" {
		t.Fatalf("X-Narad-Wait-Clamped = %q on a wait inside the ceiling, want no header", got)
	}
}
