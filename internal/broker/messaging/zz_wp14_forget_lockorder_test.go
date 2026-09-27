package messaging

import (
	"fmt"
	"os"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// Lock order between a forget, the pump and a log open. Three edges meet
// here:
//
//	pump:   a topic's st.mu, then Logs.mu (consumable -> PeekHighWatermark -> Peek)
//	open:   Logs.mu (write), then d.mu (the opened hook -> wakeNotifier -> stateFor)
//	forget: must not hold d.mu while it waits for st.mu
//
// A forget that took d.mu and then waited for a topic's st.mu closed the
// cycle Logs.mu -> d.mu -> st.mu -> Logs.mu, and every Get on the node
// hung from then on.

// A forget that waits for a topic's dispatch lock leaves the dispatcher's
// map lock free, so a log open anywhere on the node goes through.
func TestZZWP14ForgetWaitingOnTopicLeavesMapLockFree(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	ms.topics["other"] = topic.Topic{Name: "other", Partitions: 1, VisibilityTimeoutMs: 60_000}
	e := newTestEngine(t, ms, nil, nil)
	if _, err := e.logs.Get("orders", 0); err != nil {
		t.Fatal(err)
	}
	st := e.dispatch.peekState("orders")
	if st == nil {
		t.Fatal("opening the log created no dispatch state; the test exercises nothing")
	}

	// Stand in for the pump inside consumable(), which holds the topic's
	// lock across Logs reads and, for a closed log, file reads.
	st.mu.Lock()
	forgot := make(chan struct{})
	go func() {
		// The topic manager's dropTopicCaches: a purge, or the retired
		// hook of an open that quarantined a stale incarnation.
		e.ForgetTopic("orders")
		close(forgot)
	}()
	zzWP7aWaitStack(t, "the forget to wait for the topic's dispatch lock", func(count func(string) int) bool {
		return count("(*dispatcher).forget") > 0
	})

	opened := make(chan error, 1)
	go func() {
		// A lazy open of another topic's log: its opened hook calls
		// stateFor under Logs.mu.
		_, err := e.logs.Get("other", 0)
		opened <- err
	}()
	var openErr error
	blocked := false
	select {
	case openErr = <-opened:
	case <-time.After(2 * time.Second):
		blocked = true
	}
	st.mu.Unlock()
	<-forgot
	if blocked {
		openErr = <-opened
		t.Fatal("a log open waited behind a forget that held the dispatcher's map lock while it waited for a topic's dispatch lock")
	}
	if openErr != nil {
		t.Fatal(openErr)
	}

	// The forget still did its job once it got the topic's lock.
	if e.dispatch.peekState("orders") != nil {
		t.Fatal("the forgotten topic's dispatch state is still in the map")
	}
	if !st.retired.Load() {
		t.Fatal("the dropped state was not marked retired")
	}
	// Demand arriving afterwards lands on a fresh, live state.
	e.dispatch.registerRemote("orders", []int{0}, &fakeRemote{refuse: true})
	if live := e.dispatch.peekState("orders"); live == nil || live == st || live.retired.Load() {
		t.Fatal("demand after the forget did not get a fresh live state")
	}
}

// zzWP14SpinRemote is a peer token with no blocking hook: Expired is an
// atomic load and Notify passes at once, so the pump keeps serving it and
// runs consumable() under the topic's lock on every turn, as it does in
// production for a peer that keeps declining.
type zzWP14SpinRemote struct {
	stop     atomic.Bool
	notifies atomic.Int64
}

func (r *zzWP14SpinRemote) Expired() bool { return r.stop.Load() }

func (r *zzWP14SpinRemote) Notify(_ string, done func(bool)) bool {
	r.notifies.Add(1)
	done(false)
	return true
}

// The real pump, a forget loop and a lazy-open loop, with nothing
// steering them. Before the fix this wedged Logs.mu within milliseconds.
func TestZZWP14ForgetPumpAndOpenDoNotDeadlock(t *testing.T) {
	const parts = 4
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: parts, VisibilityTimeoutMs: 60_000}
	ms.topics["other"] = topic.Topic{Name: "other", Partitions: parts, VisibilityTimeoutMs: 60_000}
	e := newTestEngine(t, ms, nil, nil)
	scan := make([]int, parts)
	for p := range parts {
		scan[p] = p
		commitRecordsOn(t, e, "orders", p, 5)
		// Closed with a persisted boundary: consumable() reads it from
		// disk under the topic's lock.
		if err := e.logs.ClosePartition("orders", p); err != nil {
			t.Fatal(err)
		}
	}
	rd := &zzWP14SpinRemote{}
	e.dispatch.registerRemote("orders", scan, rd)
	waitFor(t, func() bool { return rd.notifies.Load() > 0 }, "the pump never served the peer token")

	var forgets, opens atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			e.ForgetTopic("orders")
			forgets.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			p := i % parts
			if _, err := e.logs.Get("other", p); err == nil {
				_ = e.logs.ClosePartition("other", p)
			}
			opens.Add(1)
		}
	}()

	type progress struct{ notifies, forgets, opens int64 }
	read := func() progress { return progress{rd.notifies.Load(), forgets.Load(), opens.Load()} }
	last, lastMove := read(), time.Now()
	// Race for a second, then stop at the next sign of progress; a stall
	// keeps the loop going until the check below calls it.
	for end := time.Now().Add(time.Second); ; {
		time.Sleep(20 * time.Millisecond)
		if cur := read(); cur != last {
			last, lastMove = cur, time.Now()
			if time.Now().After(end) {
				break
			}
			continue
		}
		if time.Since(lastMove) < 3*time.Second {
			continue
		}
		// Nothing moved for 3s: the locks are held in a cycle. Cleanup
		// would hang on them (Logs.CloseAll), and so would a panic here,
		// since the test runner runs cleanups before re-panicking. Dump
		// the stacks and crash the binary from another goroutine instead
		// of hanging it until the -timeout.
		buf := make([]byte, 8<<20)
		n := goruntime.Stack(buf, true)
		fmt.Fprintf(os.Stderr, "DEADLOCK: forget, pump and log open made no progress for 3s (%+v)\n%s\n", last, buf[:n])
		go panic("forget, pump and log open deadlocked")
		select {}
	}
	close(stop)
	wg.Wait()
	rd.stop.Store(true)
	if last.forgets == 0 || last.opens == 0 {
		t.Fatalf("the race never ran: %+v", last)
	}
}
