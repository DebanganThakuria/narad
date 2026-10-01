package messaging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// zzShipWaitRouter is a fakeRouter for a node that owns some of the
// topic's partitions: RouteConsume declines with the local partition
// pick, the remote owners have nothing, and RouteConsumeWait writes
// what wait writes and reports whether it handled the request. The
// shared fake always declines the wait.
type zzShipWaitRouter struct {
	*fakeRouter
	wait  func(w http.ResponseWriter) bool
	waits int
}

func (r *zzShipWaitRouter) RouteConsumeWait(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ time.Duration, _ handlers.LocalConsumeWaiter) bool {
	r.waits++
	return r.wait(w)
}

func newZZShipWaitRouter(pick *int, wait func(w http.ResponseWriter) bool) *zzShipWaitRouter {
	return &zzShipWaitRouter{
		fakeRouter: &fakeRouter{routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
			return false, pick
		}},
		wait: wait,
	}
}

// zzShipWriteRecord writes m as a single consume that delivered it.
func zzShipWriteRecord(w http.ResponseWriter, m topic.Message) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(m.AppendJSON(nil), '\n'))
}

// A batch long-poll on a node that owns some partitions, whose wait the
// router's token race won with one record: that record goes first, and
// one non-blocking scan of the local partitions from the router's pick
// tops the batch up with at most max-1 more, its byte bound less the
// record in hand.
func TestShipBatchLocalOwnerWaitHandledTopsUp(t *testing.T) {
	remote, more := zzWP12Msg(0, 6), zzWP12Msg(2, 7)
	pick := 2
	br := &zzWP12BatchBroker{
		fakeBroker: &fakeBroker{},
		batch: func(call int, _ brokermsg.ConsumeOpts, _ int) ([]topic.Message, error) {
			if call == 0 {
				return nil, nil // the probe finds nothing: a waiter comes back
			}
			return []topic.Message{more}, nil
		},
		wait: func(time.Duration) (topic.Message, bool, error) {
			t.Error("the local wait ran although the router handled the wait")
			return topic.Message{}, false, nil
		},
	}
	router := newZZShipWaitRouter(&pick, func(w http.ResponseWriter) bool {
		zzShipWriteRecord(w, remote)
		return true
	})
	res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(remote, more) {
		t.Fatalf("batch = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(remote, more))
	}
	if ct := res.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	calls := br.seen()
	if len(calls) != 2 || router.waits != 1 {
		t.Fatalf("calls = %+v, router waits %d; want the probe and one top-up around one wait", calls, router.waits)
	}
	top := calls[1]
	firstLen := len(remote.AppendJSON(nil))
	if top.max != 4 || top.held != 0 || top.opts.Wait != 0 || top.opts.Partition != nil ||
		top.opts.ScanStart == nil || *top.opts.ScanStart != pick || top.opts.MaxBytes != consumeBatchReserveBytes-firstLen {
		t.Fatalf("top-up = %+v; want a non-blocking scan of 4 from partition %d with %d bytes left", top, pick, consumeBatchReserveBytes-firstLen)
	}
}

// When the router handles the wait without a record, its response goes
// out unchanged: a 204 as a 204, and a plain-text error with its status,
// body and Content-Type. No top-up scan runs.
func TestShipBatchLocalOwnerWaitHandledPassesThrough(t *testing.T) {
	pick := 1
	for _, tc := range []struct {
		name   string
		write  func(w http.ResponseWriter)
		status int
		body   string
		ct     string
	}{
		{"nothing", func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }, http.StatusNoContent, "", ""},
		{"owner down", func(w http.ResponseWriter) {
			http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
		}, http.StatusServiceUnavailable, "partition owner is down; retry later\n", "text/plain"},
	} {
		br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records()}
		router := newZZShipWaitRouter(&pick, func(w http.ResponseWriter) bool {
			tc.write(w)
			return true
		})
		res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s")
		if res.Code != tc.status || res.Body.String() != tc.body || !strings.HasPrefix(res.Header().Get("Content-Type"), tc.ct) {
			t.Fatalf("%s: %d %q %q, want %d %q %q", tc.name, res.Code, res.Header().Get("Content-Type"), res.Body, tc.status, tc.ct, tc.body)
		}
		if calls := br.seen(); len(calls) != 1 {
			t.Fatalf("%s: calls = %+v, want the probe alone", tc.name, calls)
		}
	}
}

// When the router declines the wait (nothing remote to race), the local
// wait runs alone: its record is topped up from the router's pick; no
// record, or ownership that just moved, is 204; and any other failure
// maps as a single consume maps it.
func TestShipBatchLocalOwnerWaitDeclined(t *testing.T) {
	a, b := zzWP12Msg(1, 3), zzWP12Msg(1, 4)
	pick := 1
	for _, tc := range []struct {
		name   string
		wait   func(time.Duration) (topic.Message, bool, error)
		status int
		body   string
	}{
		{"record", func(time.Duration) (topic.Message, bool, error) { return a, true, nil }, http.StatusOK, zzWP12Envelope(a, b)},
		{"nothing", func(time.Duration) (topic.Message, bool, error) { return topic.Message{}, false, nil }, http.StatusNoContent, ""},
		{"ownership moved", func(time.Duration) (topic.Message, bool, error) {
			return topic.Message{}, false, errs.ErrNotPartitionOwner
		}, http.StatusNoContent, ""},
		{"topic gone", func(time.Duration) (topic.Message, bool, error) {
			return topic.Message{}, false, errs.ErrTopicNotFound
		}, http.StatusNotFound, ""},
	} {
		br := &zzWP12BatchBroker{
			fakeBroker: &fakeBroker{},
			batch: func(call int, _ brokermsg.ConsumeOpts, _ int) ([]topic.Message, error) {
				if call == 0 {
					return nil, nil
				}
				return []topic.Message{b}, nil
			},
			wait: tc.wait,
		}
		router := newZZShipWaitRouter(&pick, func(http.ResponseWriter) bool { return false })
		res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s")
		if res.Code != tc.status || (tc.body != "" && res.Body.String() != tc.body) {
			t.Fatalf("%s: %d %q, want %d %q", tc.name, res.Code, res.Body, tc.status, tc.body)
		}
		if router.waits != 1 {
			t.Fatalf("%s: router asked to wait %d times, want once", tc.name, router.waits)
		}
		calls := br.seen()
		if tc.status != http.StatusOK {
			if len(calls) != 1 {
				t.Fatalf("%s: calls = %+v, want the probe alone", tc.name, calls)
			}
			continue
		}
		if len(calls) != 2 {
			t.Fatalf("%s: calls = %+v, want the probe and one top-up", tc.name, calls)
		}
		top := calls[1]
		if top.max != 4 || top.held != 1 || top.opts.Wait != 0 || top.opts.ScanStart == nil || *top.opts.ScanStart != pick ||
			top.opts.MaxBytes != consumeBatchReserveBytes-len(a.Key)-len(a.Payload) {
			t.Fatalf("%s: top-up = %+v; want a non-blocking scan of 4 from partition %d holding the waited record", tc.name, top, pick)
		}
	}
}

// The reply bound covers the top-up after a declined wait on a node that
// owns some partitions: the waited record goes out first, and what the
// top-up adds past the bound is cut and given back at once.
func TestShipBatchLocalOwnerWaitReplyBound(t *testing.T) {
	payload := make([]byte, 64<<10)
	br := newZZWP20BoundBroker(0, payload)
	br.burst = 101
	pick := 0
	router := newZZShipWaitRouter(&pick, func(http.ResponseWriter) bool { return false })
	res := zzWP12Consume(newTestSet(br, router), "?max=100&wait=1s")
	msgs := zzWP20CheckBound(t, br, 0, res, payload)
	if len(msgs) < 2 || len(msgs) == br.reserved() {
		t.Fatalf("sent %d of %d reserved records, want the waited one topped up and the top-up cut", len(msgs), br.reserved())
	}
}

// On the local path (a single node, or a pinned partition this node
// owns), a failure of the wait maps as a single consume maps it, except
// that a queue consume whose partitions just moved answers 204.
func TestShipBatchLocalWaitErrors(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		err         error
		status      int
	}{
		{"queue, ownership moved", "?max=5&wait=1s", errs.ErrNotPartitionOwner, http.StatusNoContent},
		{"pinned, ownership moved", "?max=5&partition=2&wait=1s", errs.ErrNotPartitionOwner, http.StatusMisdirectedRequest},
		{"topic gone", "?max=5&wait=1s", errs.ErrTopicNotFound, http.StatusNotFound},
	} {
		br := &zzWP12BatchBroker{
			fakeBroker: &fakeBroker{},
			batch:      zzWP12Records(),
			wait:       func(time.Duration) (topic.Message, bool, error) { return topic.Message{}, false, tc.err },
		}
		res := zzWP12Consume(newTestSet(br, nil), tc.query)
		if res.Code != tc.status {
			t.Fatalf("%s: status %d (%s), want %d", tc.name, res.Code, res.Body, tc.status)
		}
		if calls := br.seen(); len(calls) != 1 {
			t.Fatalf("%s: calls = %+v, want no top-up after a failed wait", tc.name, calls)
		}
	}
}

// On a node that owns some partitions, a probe that fails outright maps
// as a single consume's failure and asks no remote owner, while a probe
// whose partitions just moved goes on to the remote owners.
func TestShipBatchLocalOwnerProbeErrors(t *testing.T) {
	remote := zzWP12Msg(0, 9)
	pick := 1
	remoteAsked := 0
	router := &fakeRouter{
		routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
			return false, &pick
		},
		routeConsumeRemote: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string) (bool, bool) {
			remoteAsked++
			zzShipWriteRecord(w, remote)
			return true, true
		},
	}
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) {
		return nil, errs.ErrTopicNotFound
	}}
	if res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s"); res.Code != http.StatusNotFound {
		t.Fatalf("failed probe = %d %q, want 404", res.Code, res.Body)
	}
	if remoteAsked != 0 {
		t.Fatal("remote owners asked after the local probe failed")
	}

	br.batch = func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) { return nil, errs.ErrNotPartitionOwner }
	if res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s"); res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(remote) {
		t.Fatalf("probe that lost its partitions = %d %q, want the remote record %q", res.Code, res.Body, zzWP12Envelope(remote))
	}
	if remoteAsked != 1 {
		t.Fatalf("remote owners asked %d times, want once", remoteAsked)
	}
}

// A top-up that fails, or has no room, only ends the top-up: the record
// the wait delivered is reserved and still goes out.
func TestShipBatchTopUpEarlyExits(t *testing.T) {
	a := zzWP12Msg(0, 1)
	br := &zzWP12BatchBroker{
		fakeBroker: &fakeBroker{},
		batch: func(call int, _ brokermsg.ConsumeOpts, _ int) ([]topic.Message, error) {
			if call == 0 {
				return nil, nil
			}
			return nil, errors.New("scan failed")
		},
		wait: func(time.Duration) (topic.Message, bool, error) { return a, true, nil },
	}
	res := zzWP12Consume(newTestSet(br, nil), "?max=5&wait=1s")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(a) {
		t.Fatalf("failed top-up = %d %q, want the waited record alone %q", res.Code, res.Body, zzWP12Envelope(a))
	}
	if calls := br.seen(); len(calls) != 2 {
		t.Fatalf("calls = %+v, want the probe and the failed top-up", calls)
	}

	br = &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records(), wait: br.wait}
	res = zzWP12Consume(newTestSet(br, nil), "?max=1&wait=1s")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(a) {
		t.Fatalf("batch of one = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(a))
	}
	if calls := br.seen(); len(calls) != 1 {
		t.Fatalf("calls = %+v, want no top-up for a batch of one", calls)
	}
}

// Records left out of a response are given back one by one, under a
// context the client's departure does not cancel: a handle that does not
// decode is skipped, a stale one is expected and not logged, and any
// other Nack failure is logged and does not stop the rest.
func TestShipBatchReleaseGivesBackEachRecord(t *testing.T) {
	var mu sync.Mutex
	var nacked []int64
	var cancelled bool
	br := &fakeBroker{nackFn: func(ctx context.Context, topicName string, h consumer.Handle) error {
		mu.Lock()
		defer mu.Unlock()
		nacked = append(nacked, h.Nonce)
		cancelled = cancelled || ctx.Err() != nil || topicName != "orders"
		switch h.Nonce {
		case 2:
			return fmt.Errorf("%w: reservation lapsed", consumer.ErrHandleStale)
		case 3:
			return errors.New("disk full")
		}
		return nil
	}}
	var logged bytes.Buffer
	s := handlers.New(handlers.Deps{Broker: br, Logger: slog.New(slog.NewTextHandler(&logged, nil))})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=5", nil).WithContext(ctx)
	b := &batchConsume{s: s, r: req, topic: "orders"}

	msgs := []topic.Message{
		{ReceiptHandle: "not-a-handle"},
		{ReceiptHandle: zzWP12Handle(0, 1, 2)},
		{ReceiptHandle: zzWP12Handle(0, 2, 3)},
		{ReceiptHandle: zzWP12Handle(0, 3, 4)},
	}
	b.release(msgs)

	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(nacked) != "[2 3 4]" || cancelled {
		t.Fatalf("nacked %v (cancelled or wrong topic: %v), want 2 3 4 under a live context", nacked, cancelled)
	}
	out := logged.String()
	if strings.Count(out, "release a record left out of a batch consume") != 1 || !strings.Contains(out, "disk full") ||
		strings.Contains(out, "reservation lapsed") {
		t.Fatalf("log = %q, want one warning, for the failed Nack only", out)
	}
}
