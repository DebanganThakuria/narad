package messaging

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// The cluster router implements the local-owner batch forms the handler
// asserts for; a signature drift would otherwise silently drop a
// local-owner batch consume back to one remote record. The test router
// below implements them too, so its tests exercise the batch path.
var (
	_ localOwnerBatchRouter = (*cluster.Router)(nil)
	_ localOwnerBatchRouter = (*zzPerfCRouter)(nil)
)

// zzPerfCRouter is a router for a node that owns some of the topic's
// partitions (RouteConsume declines with the local pick) and has the
// batch forms of the remote probe and the token wait. remote and wait
// write what the owner or the local waiter would; each call is counted
// with the max it asked for. The single-record forms fail the test.
type zzPerfCRouter struct {
	*fakeRouter
	t      *testing.T
	remote func(w http.ResponseWriter) (forwarded, batch bool)
	wait   func(w http.ResponseWriter, local handlers.LocalConsumeWaiter) (handled, batch bool)

	remoteMaxes []int
	waitMaxes   []int
	waitBudget  time.Duration
}

func newZZPerfCRouter(t *testing.T, pick *int) *zzPerfCRouter {
	r := &zzPerfCRouter{t: t}
	r.fakeRouter = &fakeRouter{
		routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
			return false, pick
		},
		routeConsumeRemote: func(context.Context, http.ResponseWriter, *http.Request, string) (bool, bool) {
			t.Error("the single-record remote probe ran on a router with the batch form")
			return false, false
		},
	}
	r.remote = func(http.ResponseWriter) (bool, bool) { return false, false }
	r.wait = func(http.ResponseWriter, handlers.LocalConsumeWaiter) (bool, bool) { return false, false }
	return r
}

func (r *zzPerfCRouter) RouteConsumeRemoteBatch(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, max int) (bool, bool) {
	r.remoteMaxes = append(r.remoteMaxes, max)
	return r.remote(w)
}

func (r *zzPerfCRouter) RouteConsumeWaitBatch(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, wait time.Duration, local handlers.LocalConsumeWaiter, max int) (bool, bool) {
	r.waitMaxes = append(r.waitMaxes, max)
	r.waitBudget = wait
	return r.wait(w, local)
}

func (r *zzPerfCRouter) RouteConsumeWait(context.Context, http.ResponseWriter, *http.Request, string, time.Duration, handlers.LocalConsumeWaiter) bool {
	r.t.Error("the single-record wait ran on a router with the batch form")
	return false
}

// zzPerfCWriteBody writes body as an owner's reply the router passes on.
func zzPerfCWriteBody(w http.ResponseWriter, status int, contentType, body string) {
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(status)
	if body != "" {
		_, _ = w.Write([]byte(body))
	}
}

// zzPerfCEmptyProbe is a broker whose partitions are empty: the probe
// finds nothing and hands back a waiter; any later scan (a top-up)
// finds more, if given.
func zzPerfCEmptyProbe(more ...topic.Message) *zzWP12BatchBroker {
	return &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: func(call int, _ brokermsg.ConsumeOpts, _ int) ([]topic.Message, error) {
		if call == 0 {
			return nil, nil
		}
		return more, nil
	}}
}

// On a node that owns some of the topic's partitions, all empty, a batch
// consume asks the other owners for up to max through the router's batch
// probe. What the router wrote goes out as follows: an owner's batch
// exactly as it is, a single record (an owner too old to take max) as a
// one-message batch, and anything else unchanged. No local top-up runs
// after a forwarded answer.
func TestPerfCLocalOwnerBatchProbeUsesBatchRouter(t *testing.T) {
	one, two := zzWP12Msg(1, 1), zzWP12Msg(1, 2)
	ownerBatch := zzWP12Envelope(one, two)
	pick := 0
	for _, tc := range []struct {
		name   string
		batch  bool
		write  func(w http.ResponseWriter)
		status int
		body   string
		ct     string
	}{
		{"owner batch", true, func(w http.ResponseWriter) {
			zzPerfCWriteBody(w, http.StatusOK, "application/json", ownerBatch)
		}, http.StatusOK, ownerBatch, "application/json"},
		{"legacy owner's single record", false, func(w http.ResponseWriter) {
			zzShipWriteRecord(w, one)
		}, http.StatusOK, zzWP12Envelope(one), "application/json"},
		{"owner down", true, func(w http.ResponseWriter) {
			http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
		}, http.StatusServiceUnavailable, "partition owner is down; retry later\n", "text/plain; charset=utf-8"},
	} {
		br := zzPerfCEmptyProbe(zzWP12Msg(0, 5))
		router := newZZPerfCRouter(t, &pick)
		router.remote = func(w http.ResponseWriter) (bool, bool) {
			tc.write(w)
			return true, tc.batch
		}
		res := zzWP12Consume(newTestSet(br, router), "?max=7&wait=1s")
		if res.Code != tc.status || res.Body.String() != tc.body || res.Header().Get("Content-Type") != tc.ct {
			t.Fatalf("%s: %d %q %q, want %d %q %q", tc.name, res.Code, res.Header().Get("Content-Type"), res.Body, tc.status, tc.ct, tc.body)
		}
		if tc.status == http.StatusOK && res.Header().Get("Content-Length") != strconv.Itoa(len(tc.body)) {
			t.Fatalf("%s: Content-Length %q, want %d", tc.name, res.Header().Get("Content-Length"), len(tc.body))
		}
		if len(router.remoteMaxes) != 1 || router.remoteMaxes[0] != 7 || len(router.waitMaxes) != 0 {
			t.Fatalf("%s: batch probes %v, batch waits %v; want one probe for 7 and no wait", tc.name, router.remoteMaxes, router.waitMaxes)
		}
		if calls := br.seen(); len(calls) != 1 {
			t.Fatalf("%s: calls = %+v, want the local probe alone", tc.name, calls)
		}
	}
}

// When the other owners have nothing, the wait goes through the router's
// batch wait with the request's max and wait. An owner's batch claimed
// there goes out as it is, with no local top-up; a wait handled without
// a record goes out unchanged.
func TestPerfCLocalOwnerBatchWaitUsesBatchRouter(t *testing.T) {
	one, two, three := zzWP12Msg(1, 1), zzWP12Msg(1, 2), zzWP12Msg(1, 3)
	ownerBatch := zzWP12Envelope(one, two, three)
	pick := 2
	for _, tc := range []struct {
		name   string
		batch  bool
		write  func(w http.ResponseWriter)
		status int
		body   string
	}{
		{"owner batch", true, func(w http.ResponseWriter) {
			zzPerfCWriteBody(w, http.StatusOK, "application/json", ownerBatch)
		}, http.StatusOK, ownerBatch},
		{"nothing", false, func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }, http.StatusNoContent, ""},
		{"failure", false, func(w http.ResponseWriter) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}, http.StatusInternalServerError, "boom\n"},
	} {
		br := zzPerfCEmptyProbe(zzWP12Msg(2, 9))
		router := newZZPerfCRouter(t, &pick)
		router.wait = func(w http.ResponseWriter, local handlers.LocalConsumeWaiter) (bool, bool) {
			if local == nil {
				t.Errorf("%s: the batch wait got no local waiter to race", tc.name)
			}
			tc.write(w)
			return true, tc.batch
		}
		res := zzWP12Consume(newTestSet(br, router), "?max=6&wait=1s")
		if res.Code != tc.status || res.Body.String() != tc.body {
			t.Fatalf("%s: %d %q, want %d %q", tc.name, res.Code, res.Body, tc.status, tc.body)
		}
		if len(router.remoteMaxes) != 1 || router.remoteMaxes[0] != 6 || len(router.waitMaxes) != 1 || router.waitMaxes[0] != 6 {
			t.Fatalf("%s: batch probes %v, batch waits %v; want one of each for 6", tc.name, router.remoteMaxes, router.waitMaxes)
		}
		if router.waitBudget != time.Second {
			t.Fatalf("%s: batch wait budget %v, want the request's 1s", tc.name, router.waitBudget)
		}
		if calls := br.seen(); len(calls) != 1 {
			t.Fatalf("%s: calls = %+v, want the local probe alone", tc.name, calls)
		}
	}
}

// When the local waiter wins the batch wait's race, the router writes
// its one record with batch false. The handler tops that record up with
// one non-blocking scan of the local partitions from the router's pick,
// as it did before the batch wait existed.
func TestPerfCLocalOwnerBatchLocalWaiterWinsTopsUp(t *testing.T) {
	waited, more := zzWP12Msg(2, 4), zzWP12Msg(2, 5)
	pick := 2
	br := zzPerfCEmptyProbe(more)
	br.wait = func(time.Duration) (topic.Message, bool, error) { return waited, true, nil }
	router := newZZPerfCRouter(t, &pick)
	router.wait = func(w http.ResponseWriter, local handlers.LocalConsumeWaiter) (bool, bool) {
		msg, found, _, err := local.Wait(context.Background(), time.Second, nil)
		if err != nil || !found {
			t.Errorf("local wait = %v %v, want the local record", found, err)
			return false, false
		}
		zzShipWriteRecord(w, msg)
		return true, false
	}
	res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(waited, more) {
		t.Fatalf("batch = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(waited, more))
	}
	calls := br.seen()
	if len(calls) != 2 {
		t.Fatalf("calls = %+v, want the probe and one top-up", calls)
	}
	top := calls[1]
	firstLen := len(waited.AppendJSON(nil))
	if top.max != 4 || top.held != 0 || top.opts.Wait != 0 || top.opts.Partition != nil ||
		top.opts.ScanStart == nil || *top.opts.ScanStart != pick || top.opts.MaxBytes != consumeBatchReserveBytes-firstLen {
		t.Fatalf("top-up = %+v; want a non-blocking scan of 4 from partition %d with %d bytes left", top, pick, consumeBatchReserveBytes-firstLen)
	}
}

// A router without the batch forms keeps the single-record calls: one
// remote probe, whose record goes out as a one-message batch, or, when
// it finds nothing, one raced wait.
func TestPerfCLocalOwnerBatchOldRouterKeepsSingleCalls(t *testing.T) {
	remote := zzWP12Msg(1, 8)
	pick := 0

	br := zzPerfCEmptyProbe()
	router := newZZShipWaitRouter(&pick, func(http.ResponseWriter) bool {
		t.Error("the wait ran after the remote probe delivered")
		return false
	})
	probes := 0
	router.routeConsumeRemote = func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string) (bool, bool) {
		probes++
		zzShipWriteRecord(w, remote)
		return true, true
	}
	res := zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(remote) || probes != 1 || router.waits != 0 {
		t.Fatalf("old router, remote record: %d %q after %d probes and %d waits, want %q from one probe", res.Code, res.Body, probes, router.waits, zzWP12Envelope(remote))
	}

	br = zzPerfCEmptyProbe()
	router = newZZShipWaitRouter(&pick, func(w http.ResponseWriter) bool {
		w.WriteHeader(http.StatusNoContent)
		return true
	})
	probes = 0
	router.routeConsumeRemote = func(context.Context, http.ResponseWriter, *http.Request, string) (bool, bool) {
		probes++
		return false, true
	}
	res = zzWP12Consume(newTestSet(br, router), "?max=5&wait=1s")
	if res.Code != http.StatusNoContent || probes != 1 || router.waits != 1 {
		t.Fatalf("old router, nothing: %d after %d probes and %d waits, want 204 from one probe and one wait", res.Code, probes, router.waits)
	}
}
