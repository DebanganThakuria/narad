package cluster

// Every stall of a remote link writes one log line when the cursor
// enters it, whichever path stalled it (an answer that closes the gate,
// a stall answer, a failed lookup), at error level when only a person's
// fix clears it, and not again on each retry.

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// stalledLines returns the "remote child stalled" records whose state
// attribute is state.
func (l *recordedLog) stalledLines(state string) []slog.Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []slog.Record
	for _, r := range l.records {
		if r.Message != "remote child stalled" {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "state" && a.Value.String() == state {
				out = append(out, r)
				return false
			}
			return true
		})
	}
	return out
}

func attrOf(r slog.Record, key string) string {
	var v string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value.String()
			return false
		}
		return true
	})
	return v
}

func TestRemoteChildAuthFailedIsLoggedOnceAsAnError(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{password: "a-wrong-password-0123456789"})
	logs := &recordedLog{}
	rg.src.runner.logger = slog.New(logs)
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 10, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateAuthFailed, 15*time.Second)
	rigWait(t, "the auth_failed log line", 5*time.Second, func() bool {
		return len(logs.stalledLines(topic.RemoteStateAuthFailed)) > 0
	})
	time.Sleep(time.Second)
	lines := logs.stalledLines(topic.RemoteStateAuthFailed)
	if len(lines) != 1 {
		t.Fatalf("%d auth_failed lines, want one", len(lines))
	}
	if r := lines[0]; r.Level != slog.LevelError || attrOf(r, "remote") != "b" || attrOf(r, "child") != "orders-to-b" {
		t.Fatalf("logged %s with remote=%q child=%q, want an error naming remote b and child orders-to-b",
			r.Level, attrOf(r, "remote"), attrOf(r, "child"))
	}
}

func TestRemoteChildStallIsLoggedOncePerStateNotPerRetry(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{
		grant:         []string{"other"},
		rigSourceOpts: rigSourceOpts{stallRetry: 200 * time.Millisecond},
	})
	logs := &recordedLog{}
	rg.src.runner.logger = slog.New(logs)
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 10, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateForbidden, 15*time.Second)
	time.Sleep(2 * time.Second)
	if n := len(logs.stalledLines(topic.RemoteStateForbidden)); n != 1 {
		t.Fatalf("%d forbidden lines over about ten stall retries, want one", n)
	}

	// The remote deleted with --force: the lookup fails, which says so too.
	rg.src.lookup.Delete("b")
	rg.waitState(t, 0, topic.RemoteStateRemoteMissing, 10*time.Second)
	rigWait(t, "the remote_missing log line", 5*time.Second, func() bool {
		lines := logs.stalledLines(topic.RemoteStateRemoteMissing)
		return len(lines) == 1 && lines[0].Level == slog.LevelError
	})
}

// A lane blocked on a record the target refuses retries it every stall
// interval; the refusal is logged once, when the lane first blocks on
// that record, not on every retry.
func TestRemoteChildBlockedRecordIsLoggedOncePerRecord(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 200 * time.Millisecond}})
	schema := []byte(`{"type":"object","required":["seq"],"properties":{"seq":{"type":"integer"}}}`)
	if _, err := rg.target.broker.UpdateTopicSchema(context.Background(), "orders", schema, 0); err != nil {
		t.Fatal(err)
	}
	logs := &recordedLog{}
	rg.src.runner.logger = slog.New(logs)
	rg.src.start()
	defer rg.src.stop()
	rg.src.producePayload(t, 0, "bad", []byte(`{"not_seq":true}`))
	rg.waitState(t, 0, topic.RemoteStateRejectedRecord, 20*time.Second)
	time.Sleep(2 * time.Second) // about ten retries
	if n := logs.count(slog.LevelWarn, "remote child blocked on a record the target refuses"); n != 1 {
		t.Fatalf("%d blocked-record lines over about ten retries of one record, want one", n)
	}
}

// A cursor that cannot hold its records re-reads its slab after every
// wait; it says so once per remoteStallLogGap, not on every pass, while
// the re-read counter keeps counting each one.
func TestRemoteChildRereadIsLoggedOncePerGap(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{heldBudget: -1}})
	rg.target.faults.set("down")
	logs := &recordedLog{}
	rg.src.runner.logger = slog.New(logs)
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 10, 2, 0)
	rereads := rg.src.metrics.RemoteLink.RereadsTotal.WithLabelValues("orders", "orders-to-b")
	rigWait(t, "three re-reads", 20*time.Second, func() bool { return counterValue(rereads) >= 3 })
	const msg = "remote child could not hold a waiting lane's records (remotes.max_held_bytes is full): it reads them again after the wait and sends only what the target does not have yet"
	if n := logs.count(slog.LevelError, msg); n != 1 {
		t.Fatalf("%d re-read lines over %v re-reads, want one", n, counterValue(rereads))
	}
}
