package cluster

// Every stall of a remote link writes one log line when the cursor
// enters it, whichever path stalled it (an answer that closes the gate,
// a stall answer, a failed lookup), at error level when only a person's
// fix clears it, and not again on each retry.

import (
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
	rg := newRemoteRig(t, remoteRigOpts{grant: []string{"other"},
		rigSourceOpts: rigSourceOpts{stallRetry: 200 * time.Millisecond}})
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
