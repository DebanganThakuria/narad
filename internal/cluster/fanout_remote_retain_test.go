package cluster

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
)

// unreadableLookup answers credential_unreadable for every remote once
// told to, without holding any entry itself.
type unreadableLookup struct {
	remote.Lookup
	unreadable atomic.Bool
}

func (u *unreadableLookup) Get(name string) (*remote.Entry, error) {
	if u.unreadable.Load() {
		return nil, remote.ErrCredentialUnreadable
	}
	return u.Lookup.Get(name)
}

// A remote whose credential no longer opens drops its entry, header and
// client included; the sender must not keep the last one reachable
// while its links wait in credential_unreadable.
func TestRemoteSenderLetsGoOfAnEntryItCanNoLongerUse(t *testing.T) {
	limits := domremote.DefaultLimits()
	limits.CheckIntervalMs = 1000
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: 1}, limits: limits})
	lk := &unreadableLookup{Lookup: rg.src.lookup}
	rg.src.runner.SetRemotes(lk, 64<<20)
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 5, 2, 0), 15*time.Second)
	rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)

	collected := make(chan struct{})
	func() {
		e := entryFor(t, rg.target, "b", rigReplPass, 2, limits, nil)
		runtime.AddCleanup(e, func(ch chan struct{}) { close(ch) }, collected)
		since := time.Now().UnixMilli()
		rg.src.lookup.Set(e)
		rigWait(t, "a target check with the new entry", 10*time.Second, func() bool {
			snap, ok := rg.src.cursorState(0)
			return ok && snap.verifiedMs > since
		})
	}()
	lk.unreadable.Store(true)
	rg.src.lookup.Delete("b")
	rg.waitState(t, 0, topic.RemoteStateCredentialUnreadable, 10*time.Second)

	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("the sender still holds the entry of a remote whose credential no longer opens")
		}
	}
}
