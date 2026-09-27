package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP7bGetWithin runs Get(topicName, idx) and reports whether it
// returned within d; the Get keeps running past d either way.
func zzWP7bGetWithin(t *testing.T, g *Logs, topicName string, idx int, d time.Duration) (done <-chan error, inTime bool) {
	t.Helper()
	ch := make(chan error, 1)
	go func() {
		_, err := g.Get(topicName, idx)
		ch <- err
	}()
	select {
	case err := <-ch:
		ch <- err
		return ch, true
	case <-time.After(d):
		return ch, false
	}
}

// The Get slow path used to hold the node-wide log map lock across the
// whole open: the metastore lookup, the incarnation marker I/O and
// storage.NewLog, whose recovery reads every retained segment (about
// 400 ms per GiB on a warm cache). Every other Get on the node waited
// that long. The open now holds only the topic's guard.
func TestZZWP7bSlowOpenDoesNotStallOtherTopics(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "slow", ID: "00000000000000a5", Partitions: 1})
	ms.put(topic.Topic{Name: "b", ID: "000000000000000b", Partitions: 1})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	if _, err := g.Get("b", 0); err != nil {
		t.Fatalf("Get(b): %v", err)
	}

	gate := make(chan struct{})
	in := make(chan string, 1)
	ms.mu.Lock()
	ms.getTopicGate["slow"] = gate
	ms.getTopicIn = in
	ms.mu.Unlock()
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
		}
	}
	defer release()

	slowDone := make(chan error, 1)
	go func() {
		_, err := g.Get("slow", 0)
		slowDone <- err
	}()
	select {
	case <-in:
	case <-time.After(5 * time.Second):
		t.Fatal("Get(slow) never reached the metastore")
	}

	done, inTime := zzWP7bGetWithin(t, g, "b", 0, 2*time.Second)
	release()
	if err := <-done; err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if !inTime {
		t.Fatal("Get(b) waited for another topic's open to finish its metastore lookup")
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("Get(slow): %v", err)
	}
}

// Shutdown's CloseAll must still close a log whose open was in progress
// when it started: the open no longer holds the log map lock, so
// CloseAll finds it through the topic guard and waits for it.
func TestZZWP7bCloseAllClosesAnOpenInProgress(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "slow", ID: "00000000000000a5", Partitions: 1})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)

	gate := make(chan struct{})
	in := make(chan string, 1)
	ms.mu.Lock()
	ms.getTopicGate["slow"] = gate
	ms.getTopicIn = in
	ms.mu.Unlock()

	type opened struct {
		l   *storage.Log
		err error
	}
	slowDone := make(chan opened, 1)
	go func() {
		l, err := g.Get("slow", 0)
		slowDone <- opened{l, err}
	}()
	select {
	case <-in:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("Get(slow) never reached the metastore")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- g.CloseAll() }()
	select {
	case err := <-closeDone:
		close(gate)
		t.Fatalf("CloseAll returned (%v) while an open was in progress", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
	o := <-slowDone
	if o.err != nil {
		t.Fatalf("Get(slow): %v", o.err)
	}
	if _, ok := g.Peek("slow", 0); ok {
		t.Fatal("slow/0 is still open after CloseAll")
	}
	if _, err := o.l.Append([]byte("late")); !errors.Is(err, storage.ErrLogClosed) {
		t.Fatalf("Append on the log opened during CloseAll: %v, want ErrLogClosed", err)
	}
}
