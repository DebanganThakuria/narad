package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeHaltingStore struct {
	halted chan struct{}
	err    error
}

func (f *fakeHaltingStore) Halted() <-chan struct{} { return f.halted }
func (f *fakeHaltingStore) HaltErr() error          { return f.err }

func TestMetastoreStopFailsServe(t *testing.T) {
	stopErr := errors.New("metastore: stopped applying raft entries at index 7")
	store := &fakeHaltingStore{halted: make(chan struct{}), err: stopErr}
	failed := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchMetastoreHalt(context.Background(), store, func(err error) { failed <- err })
	}()

	close(store.halted)
	select {
	case err := <-failed:
		if !errors.Is(err, stopErr) {
			t.Fatalf("serve failed with %v, want the metastore's stop error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a stopped metastore did not fail serve")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch did not return after failing serve")
	}
}

func TestMetastoreWatchEndsWithServe(t *testing.T) {
	store := &fakeHaltingStore{halted: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchMetastoreHalt(ctx, store, func(err error) { t.Errorf("serve failed with %v on a clean shutdown", err) })
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch outlived serve")
	}
}
