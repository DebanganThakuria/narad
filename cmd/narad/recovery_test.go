package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type fakeStoreRepairer struct {
	failures atomic.Int32
	calls    atomic.Int32
}

func (f *fakeStoreRepairer) RepairOwnedPartitions(context.Context) error {
	f.calls.Add(1)
	if f.failures.Add(-1) >= 0 {
		return errors.New("temporary repair failure")
	}
	return nil
}

func TestRunStoreRecoveryRepairerRetriesUntilSuccess(t *testing.T) {
	repairer := &fakeStoreRepairer{}
	repairer.failures.Store(2)

	runStoreRecoveryRepairer(context.Background(), repairer, time.Millisecond, discardSlog())

	if got := repairer.calls.Load(); got != 3 {
		t.Fatalf("RepairOwnedPartitions calls = %d, want 3", got)
	}
}

func TestRunStoreRecoveryRepairerStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repairer := storeRepairerFunc(func(context.Context) error {
		cancel()
		return errors.New("temporary repair failure")
	})

	runStoreRecoveryRepairer(ctx, repairer, time.Hour, discardSlog())
}

type storeRepairerFunc func(context.Context) error

func (f storeRepairerFunc) RepairOwnedPartitions(ctx context.Context) error {
	return f(ctx)
}

func discardSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
