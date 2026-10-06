package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// gatedStatsBroker holds every cursor-stats read until release closes.
type gatedStatsBroker struct {
	broker.Broker
	release chan struct{}
}

func (b gatedStatsBroker) FanoutCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, error) {
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.Broker.FanoutCursorStats(ctx, parent)
}

// The unshipped check a delete starts answers for every delete that
// shares it, whatever becomes of the delete that started it: its client
// going away ends only its own wait, never the check, so a concurrent
// delete is not refused with "the unshipped check could not run".
func TestUnshippedCheckOutlivesTheDeleteThatStartedIt(t *testing.T) {
	s := linksRig(t)
	release := make(chan struct{})
	s.links.d.Broker = gatedStatsBroker{Broker: s.broker, release: release}
	parent, err := s.store.GetTopic(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := s.links.unshippedChecked(ctx, parent)
		first <- err
	}()
	var f *unshippedFlight
	rigWait(t, "the check to start", 5*time.Second, func() bool {
		s.links.mu.Lock()
		defer s.links.mu.Unlock()
		f = s.links.flights["orders"]
		return f != nil
	})
	cancel()
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("the cancelled delete got an answer it never waited for")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled delete kept waiting")
	}
	close(release)
	select {
	case <-f.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the check never ended")
	}
	if f.err != nil {
		t.Fatalf("the shared check failed with its first caller's cancellation: %v", f.err)
	}
	if _, ok := f.report.backlog["node-self"]; !ok {
		t.Fatalf("report = %+v, want the members' backlogs", f.report)
	}
}
