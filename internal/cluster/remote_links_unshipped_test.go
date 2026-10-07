package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A peer whose replica lags a partition swap reports a partition it no
// longer owns and omits the one it now owns. The unshipped check counts
// each partition once, from the owner the leader's assignments name, so
// the partition nobody reported keeps the lag incomplete and a delete
// without --force is refused instead of abandoning its records.
func TestUnshippedCheckCountsEachPartitionOnceFromItsOwner(t *testing.T) {
	s := linksRig(t)
	ctx := context.Background()
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != http.StatusCreated {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	s.produce(t, 0, 2, 2, 0)
	s.produce(t, 1, 3, 2, 100)
	// Anchor both cursors, then stop the runner: the check reads files.
	s.start()
	rigWait(t, "both cursors anchored", 10*time.Second, func() bool {
		stats, err := s.broker.FanoutCursorStats(ctx, "orders")
		return err == nil && len(stats) == 2
	})
	s.stop()
	if err := s.store.RegisterMember(ctx, metastore.Member{ID: "node-y", Addr: "127.0.0.1:2", Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	// The leader now names node-y the owner of partition 1 and keeps 0.
	if err := s.store.AssignPartition(ctx, "orders", 1, "node-y"); err != nil {
		t.Fatal(err)
	}
	// node-y has not applied the swap: it still reports partition 0 and
	// says nothing of partition 1, whose real lag is never read.
	s.runner.peer = fakePeerClient{fanoutCursorsFn: func(context.Context, string, string) ([]topic.FanoutCursorStat, error) {
		return []topic.FanoutCursorStat{{Child: "orders-to-b", Partition: 0, NextOffset: 0, HighWatermark: 0}}, nil
	}}
	parent, err := s.store.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	rigWait(t, "the leader's broker to drop partition 1", 5*time.Second, func() bool {
		stats, err := s.broker.FanoutCursorStats(ctx, "orders")
		if err != nil {
			return false
		}
		for _, st := range stats {
			if st.Partition == 1 {
				return false
			}
		}
		return len(stats) > 0
	})
	report, err := s.links.unshippedNow(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	if report.lagComplete["orders-to-b"] {
		t.Fatalf("lag complete with partition 1 unreported and partition 0 reported twice: %+v", report)
	}
	if !report.unshipped([]string{"orders-to-b"}) {
		t.Fatal("the check lets a delete abandon partition 1's records")
	}
	// Once node-y answers for what it owns, the check is whole again.
	s.runner.peer = fakePeerClient{fanoutCursorsFn: func(context.Context, string, string) ([]topic.FanoutCursorStat, error) {
		return []topic.FanoutCursorStat{
			{Child: "orders-to-b", Partition: 0, NextOffset: 0, HighWatermark: 0},
			{Child: "orders-to-b", Partition: 1, NextOffset: 3, HighWatermark: 3},
		}, nil
	}}
	report, err = s.links.unshippedNow(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	// Partition 0's two unsent records from the leader, none from node-y.
	if !report.lagComplete["orders-to-b"] || report.lag["orders-to-b"] != 2 {
		t.Fatalf("each partition answered once by its owner: %+v, want complete with lag 2", report)
	}
}
