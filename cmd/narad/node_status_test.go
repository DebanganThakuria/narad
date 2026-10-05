package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// scriptedStatusPeer answers NodeStatus with one fixed status or error,
// recording the addresses asked.
type scriptedStatusPeer struct {
	st    nodewire.NodeStatus
	err   error
	asked []string
}

func (p *scriptedStatusPeer) NodeStatus(_ context.Context, addr string) (nodewire.NodeStatus, error) {
	p.asked = append(p.asked, addr)
	return p.st, p.err
}

// serve gives the controller its logger, the process registry for its
// leader metrics, and a node status call that reads a draining node's
// dispatch backlog, turning a 3.0.x node's "unsupported" into the
// controller's own sentinel.
func TestServeGivesTheControllerALoggerRegistryAndNodeStatus(t *testing.T) {
	reg := prometheus.NewRegistry()
	peer := &scriptedStatusPeer{st: nodewire.NodeStatus{Node: "narad-2", Draining: true, ProduceInFlight: 2, DispatchBacklog: 7}}
	cfg := controllerConfig(slog.New(slog.NewTextHandler(io.Discard, nil)), reg, peer)

	if cfg.Logger == nil || cfg.Registerer != reg || cfg.NodeStatus == nil {
		t.Fatalf("controller config = %+v; want a logger, the process registry and a node status call", cfg)
	}
	st, err := cfg.NodeStatus(context.Background(), "narad-2:7942")
	if err != nil || st != (controller.NodeStatus{Draining: true, ProduceInFlight: 2, DispatchBacklog: 7}) || !slices.Equal(peer.asked, []string{"narad-2:7942"}) {
		t.Fatalf("NodeStatus = %+v, %v (asked %v); want draining, 2 in flight and a backlog of 7 from narad-2:7942", st, err, peer.asked)
	}
	peer.err = cluster.ErrNodeStatusUnsupported
	if _, err := cfg.NodeStatus(context.Background(), "old:7942"); !errors.Is(err, controller.ErrNodeStatusUnsupported) {
		t.Fatalf("NodeStatus from a 3.0.x node = %v, want controller.ErrNodeStatusUnsupported", err)
	}

	_ = controller.New(nil, cfg)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	for _, want := range []string{"narad_dead_marking_refused", "narad_colocated_child_partitions"} {
		if !slices.Contains(names, want) {
			t.Fatalf("registry has %v; want the controller's %s", names, want)
		}
	}
}

// This node answers OpNodeStatus from its own components, and the
// cluster views ask it locally and every other member over node RPC.
func TestLocalNodeStatusReportsEachComponent(t *testing.T) {
	drain := &handlers.DrainGate{}
	if !drain.Admit() {
		t.Fatal("the gate refused a produce before the drain")
	}
	drain.SetDraining(true)
	sum := runtime.QuarantineSummary{Count: 150, Bytes: 9000}
	for i := range 120 {
		sum.Copies = append(sum.Copies, runtime.QuarantinedCopy{Kind: "partition", Topic: "orders", Partition: i})
	}
	local := localNodeStatus("narad-1", drain, func() uint64 { return 3 },
		func() (runtime.QuarantineSummary, bool) { return sum, true },
		func() []cluster.MoveState {
			return []cluster.MoveState{{Topic: "orders", Partition: 4, Phase: cluster.MovePhaseCopying}}
		})

	st := local(context.Background())
	if st.Node != "narad-1" || !st.Draining || st.ProduceInFlight != 1 || st.DispatchBacklog != 3 {
		t.Fatalf("status = %+v; want narad-1 draining, 1 produce in flight and a backlog of 3", st)
	}
	if st.Quarantine.Copies != 150 || st.Quarantine.Bytes != 9000 || len(st.Quarantine.List) != nodewire.MaxStatusQuarantineList {
		t.Fatalf("quarantine = %d copies, %d bytes, %d listed; want 150, 9000, %d", st.Quarantine.Copies, st.Quarantine.Bytes, len(st.Quarantine.List), nodewire.MaxStatusQuarantineList)
	}
	if len(st.Moves) != 1 || st.Moves[0].Partition != 4 {
		t.Fatalf("moves = %+v", st.Moves)
	}

	peer := &scriptedStatusPeer{st: nodewire.NodeStatus{Node: "narad-2"}}
	ask := memberNodeStatus("narad-1", local, peer)
	if got, err := ask(context.Background(), metastore.Member{ID: "narad-1", Addr: "narad-1:7942"}); err != nil || got.Node != "narad-1" || len(peer.asked) != 0 {
		t.Fatalf("own status = %+v, %v (peer asked %v); want it answered locally", got, err, peer.asked)
	}
	if got, err := ask(context.Background(), metastore.Member{ID: "narad-2", Addr: "narad-2:7942"}); err != nil || got.Node != "narad-2" || !slices.Equal(peer.asked, []string{"narad-2:7942"}) {
		t.Fatalf("peer status = %+v, %v (asked %v); want narad-2 asked over node RPC", got, err, peer.asked)
	}
}

// The produce handlers' drain flag follows this node's own member
// record in the local replica, both ways.
func TestDrainingFlagFollowsTheMemberRecord(t *testing.T) {
	store, err := metastore.New(metastore.Config{NodeID: "narad-0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	waitForLeadership(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "narad-0", Addr: "127.0.0.1:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	flag := &handlers.DrainGate{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchDraining(ctx, store, "narad-0", flag, 10*time.Millisecond, 50*time.Millisecond)
	}()

	for _, want := range []bool{true, false} {
		if err := store.SetMemberDraining(ctx, "narad-0", want); err != nil {
			t.Fatalf("SetMemberDraining(%v): %v", want, err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for flag.Draining() != want {
			if time.Now().After(deadline) {
				t.Fatalf("drain flag stayed %v after the record changed to %v", !want, want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	<-done
}
