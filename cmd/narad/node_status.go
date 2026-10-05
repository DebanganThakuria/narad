package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// nodeStatusPeer is the peer client call the controller and the cluster
// views ask a member's status with; *cluster.PeerClient implements it.
type nodeStatusPeer interface {
	NodeStatus(ctx context.Context, addr string) (nodewire.NodeStatus, error)
}

// controllerConfig is the controller's configuration in serve: its
// logger, the process registry for its leader-only metrics, and the
// node status call its decommission pass reads a draining node's
// dispatch backlog with. A 3.0.x node's "unsupported" answer becomes
// controller.ErrNodeStatusUnsupported, so it is removed without the
// check, as 3.0.x did.
func controllerConfig(log *slog.Logger, reg prometheus.Registerer, peer nodeStatusPeer) controller.Config {
	return controller.Config{
		Logger:     log.With("component", "controller"),
		Registerer: reg,
		NodeStatus: func(ctx context.Context, addr string) (controller.NodeStatus, error) {
			st, err := peer.NodeStatus(ctx, addr)
			if errors.Is(err, cluster.ErrNodeStatusUnsupported) {
				return controller.NodeStatus{}, controller.ErrNodeStatusUnsupported
			}
			if err != nil {
				return controller.NodeStatus{}, err
			}
			return controller.NodeStatus{
				Draining:        st.Draining,
				ProduceInFlight: st.ProduceInFlight,
				DispatchBacklog: st.DispatchBacklog,
			}, nil
		},
	}
}

// localNodeStatus builds this node's answer to OpNodeStatus from the
// components that own each field. Every source is a cheap read: the
// quarantine list is the inventory the reclaim sweep last took, never a
// fresh walk. The drain flag, the produce requests in flight and the
// dispatch backlog are read in that order (see handlers.DrainGate.Admit),
// so a draining node that reports none in flight and no backlog has
// handed off everything it accepted.
func localNodeStatus(
	nodeID string,
	drain *handlers.DrainGate,
	backlog func() uint64,
	quarantine func() (runtime.QuarantineSummary, bool),
	moves func() []cluster.MoveState,
) func(context.Context) nodewire.NodeStatus {
	return func(context.Context) nodewire.NodeStatus {
		st := nodewire.NodeStatus{Node: nodeID, Moves: []nodewire.MoveState{}}
		st.Draining = drain.Draining()
		st.ProduceInFlight = drain.InFlight()
		if backlog != nil {
			st.DispatchBacklog = backlog()
		}
		st.Quarantine.List = []nodewire.QuarantinedCopy{}
		if quarantine != nil {
			if sum, ok := quarantine(); ok {
				st.Quarantine.Copies, st.Quarantine.Bytes = sum.Count, sum.Bytes
				for i, q := range sum.Copies {
					if i == nodewire.MaxStatusQuarantineList {
						break
					}
					st.Quarantine.List = append(st.Quarantine.List, nodewire.QuarantinedCopy{
						Kind: q.Kind, Topic: q.Topic, Partition: q.Partition, Dir: q.Dir, Bytes: q.Bytes, ModTime: q.ModTime,
					})
				}
			}
		}
		if moves != nil {
			st.Moves = append(st.Moves, moves()...)
		}
		return st
	}
}

// memberNodeStatus answers the cluster views' per-member status: this
// node's own from local, any other member's over node RPC.
func memberNodeStatus(nodeID string, local func(context.Context) nodewire.NodeStatus, peer nodeStatusPeer) func(context.Context, metastore.Member) (nodewire.NodeStatus, error) {
	return func(ctx context.Context, m metastore.Member) (nodewire.NodeStatus, error) {
		if m.ID == nodeID {
			return local(ctx), nil
		}
		if strings.TrimSpace(m.Addr) == "" {
			return nodewire.NodeStatus{}, errors.New("no node RPC address on record")
		}
		return peer.NodeStatus(ctx, m.Addr)
	}
}

// drainingSource is the replica view watchDraining reads.
type drainingSource interface {
	GetMember(id string) (metastore.Member, error)
	RoutingMembersVersion() uint64
}

// drainingCheckInterval is how often watchDraining looks at the routing
// members version, and drainingRefreshInterval how often it re-reads the
// member record whatever the version says (a drain flip does not move
// the routing version).
const (
	drainingCheckInterval   = 250 * time.Millisecond
	drainingRefreshInterval = time.Second
)

// watchDraining keeps the drain gate's flag equal to this node's own
// member record's Draining in the local replica until ctx ends, so the
// produce handlers refuse client produce while the node is being
// decommissioned. A failed read keeps the last value.
func watchDraining(ctx context.Context, src drainingSource, nodeID string, drain *handlers.DrainGate, check, refresh time.Duration) {
	read := func() {
		if m, err := src.GetMember(nodeID); err == nil {
			drain.SetDraining(m.Draining)
		}
	}
	read()
	version := src.RoutingMembersVersion()
	last := time.Now()
	ticker := time.NewTicker(check)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if v := src.RoutingMembersVersion(); v != version || now.Sub(last) >= refresh {
				version, last = v, now
				read()
			}
		}
	}
}
