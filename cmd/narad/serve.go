package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/logger"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// runServe boots a Narad node: config, observability, metastore, broker,
// cluster plumbing, background workers, and finally the HTTP API server.
// Construction order and the defer stack are load-bearing; read the inline
// comments before reordering anything.
func runServe(args []string) error {
	bootStart := time.Now()

	cfg, err := loadServeConfig(args)
	if err != nil || cfg == nil {
		return err
	}

	log, err := logger.New(cfg.Log.Format, cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	reg, m := buildMetrics()

	if err = os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}

	nodeID, err := resolveNodeID(cfg)
	if err != nil {
		return err
	}
	clusterTLS, err := clusterTLSConfig(cfg.Security)
	if err != nil {
		return fmt.Errorf("cluster tls: %w", err)
	}
	if clusterTLS != nil {
		log.Info("raft metadata transport secured with mutual TLS")
	} else {
		log.Warn("raft metadata transport is plaintext; restrict the cluster port by network policy")
	}
	joinOnly := joinOnlyNode(nodeID, cfg.Cluster.InitialMembers)
	if joinOnly {
		log.Info("node is not an initial member; will join the existing cluster instead of bootstrapping", "node", nodeID)
	}
	metastoreDir := filepath.Join(cfg.Storage.DataDir, "metastore")
	hasState, err := metastore.HasExistingState(metastoreDir)
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	fresh := !hasState
	if !joinOnly && fresh && len(cfg.Cluster.Peers) > 0 {
		// An initial member with NO Raft state is either part of a
		// brand-new install or a member whose volume was replaced while
		// the cluster kept running. Bootstrapping in the second case
		// seeds a rival configuration; ask the peers first and join
		// instead if any of them answers for a configured cluster.
		probe := cluster.NewPeerClient(existingClusterProbeTimeout, cfg.Security.ClusterSecret)
		if existingClusterAnswers(context.Background(), probe, cfg, nodeID, log) {
			joinOnly = true
			log.Warn("initial member has no raft state but a configured cluster answered; joining it instead of bootstrapping", "node", nodeID)
		}
		closeWithLog(log, "probe rpc client", probe.Close)
	}
	ms, err := metastore.New(metastore.Config{
		NodeID:        nodeID,
		DataDir:       metastoreDir,
		BindAddr:      cfg.Cluster.Addr,
		AdvertiseAddr: clusterAdvertiseAddr(cfg, nodeID),
		Peers:         bootstrapPeers(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers, cfg.Cluster.InitialMembers),
		JoinOnly:      joinOnly,
		TLS:           clusterTLS,
	})
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	defer closeWithLog(log, "metastore", ms.Close)

	schemas := schema.NewJSONSchema()
	if err = initializeSchemas(context.Background(), ms, schemas); err != nil {
		return fmt.Errorf("initialize schemas: %w", err)
	}

	bc, err := buildBroker(cfg, nodeID, ms, schemas, m, log)
	if err != nil {
		return err
	}
	defer closeWithLog(log, "broker", bc.broker.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// failServe cancels ctx with a cause when a critical background
	// component fails, so runServe shuts down and exits with that error
	// instead of serving client traffic in a degraded state.
	ctx, failServe := context.WithCancelCause(ctx)
	defer failServe(nil)

	// The member address borrows its host from the advertised cluster
	// address, so a pod outside the pinned peer list still registers a
	// reachable endpoint.
	memberAddr := advertisedMemberAddr(nodeID, cfg.HTTP.Addr, clusterAdvertiseAddr(cfg, nodeID), cfg.Cluster.Peers)
	// A multi-node cluster whose advertised member address is unroutable
	// (a bare 0.0.0.0/:: bind with no host to borrow) will silently fail
	// member registration — every peer resolves this node as its own
	// loopback. Warn loudly; a single-node run is unaffected.
	if len(cfg.Cluster.Peers) > 0 && memberAddrLikelyUnroutable(memberAddr) {
		log.Warn("advertised member address is not routable by peers; set http.addr to a host:port or give this node a hostful cluster peer entry — member registration will not converge",
			"member_addr", memberAddr, "node", nodeID)
	}
	member := metastore.Member{
		ID:          nodeID,
		Addr:        memberAddr,
		ClusterAddr: clusterAdvertiseAddr(cfg, nodeID),
		Status:      metastore.MemberAlive,
	}
	cs := buildClusterStack(cfg, nodeID, ms, bc, reg, log)
	// Registered before the goroutine drain below, so it runs after every
	// peer-RPC user has stopped (defers are LIFO).
	defer closeWithLog(log, "peer rpc client", cs.peerRPC.Close)

	// Start background processes. This defer is registered AFTER the
	// broker/metastore Close defers above so it runs BEFORE them (defers
	// are LIFO): the goroutines below must be cancelled and drained
	// before the broker and metastore they use are closed, on every
	// return path.
	var wg sync.WaitGroup
	defer func() {
		stop()
		wg.Wait()
	}()

	// Gate topic creation until startup reconciliation completes. The
	// cluster RPC listener started below can deliver a peer-forwarded
	// CreateTopic while runStartupReconcile's orphan sweep is still
	// walking topic directories; ungated, such a create could mkdir its
	// topic dir after the sweep's existence check and have the live
	// directory removed as an orphan. The gate is armed BEFORE the QUIC
	// listener starts and released right after runStartupReconcile
	// returns (on success and failure alike — a degraded reconcile must
	// not leave creates blocked forever). The deferred release is an
	// idempotent safety net for any early-return path; registered after
	// the wg.Wait defer above, it runs first (defers are LIFO), so on
	// shutdown a create blocked on the gate — cluster RPC requests carry
	// no deadline — unblocks before we wait for the RPC goroutines.
	bc.createGate.ArmCreateGate()
	defer bc.createGate.ReleaseCreateGate()

	// Cluster admission. A join-only node asks the existing leader to
	// add it to the Raft voter set right away. Every other node (an
	// initial member with prior state) normally sees a leader within an
	// election timeout; if it does not, it runs the same loop after a
	// bounded wait, because a node that was Raft-removed and came back
	// with its old volume otherwise sits leaderless forever. The loop
	// exits at the first leader sighting without sending anything, so a
	// healthy restart never issues a join.
	wg.Go(func() {
		runClusterJoinWhenLeaderless(ctx, ms, cs.peerRPC, cfg, nodeID, joinOnly, fresh, log)
	})
	wg.Go(func() { runMemberHeartbeater(ctx, ms, member, 5*time.Second, cs.peerRPC, log) })
	wg.Go(func() { cs.controller.Run(ctx) })
	wg.Go(func() { bc.offsets.RunPurger(ctx, time.Second) })
	wg.Go(func() { cs.dispatcher.Run(ctx) })
	wg.Go(func() { cs.fanout.Run(ctx) })
	wg.Go(func() { cs.mover.Run(ctx) })
	startPprofServer(ctx, &wg, cfg.HTTP.PprofAddr, log)
	wg.Go(func() { serveClusterRPC(ctx, cfg, cs.rpcServer, failServe, log) })

	poller := metrics.NewPoller(m, bc.broker, log, cfg.Storage.DataDir)
	wg.Go(func() { poller.Run(ctx) })
	wg.Go(func() { bc.logs.RunIdleEviction(ctx, time.Duration(cfg.Storage.IdleLogEvictionMs)*time.Millisecond) })

	// Startup reconciliation: once this node's metastore replica is caught
	// up, reclaim orphaned topic dirs (crash safety) and open owned
	// partition logs so their retention reapers run even for topics that
	// are idle after a restart. It runs in the BACKGROUND so the HTTP
	// listener below starts immediately: under quorum loss the metastore
	// catch-up wait can take the full startupReconcileCaughtUpTimeout
	// (60s), and a synchronous reconcile would leave /healthz unanswered
	// for that whole window — long enough for a Kubernetes startup probe
	// to kill the pod in a restart loop. Correctness is preserved because
	// the create gate armed above blocks topic creates on EVERY transport
	// (HTTP and cluster RPC) until the sweep is done, and MarkReady is
	// only called once the replica has provably caught up, so /readyz
	// still implies a reconciled node while /healthz answers from the
	// start.
	wg.Go(func() {
		if joinOnly {
			// A join-only node must not reconcile — or mark itself ready —
			// until the leader has admitted it and Raft has a leader in
			// view. There is no timeout: /healthz answers throughout (the
			// pod stays alive), /readyz stays false (no traffic routes
			// here), and admission normally lands within seconds.
			waitForClusterLeader(ctx, ms)
		}
		caughtUp := runStartupReconcile(ctx, ms, bc.logs, cs.peerRPC, cfg.Storage.DataDir, nodeID, log)
		// The sweep can no longer race a create: open the gate so topic
		// creates (HTTP and cluster RPC) proceed. Reconcile failures are
		// non-fatal (logged and skipped inside runStartupReconcile), so
		// creates must be unblocked here regardless of how it went; the
		// idempotent deferred release above stays the shutdown safety net.
		bc.createGate.ReleaseCreateGate()
		if !caughtUp {
			// The catch-up timeout is NOT a readiness path. A node whose
			// replica never caught up (no leader reachable, removed from
			// the voter set, rival cluster) would otherwise become Ready
			// and serve a frozen replica: stale users and grants, stale
			// topics, 503 for everything ownership-related. Keep waiting
			// with no timeout; /readyz stays false meanwhile.
			if !waitMetastoreCaughtUp(ctx, ms, 0) {
				return
			}
			openOwnedPartitionLogs(ctx, ms, bc.logs, nodeID, log)
		}
		if ctx.Err() == nil {
			bc.lifecycle.MarkReady()
		}
	})

	// Authentication: seed the root admin (background, leader-gated so
	// exactly one node wins) and gate the API with Basic auth when
	// security is enabled.
	auth := buildAuthenticator(cfg, ms, log)
	seedRootAdmin(ctx, cfg, ms, log)

	// Finally build the API server. It serves /healthz immediately;
	// /readyz turns true only when the reconcile goroutine above calls
	// MarkReady AND the metastore's live check (leader in view, recent
	// contact, ownership latch set) passes on that probe.
	srv := buildAPIServer(ctx, cfg, bc.broker, bc.logs, ms, cs.router, m, reg, auth, log)
	defer bc.lifecycle.MarkNotReady()

	m.BootDurationSeconds.Set(time.Since(bootStart).Seconds())
	log.Info("narad serve starting",
		"addr", cfg.HTTP.Addr,
		"cluster_addr", cfg.Cluster.Addr,
		"data_dir", cfg.Storage.DataDir,
		"version", versionString())

	if err = srv.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}

	stop()
	wg.Wait()

	// A background component may have cancelled ctx via failServe;
	// surface its error so the process exits non-zero.
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}

	log.Info("narad serve stopped")

	return nil
}

// clusterStack is the cluster-facing plumbing runServe wires around the
// broker: reconciliation controller, request router, peer RPC client and
// server, and the ingress produce dispatcher.
type clusterStack struct {
	controller *controller.Controller
	router     *cluster.Router
	peerRPC    *cluster.PeerClient
	rpcServer  *cluster.RPCServer
	dispatcher *cluster.ProduceDispatcher
	fanout     *cluster.FanoutRunner
	mover      *cluster.MoveRunner
}

func buildClusterStack(cfg *config.Config, nodeID string, ms *metastore.Store, bc *brokerComponents, reg prometheus.Registerer, log *slog.Logger) *clusterStack {
	ctrl := controller.New(ms, controller.Config{})

	// One peer client for the whole process: the router, dispatcher,
	// fan-out runner, mover, heartbeater, and join loop all forward
	// through it, so each peer gets one QUIC connection and one set of
	// pooled streams from this node.
	peerRPC := cluster.NewPeerClient(5*time.Second, cfg.Security.ClusterSecret)
	peerRPC.SetMetrics(cluster.NewPrometheusRPCMetrics(reg))

	router := cluster.NewRouter(ms, nodeID, partition.NewHashRoundRobin(), cfg.Security.ClusterSecret)
	router.SetPeerClient(peerRPC)
	// The router clamps client-supplied long-poll waits (?wait=) on its
	// forward and re-probe paths to the same ceiling the HTTP handlers use.
	router.SetMaxConsumeWait(cfg.HTTP.MaxConsumeWait.D())

	rpcServer := cluster.NewRPCServer(bc.broker, ms, log)
	// The RPC-side clamp on wire-supplied consume waits must agree with
	// the router's and the HTTP handlers' ceiling.
	rpcServer.SetMaxConsumeWait(cfg.HTTP.MaxConsumeWait.D())
	// So a delete forwarded to this node as leader still fans the purge out
	// to the partition owners, matching the HTTP leader-direct path.
	rpcServer.SetBroadcaster(router)

	return &clusterStack{
		controller: ctrl,
		router:     router,
		peerRPC:    peerRPC,
		rpcServer:  rpcServer,
		dispatcher: cluster.NewProduceDispatcher(bc.ingress, ms, nodeID, bc.broker, peerRPC, log, cluster.ProduceDispatcherConfig{}),
		fanout: cluster.NewFanoutRunner(ms, nodeID, cfg.Storage.DataDir, bc.broker, peerRPC,
			partition.NewHashRoundRobin(), bc.metrics, log, cluster.FanoutConfig{
				MaxBatchRecords: cfg.Fanout.MaxBatchRecords,
				MaxBatchBytes:   cfg.Fanout.MaxBatchBytes,
				Linger:          time.Duration(cfg.Fanout.LingerMs) * time.Millisecond,
			}),
		mover: cluster.NewMoveRunner(ms, nodeID, cfg.Storage.DataDir, peerRPC, bc.broker, bc.metrics, log, cluster.MoveConfig{}),
	}
}

// serveClusterRPC runs the QUIC peer-RPC listener until ctx is cancelled.
// In a multi-node cluster a dead listener makes this node unreachable for
// all peer RPC, so it fails the serve loop via failServe rather than
// keeping degraded client HTTP alive.
func serveClusterRPC(ctx context.Context, cfg *config.Config, rpc *cluster.RPCServer, failServe func(error), log *slog.Logger) {
	err := clusterrpc.ServeQUIC(ctx, cfg.HTTP.Addr, cfg.Security.ClusterSecret, log, rpc)
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	log.Error("cluster rpc server", "addr", cfg.HTTP.Addr, "err", err)
	if len(cfg.Cluster.Peers) > 0 {
		failServe(fmt.Errorf("cluster rpc server: %w", err))
	}
}
