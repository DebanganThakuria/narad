package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const applyTimeout = 5 * time.Second

// Config configures the Raft-backed metastore.
type Config struct {
	NodeID        string
	DataDir       string
	BindAddr      string
	AdvertiseAddr string
	Peers         []Peer
	// JoinOnly prevents this node from bootstrapping a cluster when it
	// has no prior Raft state: it starts with an EMPTY configuration and
	// waits for the existing leader to admit it via AddVoter (the
	// OpJoinCluster RPC). Without it, a scale-out node would bootstrap a
	// phantom cluster from its peer list and never join the real one.
	JoinOnly bool
	Logger   io.Writer
	// TLS, when non-nil, secures the Raft transport with mutual TLS.
	// Nil runs it as plain TCP (relying on network isolation).
	TLS *TLSConfig
}

// Peer is a known Raft voter used for cluster bootstrap.
type Peer struct {
	ID   string
	Addr string
}

// Store is the Raft-backed metastore. Writes go through Raft consensus;
// reads are served from the local bbolt replica.
type Store struct {
	r *raft.Raft
	// leaderCommit records the highest commit index a leader has
	// reported to this node over the Raft transport since the process
	// started; AppliedCaughtUp compares applied_index against it.
	leaderCommit *commitObservingTransport
	// ownershipReady latches the first time the replica is provably
	// caught up after this process started; see ListAssignments.
	// latchMu serialises the self-leader Barrier that guards the latch.
	ownershipReady atomic.Bool
	latchMu        sync.Mutex
	fsm            *fsmState

	// attachOffsets, when registered (SetAttachOffsetResolver), observes
	// the parent's per-partition committed tail for an attach so the
	// link records its exact starting point; see fanout_anchor.go.
	attachOffsetsMu sync.RWMutex
	attachOffsets   AttachOffsetResolver
}

// New opens or creates the Raft metastore at cfg.DataDir.
func New(cfg Config) (*Store, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("metastore: mkdir: %w", err)
	}

	fsm, err := newFSM(filepath.Join(cfg.DataDir, "fsm.db"))
	if err != nil {
		return nil, fmt.Errorf("metastore: fsm: %w", err)
	}

	r, transport, err := newRaft(cfg, fsm)
	if err != nil {
		return nil, err
	}
	return &Store{r: r, leaderCommit: transport, fsm: fsm}, nil
}

// newRaft wires up the Raft node: log/stable store, snapshot store, TCP
// transport, and — only when no prior state exists on disk — a one-time
// cluster bootstrap from cfg plus cfg.Peers. It also returns the
// commit-observing transport wrapper so the store can read the leader's
// commit index.
func newRaft(cfg Config, fsm *fsmState) (*raft.Raft, *commitObservingTransport, error) {
	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.DataDir, "raft.db"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("metastore: raft store: %w", err)
	}

	logOutput := cfg.Logger
	if logOutput == nil {
		logOutput = io.Discard
	}

	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, logOutput)
	if err != nil {
		return nil, nil, fmt.Errorf("metastore: snapshots: %w", err)
	}

	rawTransport, advertiseAddr, err := newTransport(cfg, logOutput)
	if err != nil {
		return nil, nil, err
	}
	// Raft only ever sees the wrapper; see leader_commit.go for why the
	// leader's commit index has to be read off the wire.
	transport := newCommitObservingTransport(rawTransport)

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.LogOutput = logOutput

	r, err := raft.NewRaft(rc, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		_ = transport.Close()
		return nil, nil, fmt.Errorf("metastore: raft: %w", err)
	}

	hasState, err := raft.HasExistingState(boltStore, boltStore, snapStore)
	if err != nil {
		return nil, nil, fmt.Errorf("metastore: check state: %w", err)
	}
	if !hasState && !cfg.JoinOnly {
		if err := bootstrapCluster(r, cfg, advertiseAddr); err != nil {
			return nil, nil, err
		}
	}
	return r, transport, nil
}

// HasExistingState reports whether a Raft log, stable store, or snapshot
// already exists under dataDir, without starting Raft. serve uses it
// before New to decide whether an initial member that would otherwise
// bootstrap must instead ask the running cluster for admission (a node
// whose volume was replaced must not seed a rival cluster).
func HasExistingState(dataDir string) (bool, error) {
	raftPath := filepath.Join(dataDir, "raft.db")
	if _, err := os.Stat(raftPath); errors.Is(err, os.ErrNotExist) {
		// No log at all. A snapshot without a log is not a state Raft
		// would start from either, so this is definitive.
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("metastore: stat raft store: %w", err)
	}
	boltStore, err := raftboltdb.New(raftboltdb.Options{Path: raftPath})
	if err != nil {
		return false, fmt.Errorf("metastore: raft store: %w", err)
	}
	defer boltStore.Close()
	snapStore, err := raft.NewFileSnapshotStore(dataDir, 2, io.Discard)
	if err != nil {
		return false, fmt.Errorf("metastore: snapshots: %w", err)
	}
	has, err := raft.HasExistingState(boltStore, boltStore, snapStore)
	if err != nil {
		return false, fmt.Errorf("metastore: check state: %w", err)
	}
	return has, nil
}

// newTransport validates the bind address and returns a TCP transport
// advertising cfg.AdvertiseAddr (falling back to cfg.BindAddr), along
// with the advertise address actually used.
func newTransport(cfg Config, logOutput io.Writer) (raft.Transport, string, error) {
	if _, err := net.ResolveTCPAddr("tcp", cfg.BindAddr); err != nil {
		return nil, "", fmt.Errorf("metastore: bind addr: %w", err)
	}
	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.BindAddr
	}
	resolved, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, "", fmt.Errorf("metastore: advertise addr: %w", err)
	}

	transportLog := hclog.New(&hclog.LoggerOptions{Output: logOutput})

	if cfg.TLS != nil {
		stream, err := newTLSStreamLayer(cfg.BindAddr, resolved, cfg.TLS)
		if err != nil {
			return nil, "", fmt.Errorf("metastore: tls transport: %w", err)
		}
		return raft.NewNetworkTransportWithLogger(stream, 3, 10*time.Second, transportLog), advertiseAddr, nil
	}

	transport, err := raft.NewTCPTransportWithLogger(cfg.BindAddr, resolved, 3, 10*time.Second, transportLog)
	if err != nil {
		return nil, "", fmt.Errorf("metastore: transport: %w", err)
	}
	return transport, advertiseAddr, nil
}

// bootstrapCluster seeds the initial voter set: this node plus cfg.Peers.
func bootstrapCluster(r *raft.Raft, cfg Config, advertiseAddr string) error {
	servers := []raft.Server{{ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(advertiseAddr)}}
	for _, p := range cfg.Peers {
		servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)})
	}
	if f := r.BootstrapCluster(raft.Configuration{Servers: servers}); f.Error() != nil {
		return fmt.Errorf("metastore: bootstrap: %w", f.Error())
	}
	return nil
}

// apply encodes payload as JSON and submits it through Raft, returning
// either the Raft error or the FSM's business error for this command.
func (s *Store) apply(ctx context.Context, op opCode, payload any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	b, err := json.Marshal(cmd{Op: op, Data: data})
	if err != nil {
		return err
	}

	f := s.r.Apply(b, applyTimeout)
	if err := f.Error(); err != nil {
		return classifyRaftError(err)
	}
	if resp, ok := f.Response().(error); ok {
		return resp
	}
	return nil
}

// classifyRaftError maps Raft's leadership/availability failures onto
// errs.ErrUnavailable so the HTTP layer answers 503 (retryable) instead
// of 500. These all mean "no committed decision right now" — expected
// during elections, partitions, and rolling restarts — not a bug. The
// original error is wrapped so logs keep the specific cause.
func classifyRaftError(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrLeadershipTransferInProgress),
		errors.Is(err, raft.ErrRaftShutdown),
		errors.Is(err, raft.ErrEnqueueTimeout):
		return fmt.Errorf("%w: %v", errs.ErrUnavailable, err)
	default:
		return err
	}
}

// ownershipViewReady reports whether the local replica may be used to
// decide partition ownership: it has caught up with the leader at least
// once since this process started (ListAssignments explains why). The
// latch never clears: a replica that later falls behind is the ordinary
// steady-state lag every node has, and partition moves coordinate with
// the old owner through the handoff protocol rather than through this
// view. A store without Raft (tests, tools) is always ready.
//
// A follower latches once AppliedCaughtUp holds, which requires the
// FSM to have applied everything the leader reported committed (see
// leader_commit.go). A node that leads ITSELF must additionally pass a
// Raft barrier first: election proves a fresh leader's log is complete,
// not that its FSM has replayed it, and a just-elected node restored
// from an old snapshot would otherwise latch on a stale view.
func (s *Store) ownershipViewReady() bool {
	if s.ownershipReady.Load() {
		return true
	}
	if s.r == nil {
		s.ownershipReady.Store(true)
		return true
	}
	if !s.AppliedCaughtUp() {
		return false
	}
	if s.r.State() == raft.Leader {
		s.latchMu.Lock()
		defer s.latchMu.Unlock()
		if s.ownershipReady.Load() {
			return true
		}
		if err := s.Barrier(); err != nil {
			return false
		}
	}
	s.ownershipReady.Store(true)
	return true
}

// OwnershipViewReady reports whether the ownership latch is set (setting
// it if the replica is now provably caught up). Readiness checks use it
// so a node is only routable once its view of partition ownership is
// trustworthy.
func (s *Store) OwnershipViewReady() bool {
	return s.ownershipViewReady()
}
