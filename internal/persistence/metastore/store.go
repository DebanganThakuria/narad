package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

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
	r   *raft.Raft
	fsm *fsmState
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

	r, err := newRaft(cfg, fsm)
	if err != nil {
		return nil, err
	}
	return &Store{r: r, fsm: fsm}, nil
}

// newRaft wires up the Raft node: log/stable store, snapshot store, TCP
// transport, and — only when no prior state exists on disk — a one-time
// cluster bootstrap from cfg plus cfg.Peers.
func newRaft(cfg Config, fsm *fsmState) (*raft.Raft, error) {
	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.DataDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("metastore: raft store: %w", err)
	}

	logOutput := cfg.Logger
	if logOutput == nil {
		logOutput = io.Discard
	}

	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, logOutput)
	if err != nil {
		return nil, fmt.Errorf("metastore: snapshots: %w", err)
	}

	transport, advertiseAddr, err := newTransport(cfg, logOutput)
	if err != nil {
		return nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.LogOutput = logOutput

	r, err := raft.NewRaft(rc, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("metastore: raft: %w", err)
	}

	hasState, err := raft.HasExistingState(boltStore, boltStore, snapStore)
	if err != nil {
		return nil, fmt.Errorf("metastore: check state: %w", err)
	}
	if !hasState && !cfg.JoinOnly {
		if err := bootstrapCluster(r, cfg, advertiseAddr); err != nil {
			return nil, err
		}
	}
	return r, nil
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
		return err
	}
	if resp, ok := f.Response().(error); ok {
		return resp
	}
	return nil
}
