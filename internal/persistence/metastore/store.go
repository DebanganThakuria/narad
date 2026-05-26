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
	Logger        io.Writer
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

	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(cfg.DataDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("metastore: raft store: %w", err)
	}

	logW := cfg.Logger
	if logW == nil {
		logW = io.Discard
	}
	transportLog := hclog.New(&hclog.LoggerOptions{Output: logW})

	snapStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, logW)
	if err != nil {
		return nil, fmt.Errorf("metastore: snapshots: %w", err)
	}

	if _, err := net.ResolveTCPAddr("tcp", cfg.BindAddr); err != nil {
		return nil, fmt.Errorf("metastore: bind addr: %w", err)
	}
	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.BindAddr
	}
	resolvedAdvertiseAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("metastore: advertise addr: %w", err)
	}
	transport, err := raft.NewTCPTransportWithLogger(cfg.BindAddr, resolvedAdvertiseAddr, 3, 10*time.Second, transportLog)
	if err != nil {
		return nil, fmt.Errorf("metastore: transport: %w", err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.LogOutput = logW

	r, err := raft.NewRaft(rc, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("metastore: raft: %w", err)
	}

	hasState, err := raft.HasExistingState(boltStore, boltStore, snapStore)
	if err != nil {
		return nil, fmt.Errorf("metastore: check state: %w", err)
	}
	if !hasState {
		servers := []raft.Server{{ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(advertiseAddr)}}
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)})
		}
		if f := r.BootstrapCluster(raft.Configuration{Servers: servers}); f.Error() != nil {
			return nil, fmt.Errorf("metastore: bootstrap: %w", f.Error())
		}
	}

	return &Store{r: r, fsm: fsm}, nil
}

// apply encodes payload as JSON and submits it through Raft.
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
