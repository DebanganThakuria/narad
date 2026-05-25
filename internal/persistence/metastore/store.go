package metastore

import (
	"bytes"
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
	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

const applyTimeout = 5 * time.Second

// Config configures the Raft-backed metastore.
type Config struct {
	NodeID        string // stable pod identity, e.g. "narad-0"
	DataDir       string // raft.db, fsm.db, snapshots/ all go here
	BindAddr      string // TCP address for Raft transport, e.g. "0.0.0.0:7943"
	AdvertiseAddr string // cluster-routable Raft address advertised to peers
	Peers         []Peer // empty = single-node bootstrap
	// Logger receives Raft's internal log output. Defaults to io.Discard
	// (quiet). Pass os.Stderr or a structured writer for production.
	Logger io.Writer
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

// -- topic methods --

func (s *Store) CreateTopic(ctx context.Context, t topic.Topic) error {
	return s.apply(ctx, opCreateTopic, t)
}

func (s *Store) UpdateTopic(ctx context.Context, t topic.Topic) error {
	return s.apply(ctx, opUpdateTopic, t)
}

func (s *Store) DeleteTopic(ctx context.Context, name string) error {
	return s.apply(ctx, opDeleteTopic, name)
}

func (s *Store) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var t topic.Topic
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketTopics).Get([]byte(name))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &t)
	})
	return t, err
}

func (s *Store) ListTopics(_ context.Context, opts ListOptions) ([]topic.Topic, string, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []topic.Topic
	var nextToken string
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketTopics).Cursor()
		var k, v []byte
		if opts.PageToken != "" {
			// Token is the last item of the previous page — seek to it and step past.
			k, v = c.Seek([]byte(opts.PageToken))
			if k != nil {
				k, v = c.Next()
			}
		} else {
			k, v = c.First()
		}
		for ; k != nil; k, v = c.Next() {
			if opts.Limit > 0 && len(out) >= opts.Limit {
				// nextToken is the last item of the current page, not the first of next.
				nextToken = out[len(out)-1].Name
				break
			}
			var t topic.Topic
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			out = append(out, t)
		}
		return nil
	})
	return out, nextToken, err
}

// -- schema methods --

func (s *Store) PutSchema(ctx context.Context, topicName string, version int, schema []byte) error {
	return s.apply(ctx, opPutSchema, schemaPayload{Topic: topicName, Version: version, Schema: schema})
}

func (s *Store) GetSchema(_ context.Context, topicName string, version int) ([]byte, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []byte
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketSchemas).Get(schemaKey(topicName, version))
		if v == nil {
			return ErrNotFound
		}
		out = make([]byte, len(v))
		copy(out, v)
		return nil
	})
	return out, err
}

// -- assignment methods --

func (s *Store) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string, followerID string) error {
	return s.apply(ctx, opAssignPartition, Assignment{Topic: topicName, Partition: partition, OwnerID: ownerID, FollowerID: followerID})
}

func (s *Store) GetAssignment(topicName string, partition int) (Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var a Assignment
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketAssignments).Get(assignmentKey(topicName, partition))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &a)
	})
	return a, err
}

// ListAssignments returns all partition assignments for topicName.
func (s *Store) ListAssignments(topicName string) ([]Assignment, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Assignment
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		prefix := []byte(topicName + ":")
		c := tx.Bucket(bucketAssignments).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var a Assignment
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			out = append(out, a)
		}
		return nil
	})
	return out, err
}

// -- member methods --

func (s *Store) RegisterMember(ctx context.Context, m Member) error {
	return s.apply(ctx, opMemberJoin, m)
}

// Heartbeat records that podID was seen alive at the given Unix timestamp.
// The caller supplies the timestamp so Apply stays deterministic.
func (s *Store) Heartbeat(ctx context.Context, podID string, at int64) error {
	return s.apply(ctx, opMemberHeartbeat, heartbeatPayload{ID: podID, At: at})
}

func (s *Store) MarkMemberDead(ctx context.Context, podID string) error {
	return s.apply(ctx, opMemberDead, podID)
}

func (s *Store) GetMember(podID string) (Member, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var m Member
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMembers).Get([]byte(podID))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &m)
	})
	return m, err
}

func (s *Store) ListMembers() ([]Member, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Member
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMembers).ForEach(func(_, v []byte) error {
			var m Member
			if err := json.Unmarshal(v, &m); err != nil {
				return err
			}
			out = append(out, m)
			return nil
		})
	})
	return out, err
}

// -- lifecycle --

func (s *Store) Close() error {
	// If this node is the leader, transfer leadership to a follower before
	// shutting down. This lets the cluster elect a new leader immediately
	// (~150ms) instead of waiting for the full heartbeat timeout + election
	// window (~300-600ms). Best-effort: if there are no followers (single-node
	// dev deployment) or the transfer fails, we proceed with normal shutdown.
	if s.r.State() == raft.Leader {
		s.r.LeadershipTransfer() //nolint:errcheck
	}
	if err := s.r.Shutdown().Error(); err != nil {
		return err
	}
	s.fsm.mu.Lock()
	defer s.fsm.mu.Unlock()
	return s.fsm.db.Close()
}

// IsLeader reports whether this node is the current Raft leader.
func (s *Store) IsLeader() bool {
	return s.r.State() == raft.Leader
}

// LeaderCh returns a channel that fires true when this node becomes leader
// and false when it loses leadership. The controller listens on this to
// start and stop its background loops.
func (s *Store) LeaderCh() <-chan bool {
	return s.r.LeaderCh()
}

// LeaderAddr returns the current Raft leader address as host:port, or empty
// when no leader is known.
func (s *Store) LeaderAddr() string {
	serverAddress, _ := s.r.LeaderWithID()
	return string(serverAddress)
}

// Ensure Store satisfies Metastore at compile time.
var _ Metastore = (*Store)(nil)
