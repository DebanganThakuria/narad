package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
)

// syncBuffer is a bytes.Buffer safe for a logger written from several
// goroutines while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func capturedLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func threePeers() []config.ClusterPeer {
	return []config.ClusterPeer{{ID: "a", Addr: "a:7943"}, {ID: "b", Addr: "b:7943"}, {ID: "c", Addr: "c:7943"}}
}

// TestNodeRPCSecretFollowsTheDeployment pins which deployments get a
// generated node RPC secret: only a secured node with no peers and no
// secret of its own. The value is never logged.
func TestNodeRPCSecretFollowsTheDeployment(t *testing.T) {
	t.Run("secured single node gets a per-process secret", func(t *testing.T) {
		var secrets []string
		for range 2 {
			cfg := config.Default()
			log, buf := capturedLogger()
			if err := secureNodeRPC(cfg, log); err != nil {
				t.Fatal(err)
			}
			secret := cfg.Security.ClusterSecret
			if len(secret) != 64 || strings.Trim(secret, "0123456789abcdef") != "" {
				t.Fatalf("generated secret %d chars, want 64 hex chars", len(secret))
			}
			out := buf.String()
			if strings.Contains(out, secret) {
				t.Fatal("the generated secret was logged")
			}
			if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "node RPC is closed to other processes") || !strings.Contains(out, "component=audit") {
				t.Fatalf("no audit info line saying node RPC is closed to other processes: %s", out)
			}
			secrets = append(secrets, secret)
		}
		if secrets[0] == secrets[1] {
			t.Fatal("two processes generated the same secret")
		}
	})

	t.Run("an explicit secret is kept and nothing is logged", func(t *testing.T) {
		cfg := config.Default()
		cfg.Security.ClusterSecret = "operator-secret"
		log, buf := capturedLogger()
		if err := secureNodeRPC(cfg, log); err != nil {
			t.Fatal(err)
		}
		if cfg.Security.ClusterSecret != "operator-secret" {
			t.Fatal("explicit secret replaced")
		}
		if out := buf.String(); out != "" {
			t.Fatalf("logged for a node with an explicit secret: %s", out)
		}
	})

	warnsOpenPlane := func(t *testing.T, cfg *config.Config) {
		t.Helper()
		log, buf := capturedLogger()
		if err := secureNodeRPC(cfg, log); err != nil {
			t.Fatal(err)
		}
		if cfg.Security.ClusterSecret != "" {
			t.Fatal("generated a secret with security off")
		}
		out := buf.String()
		if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "node RPC plane is unauthenticated") || !strings.Contains(out, "component=audit") {
			t.Fatalf("no audit warning naming the unauthenticated node RPC plane: %s", out)
		}
	}

	t.Run("security off on a single node stays open and warns", func(t *testing.T) {
		cfg := config.Default()
		cfg.Security.Enabled = false
		warnsOpenPlane(t, cfg)
	})

	t.Run("security off with peers stays open and warns", func(t *testing.T) {
		cfg := config.Default()
		cfg.Security.Enabled = false
		cfg.Security.AllowInsecureCluster = true
		cfg.Cluster.Peers = threePeers()
		warnsOpenPlane(t, cfg)
	})

	t.Run("secured peers without a secret are left to validation", func(t *testing.T) {
		cfg := config.Default()
		cfg.Cluster.Peers = threePeers()
		log, _ := capturedLogger()
		if err := secureNodeRPC(cfg, log); err != nil {
			t.Fatal(err)
		}
		if cfg.Security.ClusterSecret != "" {
			t.Fatal("generated a per-process secret that peers could never share")
		}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "security.cluster_secret") {
			t.Fatalf("Validate() = %v, want the missing cluster secret refused", err)
		}
	})
}

// freeUDPAddr returns a 127.0.0.1 address whose UDP port was free a
// moment ago (the node RPC plane listens on UDP at the API address).
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	if err := pc.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// singleNodeStore opens a one-voter metastore and waits for it to lead.
func singleNodeStore(t *testing.T) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	waitForLeadership(t, store)
	return store
}

// mintAdminOverNodeRPC sends one OpCreateUser for an admin "mallory"
// as a process that knows no cluster secret.
func mintAdminOverNodeRPC(t *testing.T, addr string) (status int, err error) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(user.User{Username: "mallory", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionAdmin}}})
	if err != nil {
		t.Fatal(err)
	}
	pc := cluster.NewPeerClient(3*time.Second, "")
	defer func() { _ = pc.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := pc.CreateUser(ctx, addr, body)
	if err != nil {
		return 0, err
	}
	return res.Status, nil
}

func requireNoMallory(t *testing.T, store *metastore.Store) {
	t.Helper()
	if _, err := store.GetUser(context.Background(), "mallory"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("GetUser(mallory) = %v, want not found: an unauthenticated node RPC peer created an admin", err)
	}
}

// TestSecuredSingleNodeRefusesUnauthenticatedNodeRPC runs the node RPC
// plane the way runServe builds it for a secured node with no peers and
// no NARAD_CLUSTER_SECRET, then tries to mint an admin over it without
// the secret.
func TestSecuredSingleNodeRefusesUnauthenticatedNodeRPC(t *testing.T) {
	cfg := config.Default()
	cfg.HTTP.Addr = freeUDPAddr(t)
	log, _ := capturedLogger()
	if err := secureNodeRPC(cfg, log); err != nil {
		t.Fatal(err)
	}
	store := singleNodeStore(t)
	rpc := cluster.NewRPCServer(nil, store, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var failMu sync.Mutex
	var failed error
	go func() {
		defer close(done)
		serveClusterRPC(ctx, cfg, rpc, func(err error) {
			failMu.Lock()
			defer failMu.Unlock()
			failed = err
		}, log)
	}()
	t.Cleanup(func() { cancel(); <-done })

	// The node itself, holding the resolved secret, can talk to it.
	self := cluster.NewPeerClient(2*time.Second, cfg.Security.ClusterSecret)
	defer func() { _ = self.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := self.AppliedIndex(probeCtx, cfg.HTTP.Addr)
		probeCancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node RPC never answered the node's own client: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	status, err := mintAdminOverNodeRPC(t, cfg.HTTP.Addr)
	t.Logf("unauthenticated OpCreateUser: status=%d err=%v", status, err)
	if err == nil && status == http.StatusCreated {
		t.Errorf("an unauthenticated node RPC peer created a user (status %d)", status)
	}
	requireNoMallory(t, store)
	failMu.Lock()
	defer failMu.Unlock()
	if failed != nil {
		t.Fatalf("serve failed: %v", failed)
	}
}

// TestNodeRPCListenerRefusesToServeASecuredNodeWithoutASecret pins the
// listener's own fail-closed check: handed a secured config with an
// empty secret (as if a later change skipped secureNodeRPC), it fails
// the serve loop instead of serving an unauthenticated plane.
func TestNodeRPCListenerRefusesToServeASecuredNodeWithoutASecret(t *testing.T) {
	cfg := config.Default()
	cfg.HTTP.Addr = freeUDPAddr(t)
	if !cfg.Security.Enabled || cfg.Security.ClusterSecret != "" {
		t.Fatal("precondition: the default config is secured with no cluster secret")
	}
	log, _ := capturedLogger()
	store := singleNodeStore(t)
	rpc := cluster.NewRPCServer(nil, store, log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	failed := make(chan error, 1)
	go func() {
		defer close(done)
		serveClusterRPC(ctx, cfg, rpc, func(err error) { failed <- err }, log)
	}()
	t.Cleanup(func() { cancel(); <-done })

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("serveClusterRPC is still serving a secured node that has no cluster secret")
	}

	status, err := mintAdminOverNodeRPC(t, cfg.HTTP.Addr)
	t.Logf("unauthenticated OpCreateUser: status=%d err=%v", status, err)
	if err == nil && status == http.StatusCreated {
		t.Errorf("an unauthenticated node RPC peer created a user (status %d)", status)
	}
	requireNoMallory(t, store)

	select {
	case err := <-failed:
		if err == nil || !strings.Contains(err.Error(), "refusing to serve node RPC without a cluster secret") {
			t.Fatalf("failServe(%v), want the refusal", err)
		}
	default:
		t.Fatal("failServe was never called")
	}
}
