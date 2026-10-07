package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// storeWithRemote is a single-node metastore whose registry holds one
// remote (the FSM stores ciphertext it cannot open; the startup rules
// only look at whether any remote exists).
func storeWithRemote(t *testing.T, holds bool) *metastore.Store {
	t.Helper()
	ms, err := metastore.New(metastore.Config{NodeID: "n0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	waitForLeadership(t, ms)
	if holds {
		op := metastore.PutRemoteOp{
			Record: domremote.Record{
				Name: "b", ID: "id-b", URL: "https://narad-b.example", Username: "repl",
				Credential: domremote.Envelope{V: 1, KV: "0123456789abcdef", CT: bytes.Repeat([]byte{1}, 40)},
				Limits:     domremote.DefaultLimits(),
			},
			Salt: bytes.Repeat([]byte{7}, 32), SealedAtMs: time.Now().UnixMilli(),
		}
		if err := ms.PutRemote(context.Background(), op); err != nil {
			t.Fatal(err)
		}
	}
	return ms
}

// strongSecret is a cluster secret as `openssl rand -base64 32` prints
// it, drawn again in the rare case the strength rule refuses it.
func strongSecret(t *testing.T) string {
	t.Helper()
	for {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		if s := base64.StdEncoding.EncodeToString(b); remotecred.CheckSecretStrength(s) == nil {
			return s
		}
	}
}

func remotesConfig(secured bool, secret string) *config.Config {
	cfg := config.Default()
	cfg.Security.Enabled = secured
	cfg.Security.ClusterSecret = secret
	return cfg
}

func TestStartupRefusesSecurityOffOrNoSecretWithRemotes(t *testing.T) {
	ms := storeWithRemote(t, true)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := buildRemotes(remotesConfig(false, strongSecret(t)), ms, "n0", nil, quiet); !errors.Is(err, remote.ErrStartupSecurityOff) {
		t.Fatalf("security off with a remote: %v", err)
	}
	if _, err := buildRemotes(remotesConfig(true, ""), ms, "n0", nil, quiet); !errors.Is(err, remote.ErrStartupNoSecret) {
		t.Fatalf("no secret with a remote: %v", err)
	}
	// A node without remotes is not checked.
	empty := storeWithRemote(t, false)
	if _, err := buildRemotes(remotesConfig(false, ""), empty, "n0", nil, quiet); err != nil {
		t.Fatalf("security off without remotes: %v", err)
	}
}

func TestStartupWarnsWithoutRefusing(t *testing.T) {
	ms := storeWithRemote(t, true)
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	m := metrics.New(prometheus.NewRegistry())
	cfg := remotesConfig(true, "cluster-test-secret") // weak: fails the seal-time rule
	cfg.Cluster.Peers = []config.ClusterPeer{{ID: "n0", Addr: "127.0.0.1:1"}, {ID: "n1", Addr: "127.0.0.1:2"}, {ID: "n2", Addr: "127.0.0.1:3"}}
	cfg.Security.AllowPlaintextRaft = true
	rs, err := buildRemotes(cfg, ms, "n0", m, log)
	if err != nil {
		t.Fatalf("a weak secret and plaintext Raft must not stop the node: %v", err)
	}
	out := logs.String()
	for _, want := range []string{"fails the strength rule", "without TLS", "remotes.allowed_hosts is empty"} {
		if !strings.Contains(out, want) {
			t.Fatalf("startup log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "cluster-test-secret") {
		t.Fatal("the startup log quotes the secret")
	}
	if testutil.ToFloat64(m.Remote.PlaintextRaft) != 1 || testutil.ToFloat64(m.Remote.AllowlistConfigured) != 0 {
		t.Fatal("posture gauges not set")
	}
	// The weak secret cannot open what was sealed under another one:
	// the link holds, the node runs.
	if _, err := rs.cache.Get("b"); !errors.Is(err, remote.ErrCredentialUnreadable) {
		t.Fatalf("cache: %v", err)
	}
}

func TestStartupAllowlistAndPosture(t *testing.T) {
	ms := storeWithRemote(t, false)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := remotesConfig(true, strongSecret(t))
	cfg.Remotes.AllowedHosts = []string{"*.internal.example"}
	m := metrics.New(prometheus.NewRegistry())
	rs, err := buildRemotes(cfg, ms, "n0", m, quiet)
	if err != nil || !rs.guard.AllowlistConfigured() || testutil.ToFloat64(m.Remote.AllowlistConfigured) != 1 {
		t.Fatalf("allowlist: %v", err)
	}
	cfg.Remotes.AllowedHosts = []string{"a.*.example"}
	if _, err := buildRemotes(cfg, ms, "n0", nil, quiet); err == nil {
		t.Fatal("a bad allowlist entry started")
	}
	cfg = remotesConfig(true, strongSecret(t))
	cfg.Security.AllowLegacyClusterAuth = true
	cfg.Remotes.APIHopEncrypted = true
	p := nodePosture(cfg)
	if !p.SecurityEnabled || !p.LegacyClusterAuth || !p.APIHopEncrypted || !p.RaftTLS {
		t.Fatalf("posture = %+v (a single node counts as Raft TLS)", p)
	}
}

// TestGeneratedSecretNeverSealsRemotes runs the startup order runServe
// uses (secureNodeRPC, then buildRemotes) for a secured single node with
// no NARAD_CLUSTER_SECRET. The per-process secret secureNodeRPC makes is
// gone after a restart, so the remotes plane must treat the node as one
// with no secret: a node holding remotes refuses to start, and a node
// without them answers every seal with secret_missing.
func TestGeneratedSecretNeverSealsRemotes(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := remotesConfig(true, "")
	cfg.Remotes.APIHopEncrypted = true
	if err := secureNodeRPC(cfg, quiet); err != nil {
		t.Fatal(err)
	}
	if cfg.Security.ClusterSecret == "" {
		t.Fatal("precondition: secureNodeRPC generated no node RPC secret")
	}
	if _, err := buildRemotes(cfg, storeWithRemote(t, true), "n0", nil, quiet); !errors.Is(err, remote.ErrStartupNoSecret) {
		t.Fatalf("a node holding remotes with only a generated secret: err = %v, want ErrStartupNoSecret", err)
	}
	rs, err := buildRemotes(cfg, storeWithRemote(t, false), "n0", nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.service.PrepareCreate(context.Background(), remote.CreateRequest{Name: "b"}); !errors.Is(err, errs.ErrRemoteSecretMissing) {
		t.Fatalf("seal under a generated secret: err = %v, want ErrRemoteSecretMissing", err)
	}
}

// A node with no peers configured may still run Raft in plaintext with
// other nodes (they joined it): when its Raft port is reachable beyond
// loopback and has no TLS, its posture says plaintext and the Q23 warning
// and gauge fire. A loopback-bound single node counts as TLS.
func TestPeerlessNodeOnARoutableRaftAddressReportsPlaintextRaft(t *testing.T) {
	cfg := remotesConfig(true, strongSecret(t))
	cfg.Security.AllowPlaintextRaft = true
	cfg.Cluster.Addr = "10.0.0.5:7943"
	if nodePosture(cfg).RaftTLS {
		t.Fatal("a peerless node serving plaintext Raft on a routable address reports Raft TLS")
	}
	cfg.Cluster.Addr = "127.0.0.1:7943"
	if !nodePosture(cfg).RaftTLS {
		t.Fatal("a loopback-bound single node must count as Raft TLS")
	}
}
