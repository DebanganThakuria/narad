package remote_test

// A real Narad target for the outbound plane's tests: a single-node
// metastore, broker, ingress dispatcher and controller behind the real
// router (httpserver.NewRouterWithOptions), served over TLS. Checks and
// response-shape tests run against what a target actually answers,
// never a handler that fakes Narad's answers.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// realTarget is a running target cluster of one node.
type realTarget struct {
	t         testing.TB
	ms        *metastore.Store
	br        broker.Broker
	srv       *httptest.Server
	caPEM     string
	adminUser string
	adminPass string
}

// randomClusterSecret returns n random bytes in hex: test credentials are
// generated, never literals.
func randomClusterSecret(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// newRealTarget starts a target; secured turns on Basic auth and RBAC and
// seeds a root admin with a random password.
func newRealTarget(t testing.TB, secured bool) *realTarget {
	return newRealTargetWith(t, secured, true)
}

// newRealTargetWith is newRealTarget; usersRoute false builds the target
// without a metastore in its handlers, so it has no /v1/users route.
func newRealTargetWith(t testing.TB, secured, usersRoute bool) *realTarget {
	t.Helper()
	dataDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ms, err := metastore.New(metastore.Config{NodeID: "target-0", DataDir: filepath.Join(dataDir, "metastore"), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !ms.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("target metastore: no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := ms.RegisterMember(context.Background(), metastore.Member{
		ID: "target-0", Addr: "127.0.0.1:0", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("register member: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var stopped []chan struct{}
	run := func(f func(context.Context)) {
		done := make(chan struct{})
		stopped = append(stopped, done)
		go func() { defer close(done); f(ctx) }()
	}
	run(controller.New(ms, controller.Config{ReconcileInterval: 20 * time.Millisecond}).Run)

	logs := runtime.NewLogs(dataDir, storage.DefaultOptions(), ms, nil)
	lifecycle := runtime.NewLifecycle(logs)
	ing, err := ingress.OpenManager(dataDir, ingress.DefaultWALOptions())
	if err != nil {
		t.Fatalf("ingress: %v", err)
	}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1024, MaxAckedAhead: 1024}, nil
	}, nil)
	br, err := broker.New(broker.Deps{
		DataDir:        dataDir,
		StorageOptions: storage.DefaultOptions(),
		TopicConfig: broker.TopicConfig{
			DefaultPartitions: 3, MaxPartitions: 16, DefaultRetentionMs: 7 * 24 * 3600 * 1000,
			DefaultVisibilityTimeoutMs: 30_000, DefaultMaxInFlightPerPartition: 1024, DefaultMaxAckedAheadPerPartition: 1024,
		},
		Metastore:       ms,
		Partitions:      partition.NewHashRoundRobin(),
		Schemas:         schema.NewJSONSchema(),
		ConsumerOffsets: offsets,
		Logs:            logs,
		Ingress:         ing,
		Logger:          log,
		Lifecycle:       lifecycle,
	})
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	lifecycle.MarkReady()
	run(cluster.NewProduceDispatcher(ing, ms, "", br, nil, log, cluster.ProduceDispatcherConfig{PollInterval: 5 * time.Millisecond}).Run)

	tg := &realTarget{t: t, ms: ms, br: br}
	deps := handlers.Deps{Broker: br, Logs: logs, Logger: log, MaxConsumeWait: time.Second}
	var auth *security.Authenticator
	if secured {
		auth = security.New(ms, log)
		tg.adminUser, tg.adminPass = "admin", randomClusterSecret(t, 16)
		hash, err := bcrypt.GenerateFromPassword([]byte(tg.adminPass), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		if err := ms.SeedRootUser(context.Background(), user.User{Username: tg.adminUser, PasswordHash: hash, Root: true}); err != nil {
			t.Fatalf("seed admin: %v", err)
		}
		if usersRoute {
			deps.Metastore = ms // enables /v1/users
		}
	}
	tg.srv = httptest.NewTLSServer(httpserver.NewRouterWithOptions(handlers.New(deps), log, nil, nil, auth, httpserver.DefaultRouterOptions()))
	tg.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tg.srv.Certificate().Raw}))
	t.Cleanup(func() {
		tg.srv.Close()
		cancel()
		for _, done := range stopped {
			<-done
		}
		_ = br.Close()
	})
	return tg
}

// url is the target's base URL.
func (tg *realTarget) url() string { return tg.srv.URL }

// createUser creates a user with a random password and the grants, and
// returns the password.
func (tg *realTarget) createUser(name string, grants ...user.Grant) string {
	tg.t.Helper()
	pass := randomClusterSecret(tg.t, 16)
	hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.MinCost)
	if err != nil {
		tg.t.Fatal(err)
	}
	if err := tg.ms.CreateUser(context.Background(), user.User{Username: name, PasswordHash: hash, Grants: grants}); err != nil {
		tg.t.Fatalf("create user %s: %v", name, err)
	}
	return pass
}

// adminDo sends one request as the target's admin (or anonymously on
// an unsecured target) and returns the status.
func (tg *realTarget) adminDo(method, path string, body any) int {
	tg.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			tg.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, tg.url()+path, rd)
	if err != nil {
		tg.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if tg.adminUser != "" {
		req.SetBasicAuth(tg.adminUser, tg.adminPass)
	}
	resp, err := tg.srv.Client().Do(req)
	if err != nil {
		tg.t.Fatalf("%s %s: %v", method, path, err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		tg.t.Logf("%s %s: %d %s", method, path, resp.StatusCode, b)
	}
	return resp.StatusCode
}

// createTopic creates a topic as admin and waits until its partitions
// are assigned (describe answers 200).
func (tg *realTarget) createTopic(name string, extra map[string]any) {
	tg.t.Helper()
	body := map[string]any{"name": name}
	for k, v := range extra {
		body[k] = v
	}
	if st := tg.adminDo(http.MethodPost, "/v1/topics", body); st != http.StatusCreated {
		tg.t.Fatalf("create topic %s: status %d", name, st)
	}
	deadline := time.Now().Add(10 * time.Second)
	for tg.adminDo(http.MethodGet, "/v1/topics/"+name, nil) != http.StatusOK {
		if time.Now().After(deadline) {
			tg.t.Fatalf("topic %s never became describable", name)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (tg *realTarget) String() string { return fmt.Sprintf("target(%s)", tg.url()) }

// pemOf PEM-encodes a DER certificate.
func pemOf(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
