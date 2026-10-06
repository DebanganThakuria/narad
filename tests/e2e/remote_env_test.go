package e2e

// Two single-node Narads in one process, as a remote child sees them: a
// source with the remote plane wired the way serve.go wires it, and a
// target served over TLS by its real router. Both run with security on.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	obsmetrics "github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

const remoteReplUser = "repl-from-a-7f3k9q"

// The pair's credentials are minted per test process, never literals:
// the users' passwords and the source cluster's secret, 32 random bytes
// in base64 as `openssl rand -base64 32` makes one.
var (
	remoteAdminPass     = remoteRandomString(18)
	remoteReplPass      = remoteRandomString(18)
	remoteClusterSecret = remoteRandomString(32)
)

func remoteRandomString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// narad is one in-process node: store, broker, dispatcher, fan-out
// runner, and its API.
type narad struct {
	t       testing.TB
	name    string
	store   *metastore.Store
	broker  broker.Broker
	logs    *runtime.Logs
	ingress *ingress.Manager
	runner  *cluster.FanoutRunner
	plane   *cluster.RemotePlane
	lookup  *remote.StaticLookup
	metrics *obsmetrics.Metrics
	server  *httptest.Server
	caPEM   string
	// down makes the API answer 503 in HTML, as an unreachable node
	// behind a load balancer would.
	down   atomic.Bool
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newNarad starts a node. tls serves the API over TLS (a target).
func newNarad(t testing.TB, name string, tls bool) *narad {
	t.Helper()
	dir := t.TempDir()
	ms, err := metastore.New(metastore.Config{NodeID: name, DataDir: dir + "/meta", BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !ms.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := ms.RegisterMember(context.Background(), metastore.Member{ID: name, Addr: "127.0.0.1:1", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	log := newTestLogger()
	opts := storage.Options{
		Codec: codec.NewNoopCodec(), FlushInterval: 5 * time.Millisecond, SegmentBytes: 64 << 20,
		Retention: storage.RetentionConfig{CheckInterval: time.Hour},
	}
	m := obsmetrics.New(prometheus.NewRegistry())
	logs := runtime.NewLogs(dir, opts, ms, m)
	lifecycle := runtime.NewLifecycle(logs)
	ing, err := ingress.OpenManager(dir, ingress.DefaultWALOptions())
	if err != nil {
		t.Fatal(err)
	}
	br, err := broker.New(broker.Deps{
		DataDir: dir, StorageOptions: opts,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions: 3, MaxPartitions: 64, DefaultRetentionMs: 3 * topic.MinRemoteSourceRetentionMs,
			DefaultVisibilityTimeoutMs: 30_000, DefaultMaxInFlightPerPartition: 1024, DefaultMaxAckedAheadPerPartition: 1024,
		},
		Metastore: ms, Partitions: partition.NewHashRoundRobin(), Schemas: schema.NewJSONSchema(),
		ConsumerOffsets: consumer.NewInFlight(capsResolver(ms), nil), Logs: logs, Ingress: ing, Logger: log,
		Lifecycle: lifecycle, Metrics: m,
	})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.MarkReady()
	n := &narad{t: t, name: name, store: ms, broker: br, logs: logs, ingress: ing, metrics: m, lookup: remote.NewStaticLookup()}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.wg.Go(func() {
		cluster.NewProduceDispatcher(ing, ms, "", br, nil, log, cluster.ProduceDispatcherConfig{PollInterval: 5 * time.Millisecond}).Run(ctx)
	})
	n.runner = cluster.NewFanoutRunner(ms, "", dir, br, nil, partition.NewHashRoundRobin(), m, log,
		cluster.FanoutConfig{Linger: time.Millisecond, ReconcileInterval: 25 * time.Millisecond})
	n.runner.SetRemotes(n.lookup, 64<<20)
	n.wg.Go(func() { n.runner.Run(ctx) })
	startControllerFor(ctx, &n.wg, ms)

	// The plane as serve.go wires it, with checks that ask the target
	// for its topic (package A's full checks are not part of this rig).
	n.plane = cluster.NewRemotePlane(ms, nil, nil, name, log)
	n.plane.Checks = &targetIDChecks{node: name, lookup: n.lookup}
	n.plane.Links = cluster.NewRemoteLinks(cluster.RemoteLinksDeps{
		Store: ms, Runner: n.runner, Plane: n.plane, Ingress: ing, Broker: br, Metrics: m, Log: log, SelfID: name,
	})

	auth := security.New(ms, log)
	seedTestAdmin(t, ms, "admin", remoteAdminPass)
	deps := handlers.Deps{
		Broker: overlayBroker{Broker: br, runner: n.runner}, Logs: logs, Logger: log, MaxConsumeWait: 2 * time.Second, Metastore: ms,
		Remote: handlers.RemoteDeps{Writer: n.plane}, BatchBodyBudget: 256 << 20,
	}
	router := httpserver.NewRouter(handlers.New(deps), log, m, nil, auth)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.down.Load() {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "<html>down</html>")
			return
		}
		router.ServeHTTP(w, r)
	})
	if tls {
		n.server = httptest.NewTLSServer(h)
		n.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: n.server.Certificate().Raw}))
	} else {
		n.server = httptest.NewServer(h)
	}
	t.Cleanup(func() {
		n.server.Close()
		cancel()
		n.wg.Wait()
		_ = logs.CloseAll()
		_ = ing.Close()
	})
	return n
}

func startControllerFor(ctx context.Context, wg *sync.WaitGroup, ms *metastore.Store) {
	wg.Go(func() {
		controllerNew(ms).Run(ctx)
	})
}

// addUser creates a user with grants.
func (n *narad) addUser(name, password string, grants ...user.Grant) {
	n.t.Helper()
	seedUser(n.t, n.store, name, password, grants)
}

// call sends a request as user (empty: no credentials) and returns the
// status and body.
func (n *narad) call(method, path, username, password string, body any) (int, []byte, http.Header) {
	n.t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		r = bytes.NewReader(b)
	case string:
		r = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			n.t.Fatal(err)
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, n.server.URL+path, r)
	if err != nil {
		n.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	// A named client, as every SDK and the CLI send: the cross-site
	// guard refuses batch consume and ack without it.
	req.Header.Set(httpserver.ClientHeader, "e2e-remote")
	if username != "" {
		req.SetBasicAuth(username, password)
	}
	client := n.server.Client()
	resp, err := client.Do(req)
	if err != nil {
		n.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, resp.Header
}

func (n *narad) admin(method, path string, body any) (int, []byte) {
	n.t.Helper()
	status, out, _ := n.call(method, path, "admin", remoteAdminPass, body)
	return status, out
}

// registerRemote adds remote name to the source's registry through Raft,
// as the remotes API does (the password sealed under the key the cluster
// secret and the salt derive, bound to the record), and its entry to the
// lookup.
func (n *narad) registerRemote(name string, target *narad, password string, cv uint64) *remote.Entry {
	n.t.Helper()
	e, err := remote.NewStaticEntry(remote.StaticEntryConfig{
		Name: name, RemoteID: "rid-" + name, URL: target.server.URL, Username: remoteReplUser,
		Password: domremote.NewSecret([]byte(password)), CAPEM: target.caPEM, CredentialVersion: cv,
	})
	if err != nil {
		n.t.Fatal(err)
	}
	rec := domremote.Record{Name: name, ID: "rid-" + name, URL: target.server.URL, Username: remoteReplUser, CAPEM: target.caPEM}
	if err := n.store.PutRemote(context.Background(), sealedPut(n.t, n.store, rec, password)); err != nil {
		n.t.Fatalf("register remote: %v", err)
	}
	n.lookup.Set(e)
	return e
}

// sealedPut is the create op for rec with password sealed; the store's
// first remote mints the salt.
func sealedPut(t testing.TB, ms *metastore.Store, rec domremote.Record, password string) metastore.PutRemoteOp {
	t.Helper()
	keys, err := ms.RemoteKeys()
	if err != nil {
		t.Fatal(err)
	}
	op := metastore.PutRemoteOp{SealedAtMs: time.Now().UnixMilli(), Actor: "admin", RequestID: "req-put-" + rec.Name}
	salt := keys.Salt
	if len(salt) == 0 {
		salt = make([]byte, remotecred.SaltBytes)
		if _, err := rand.Read(salt); err != nil {
			t.Fatal(err)
		}
		op.Salt = salt
	}
	ring, err := remotecred.NewKeyring(remoteClusterSecret, "", salt)
	if err != nil {
		t.Fatal(err)
	}
	tuple, err := domremote.TupleOf(rec)
	if err != nil {
		t.Fatal(err)
	}
	env, err := ring.Seal([]byte(password), remotecred.ADFor(tuple))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Fingerprint, err = ring.Fingerprint(env.KV, rec.ID, []byte(password)); err != nil {
		t.Fatal(err)
	}
	rec.Credential = env
	op.Record = rec
	return op
}

// targetIDChecks passes every attach and resume check from this one
// member with the target topic's ID, read with the remote's entry.
type targetIDChecks struct {
	node   string
	lookup remote.Lookup
}

func (c *targetIDChecks) CheckEverywhere(ctx context.Context, req remote.CheckRequest) ([]remote.NodeReport, error) {
	r := remote.NodeReport{Node: c.node, Result: "pass", CredentialVersion: req.CredentialVersion, Warnings: []string{}, TargetServesIDs: true}
	e, err := c.lookup.Get(req.Remote)
	if err != nil {
		r.Result, r.Class = "fail", topic.RemoteStateRemoteMissing
		return []remote.NodeReport{r}, nil
	}
	path, err := remote.TopicPath(req.Topic)
	if err != nil {
		return nil, err
	}
	resp, err := e.Do(ctx, remote.Outbound{Method: http.MethodGet, Path: path})
	if err != nil {
		r.Result, r.Class = "fail", topic.RemoteStateUnavailable
		return []remote.NodeReport{r}, nil
	}
	body, _ := remote.ReadBody(resp, remote.MaxReadAnswerBytes)
	switch resp.StatusCode {
	case http.StatusOK:
		var tp topic.Topic
		_ = json.Unmarshal(body, &tp)
		r.TargetID = tp.ID
	case http.StatusUnauthorized:
		r.Result, r.Class = "fail", topic.RemoteStateAuthFailed
	case http.StatusNotFound:
		r.Result, r.Class = "fail", topic.RemoteStateTargetMissing
	default:
		r.Result, r.Class = "fail", fmt.Sprintf("status_%d", resp.StatusCode)
	}
	return []remote.NodeReport{r}, nil
}

func (c *targetIDChecks) RequirePosture(context.Context) error { return nil }

func (c *targetIDChecks) RequireReleases(context.Context) error { return nil }

// remotePair is a source with remote "b" pointing at a target.
type remotePair struct {
	src, dst *narad
	entry    *remote.Entry
}

func newRemotePair(t testing.TB) *remotePair {
	t.Helper()
	dst := newNarad(t, "target-0", true)
	src := newNarad(t, "source-0", false)
	dst.addUser(remoteReplUser, remoteReplPass, user.Grant{Action: user.ActionProduce, Patterns: []string{"orders"}})
	src.addUser("olivia", oliviaPass, user.Grant{Action: user.ActionCreate, Patterns: []string{"orders*"}})
	src.addUser("rita", ritaPass, user.Grant{Action: user.ActionConsume, Patterns: []string{"orders*"}})
	dst.addUser("bob", bobPass, user.Grant{Action: user.ActionConsume, Patterns: []string{"orders"}})
	if status, body := dst.admin(http.MethodPost, "/v1/topics", map[string]any{"name": "orders", "partitions": 3}); status != http.StatusCreated {
		t.Fatalf("target topic: %d %s", status, body)
	}
	status, body, _ := src.call(http.MethodPost, "/v1/topics", "olivia", oliviaPass,
		map[string]any{"name": "orders", "partitions": 3, "retention_ms": 3 * topic.MinRemoteSourceRetentionMs})
	if status != http.StatusCreated {
		t.Fatalf("source topic: %d %s", status, body)
	}
	e := src.registerRemote("b", dst, remoteReplPass, 1)
	for _, n := range []*narad{src, dst} {
		awaitAssigned(t, n.store, "orders")
	}
	return &remotePair{src: src, dst: dst, entry: e}
}

func awaitAssigned(t testing.TB, ms *metastore.Store, name string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		tp, err := ms.GetTopic(context.Background(), name)
		if err == nil {
			as, _ := ms.ListAssignments(name)
			if len(as) == tp.Partitions {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("partitions of %s never assigned", name)
}

func controllerNew(ms *metastore.Store) *controller.Controller {
	return controller.New(ms, controller.Config{ReconcileInterval: 50 * time.Millisecond, DeadTimeout: e2eMemberDeadTimeout})
}

// seedUser creates a user with grants directly in the metastore.
func seedUser(t testing.TB, ms *metastore.Store, name, password string, grants []user.Grant) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateUser(context.Background(), user.User{Username: name, PasswordHash: hash, Grants: grants}); err != nil {
		t.Fatalf("create user %s: %v", name, err)
	}
}

// registerRemoteEntry swaps the entry of an already registered remote,
// as a credential change would once it reaches this node's cache.
func (n *narad) registerRemoteEntry(name string, target *narad, password string, cv uint64) {
	n.t.Helper()
	e, err := remote.NewStaticEntry(remote.StaticEntryConfig{
		Name: name, RemoteID: "rid-" + name, URL: target.server.URL, Username: remoteReplUser,
		Password: domremote.NewSecret([]byte(password)), CAPEM: target.caPEM, CredentialVersion: cv,
	})
	if err != nil {
		n.t.Fatal(err)
	}
	n.lookup.Set(e)
}

// overlayBroker adds the runner's remote link state to the cursor
// stats, as the cluster router does in serve.go.
type overlayBroker struct {
	broker.Broker
	runner *cluster.FanoutRunner
}

func (b overlayBroker) AcceptProduceBatch(ctx context.Context, topicName string, msgs []messaging.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	return b.Broker.(broker.BatchProducer).AcceptProduceBatch(ctx, topicName, msgs)
}

func (b overlayBroker) ConsumeBatch(ctx context.Context, topicName string, opts messaging.ConsumeOpts, maxN int, dst []topic.Message) ([]topic.Message, *messaging.ConsumeWaiter, error) {
	return b.Broker.(broker.BatchConsumer).ConsumeBatch(ctx, topicName, opts, maxN, dst)
}

func (b overlayBroker) FanoutCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, error) {
	stats, err := b.Broker.FanoutCursorStats(ctx, parent)
	if err != nil {
		return nil, err
	}
	return b.runner.OverlayRemoteCursorStats(parent, stats), nil
}
