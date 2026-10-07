package cluster

// The remote child test rig: a source node (a real metastore, messaging
// engine and partition logs, the fan-out runner with the remote send
// path) and a target, a whole single-node Narad served over TLS by its
// real router, optionally behind a fault switch and a latency proxy.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func rigLogger() *slog.Logger {
	if os.Getenv("NARAD_RIG_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// rigStore is a single-node metastore with an elected leader.
func rigStore(tb testing.TB, id string) *metastore.Store {
	tb.Helper()
	ms, err := metastore.New(metastore.Config{NodeID: id, DataDir: tb.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		tb.Fatalf("metastore.New: %v", err)
	}
	tb.Cleanup(func() { _ = ms.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !ms.IsLeader() {
		if time.Now().After(deadline) {
			tb.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ms
}

// rigFaults lets a test make the target answer as something else would.
type rigFaults struct {
	// mode: "" (pass), "down" (503 HTML), "edge403" (HTML 403),
	// "reset" (hang up without answering), "slow" (sleep then pass),
	// "nobatch" (the batch route answers Go's 404), "v310" (the
	// children listing as v3.1.0 answers it: no parent_id, no remote),
	// "otherID" (the listing names another topic ID, as a different
	// cluster behind the same name would, after slowDelay; chunks
	// still land),
	// "nolisting" (the children listing answers 503 HTML; chunks
	// still land), "throttled" (429 with retryAfter as Retry-After,
	// after slowDelay).
	mode atomic.Value
	// onReset, when set, is told of each request mode "reset" is about
	// to hang up on, and whether it is a batch.
	onReset atomic.Pointer[func(batch bool)]
	// posted counts batch requests that reached the target at all,
	// whatever the mode answered them.
	posted atomic.Int64
	// batches counts batch requests that reached the real router and
	// were accepted.
	accepted atomic.Int64
	batches  atomic.Int64
	probes   atomic.Int64
	listings atomic.Int64
	// tooMany counts chunks refused in mode "max100".
	tooMany   atomic.Int64
	onAccept  atomic.Pointer[func()]
	slowDelay time.Duration
	// retryAfter is mode "throttled"'s Retry-After header.
	retryAfter string
}

func (f *rigFaults) set(mode string) { f.mode.Store(mode) }

func (f *rigFaults) get() string {
	m, _ := f.mode.Load().(string)
	return m
}

func (f *rigFaults) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batch := strings.HasSuffix(r.URL.Path, "/produce/batch")
		if strings.HasSuffix(r.URL.Path, "/children") {
			f.listings.Add(1)
		}
		if batch {
			f.posted.Add(1)
		}
		switch f.get() {
		case "down":
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "<html>down</html>")
			return
		case "throttled":
			time.Sleep(f.slowDelay)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", f.retryAfter)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":"rate limited"}`)
			return
		case "nolisting":
			if strings.HasSuffix(r.URL.Path, "/children") {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "<html>listing down</html>")
				return
			}
		case "edge403":
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "<h1>Forbidden by policy</h1>")
			return
		case "reset":
			if fn := f.onReset.Load(); fn != nil {
				(*fn)(batch)
			}
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					_ = c.Close()
					return
				}
			}
		case "nobatch":
			if batch {
				http.NotFound(w, r)
				return
			}
		case "max100":
			// v3.1.0's batch produce: at most 100 messages per request.
			if batch && r.Method == http.MethodPost && r.Header.Get("Content-Encoding") == "" {
				raw, _ := io.ReadAll(r.Body)
				var body struct {
					Messages []json.RawMessage `json:"messages"`
				}
				if json.Unmarshal(raw, &body) == nil && len(body.Messages) > 100 {
					f.tooMany.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":"too many messages: more than 100 (max 100)"}`)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(raw))
			}
		case "slow":
			time.Sleep(f.slowDelay)
		case "v310":
			if strings.HasSuffix(r.URL.Path, "/children") && r.Method == http.MethodGet {
				serveListingEdited(w, r, next, asV310Listing)
				return
			}
		case "otherID":
			if strings.HasSuffix(r.URL.Path, "/children") && r.Method == http.MethodGet {
				time.Sleep(f.slowDelay)
				serveListingEdited(w, r, next, func(listing map[string]any) { listing["parent_id"] = "another-clusters-id" })
				return
			}
		}
		if !batch {
			next.ServeHTTP(w, r)
			return
		}
		f.batches.Add(1)
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.status == http.StatusBadRequest {
			f.probes.Add(1)
		}
		if rec.status == http.StatusAccepted {
			f.accepted.Add(1)
			if fn := f.onAccept.Load(); fn != nil {
				(*fn)()
			}
		}
	})
}

// asV310Listing strips a children listing of the fields v3.1.0 does not
// serve: the top-level parent_id and each child's remote.
func asV310Listing(listing map[string]any) {
	delete(listing, "parent_id")
	if children, ok := listing["children"].([]any); ok {
		for _, c := range children {
			if m, ok := c.(map[string]any); ok {
				delete(m, "remote")
			}
		}
	}
}

// serveListingEdited answers a children listing as the target does,
// edited by edit when it is a 200.
func serveListingEdited(w http.ResponseWriter, r *http.Request, next http.Handler, edit func(map[string]any)) {
	rec := httptest.NewRecorder()
	next.ServeHTTP(rec, r)
	body := rec.Body.Bytes()
	var listing map[string]any
	if rec.Code == http.StatusOK && json.Unmarshal(body, &listing) == nil {
		edit(listing)
		body, _ = json.Marshal(listing)
	}
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.Header().Del("Content-Length")
	w.WriteHeader(rec.Code)
	_, _ = w.Write(body)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// rigTarget is a single-node Narad over TLS: the other cluster.
type rigTarget struct {
	store  *metastore.Store
	broker broker.Broker
	logs   *runtime.Logs
	// ingress is the target's ingress WAL: a 202 means a batch is
	// durable there, and its dispatcher commits it to the partition logs
	// a moment later.
	ingress *ingress.Manager
	server  *httptest.Server
	faults  *rigFaults
	caPEM   string
	secure  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

const rigReplUser = "repl-from-a"

// The rig's credentials are minted per test process, never literals:
// the replicator's and the target admin's passwords, and the source
// cluster's secret, 32 random bytes in base64 as `openssl rand -base64
// 32` makes one.
var (
	rigReplPass      = rigRandomString(18)
	rigAdminPass     = rigRandomString(18)
	rigClusterSecret = rigRandomString(32)
)

func rigRandomString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// newRigTarget starts a target. With secure, it enforces Basic auth: a
// replicator user with produce on grantTopics, and an admin.
func newRigTarget(tb testing.TB, secure bool, grantTopics ...string) *rigTarget {
	tb.Helper()
	dir := tb.TempDir()
	ms := rigStore(tb, "target-0")
	if err := ms.RegisterMember(context.Background(), metastore.Member{ID: "target-0", Addr: "127.0.0.1:1", Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
		tb.Fatal(err)
	}
	log := rigLogger()
	opts := storage.Options{
		Codec: codec.NewNoopCodec(), FlushInterval: 5 * time.Millisecond, SegmentBytes: 64 << 20,
		Retention: storage.RetentionConfig{CheckInterval: time.Hour},
	}
	logs := runtime.NewLogs(dir, opts, ms, nil)
	lifecycle := runtime.NewLifecycle(logs)
	ing, err := ingress.OpenManager(dir, ingress.DefaultWALOptions())
	if err != nil {
		tb.Fatal(err)
	}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1024, MaxAckedAhead: 1024}, nil
	}, nil)
	br, err := broker.New(broker.Deps{
		DataDir: dir, StorageOptions: opts,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions: 1, MaxPartitions: 64, DefaultRetentionMs: 7 * 24 * 3600 * 1000,
			DefaultVisibilityTimeoutMs: 30_000, DefaultMaxInFlightPerPartition: 1024, DefaultMaxAckedAheadPerPartition: 1024,
		},
		Metastore: ms, Partitions: partition.NewHashRoundRobin(), Schemas: schema.NewJSONSchema(),
		ConsumerOffsets: offsets, Logs: logs, Ingress: ing, Logger: log, Lifecycle: lifecycle,
	})
	if err != nil {
		tb.Fatal(err)
	}
	lifecycle.MarkReady()
	t := &rigTarget{store: ms, broker: br, logs: logs, ingress: ing, faults: &rigFaults{}, secure: secure}
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	t.wg.Go(func() {
		NewProduceDispatcher(ing, ms, "", br, nil, log, ProduceDispatcherConfig{PollInterval: 5 * time.Millisecond}).Run(ctx)
	})
	t.wg.Go(func() {
		controller.New(ms, controller.Config{ReconcileInterval: 50 * time.Millisecond, DeadTimeout: 5 * time.Minute}).Run(ctx)
	})
	deps := handlers.Deps{Broker: br, Logs: logs, Logger: log, MaxConsumeWait: 2 * time.Second}
	var auth *security.Authenticator
	if secure {
		auth = security.New(ms, log)
		deps.Metastore = ms
		rigSeedUser(tb, ms, "admin", rigAdminPass, user.Grant{Action: user.ActionAdmin}, true)
		var patterns []string
		patterns = append(patterns, grantTopics...)
		rigSeedUser(tb, ms, rigReplUser, rigReplPass, user.Grant{Action: user.ActionProduce, Patterns: patterns}, false)
	}
	router := httpserver.NewRouter(handlers.New(deps), log, nil, nil, auth)
	t.server = httptest.NewTLSServer(t.faults.wrap(router))
	t.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: t.server.Certificate().Raw}))
	tb.Cleanup(func() {
		t.server.Close()
		cancel()
		t.wg.Wait()
		_ = logs.CloseAll()
		_ = ing.Close()
	})
	return t
}

func rigSeedUser(tb testing.TB, ms *metastore.Store, name, password string, grant user.Grant, root bool) {
	tb.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		tb.Fatal(err)
	}
	u := user.User{Username: name, PasswordHash: hash, Grants: []user.Grant{grant}, Root: root}
	if root {
		err = ms.SeedRootUser(context.Background(), u)
	} else {
		err = ms.CreateUser(context.Background(), u)
	}
	if err != nil {
		tb.Fatalf("seed user %s: %v", name, err)
	}
}

// createTopic creates a topic on the target.
func (t *rigTarget) createTopic(tb testing.TB, name string, partitions int) topic.Topic {
	tb.Helper()
	tp, err := t.broker.CreateTopic(context.Background(), brokerCreateOpts(name, partitions))
	if err != nil {
		tb.Fatalf("target CreateTopic(%s): %v", name, err)
	}
	return tp
}

// awaitDispatched waits until the target's dispatcher committed every
// batch it answered 202 for, so its partition logs hold everything it
// accepted.
func (t *rigTarget) awaitDispatched(tb testing.TB, d time.Duration) {
	tb.Helper()
	rigWait(tb, "the target's dispatch backlog to drain", d, func() bool { return t.ingress.DispatchBacklog() == 0 })
}

// records returns every committed record of the target topic.
func (t *rigTarget) records(tb testing.TB, name string) []topic.KeyedRecord {
	tb.Helper()
	tp, err := t.store.GetTopic(context.Background(), name)
	if err != nil {
		return nil
	}
	var out []topic.KeyedRecord
	for p := range tp.Partitions {
		log, err := t.logs.Get(name, p)
		if err != nil {
			tb.Fatalf("target logs.Get(%s,%d): %v", name, p, err)
		}
		for off := int64(0); off < log.HighWatermark(); off++ {
			key, at, payload, err := log.ReadKeyed(off)
			if err != nil {
				tb.Fatalf("target ReadKeyed: %v", err)
			}
			out = append(out, topic.KeyedRecord{Key: key, Offset: off, CommittedAtUnixMs: at, Payload: payload})
		}
	}
	return out
}

// rigSource is the source node: a parent topic, a whole broker (with
// its ingress WAL, so deletes see a dispatch backlog), the runner with
// the remote send path, and the leader side of remote children.
type rigSource struct {
	store   *metastore.Store
	broker  broker.Broker
	ingress *ingress.Manager
	logs    *runtime.Logs
	dataDir string
	runner  *FanoutRunner
	lookup  *remote.StaticLookup
	metrics *metrics.Metrics
	plane   *RemotePlane
	links   *RemoteLinks
	checks  *fakeCheckRunner
	// runMu guards cancel and done: a test may stop the runner from a
	// fault hook's goroutine.
	runMu  sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

type rigSourceOpts struct {
	partitions  int
	retentionMs int64
	heldBudget  int64
	stallRetry  time.Duration
}

func newRigSource(tb testing.TB, o rigSourceOpts) *rigSource {
	tb.Helper()
	ctx := context.Background()
	if o.partitions == 0 {
		o.partitions = 1
	}
	if o.retentionMs == 0 {
		o.retentionMs = topic.MinRemoteSourceRetentionMs
	}
	if o.heldBudget == 0 {
		o.heldBudget = 64 << 20
	}
	store := rigStore(tb, "node-self")
	// The actor every remote write of these rigs names: the leader runs
	// each write as that user, looked up in its own replica.
	rigSeedUser(tb, store, "alice", rigRandomString(18), user.Grant{Action: user.ActionAdmin}, false)
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "127.0.0.1:1", Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
		tb.Fatal(err)
	}
	dataDir := tb.TempDir()
	if err := store.CreateTopic(ctx, topic.Topic{
		Name: "orders", ID: "src-orders-id", Partitions: o.partitions, RetentionMs: o.retentionMs,
		VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
	}); err != nil {
		tb.Fatal(err)
	}
	for p := range o.partitions {
		if err := store.AssignPartition(ctx, "orders", p, "node-self"); err != nil {
			tb.Fatal(err)
		}
	}
	log := rigLogger()
	opts := storage.Options{FlushInterval: time.Millisecond}
	logs := runtime.NewLogs(dataDir, opts, store, nil)
	lifecycle := runtime.NewLifecycle(logs)
	ing, err := ingress.OpenManager(dataDir, ingress.DefaultWALOptions())
	if err != nil {
		tb.Fatal(err)
	}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	br, err := broker.New(broker.Deps{
		DataDir: dataDir, StorageOptions: opts,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions: 3, MaxPartitions: 64, DefaultRetentionMs: topic.MinRemoteSourceRetentionMs,
			DefaultVisibilityTimeoutMs: 30_000, DefaultMaxInFlightPerPartition: 64, DefaultMaxAckedAheadPerPartition: 64,
		},
		Metastore: store, Partitions: partition.NewHashRoundRobin(), Schemas: schema.NewAlwaysValid(),
		ConsumerOffsets: offsets, Logs: logs, Ingress: ing, Logger: log, Lifecycle: lifecycle, SelfID: "node-self",
	})
	if err != nil {
		tb.Fatal(err)
	}
	lifecycle.MarkReady()
	tb.Cleanup(func() {
		_ = logs.CloseAll()
		_ = ing.Close()
	})
	m := metrics.New(prometheusRegistry())
	s := &rigSource{store: store, broker: br, ingress: ing, logs: logs, dataDir: dataDir, metrics: m, lookup: remote.NewStaticLookup()}
	s.runner = NewFanoutRunner(store, "node-self", dataDir, br, nil, partition.NewHashRoundRobin(), m, log,
		FanoutConfig{Linger: time.Millisecond, ReconcileInterval: 20 * time.Millisecond})
	s.runner.SetRemotes(s.lookup, o.heldBudget)
	if o.stallRetry > 0 {
		s.runner.remote.stallRetry = o.stallRetry
	}
	s.runner.remote.lagRefresh = 200 * time.Millisecond
	s.plane = NewRemotePlane(store, nil, nil, "node-self", log)
	s.checks = &fakeCheckRunner{}
	s.plane.Checks = s.checks
	s.links = NewRemoteLinks(RemoteLinksDeps{Store: store, Runner: s.runner, Plane: s.plane, Ingress: ing, Broker: br, Metrics: m, Log: log, SelfID: "node-self"})
	s.plane.Links = s.links
	return s
}

// fakeCheckRunner stands in for package A's checks: every member passes
// with the target ID it is told, unless fail names a class.
type fakeCheckRunner struct {
	mu       sync.Mutex
	targetID string
	fail     string
	posture  error
	releases error
	calls    int
	// last is the most recent check request.
	last remote.CheckRequest
	// blind: no host allowlist on the node; hereCalls counts CheckHere.
	blind     bool
	hereCalls int
}

func (f *fakeCheckRunner) AllowlistConfigured() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.blind
}

func (f *fakeCheckRunner) CheckHere(ctx context.Context, req remote.CheckRequest) ([]remote.NodeReport, error) {
	f.mu.Lock()
	f.hereCalls++
	f.calls--
	f.mu.Unlock()
	return f.CheckEverywhere(ctx, req)
}

func (f *fakeCheckRunner) CheckEverywhere(_ context.Context, req remote.CheckRequest) ([]remote.NodeReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.last = req
	r := remote.NodeReport{Node: "node-self", Result: "pass", CredentialVersion: req.CredentialVersion, TargetID: f.targetID, TargetServesIDs: f.targetID != "", Warnings: []string{}}
	if f.fail != "" {
		r.Result, r.Class = "fail", f.fail
	}
	return []remote.NodeReport{r}, nil
}

func (f *fakeCheckRunner) lastRequest() remote.CheckRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

func (f *fakeCheckRunner) RequirePosture(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.posture
}

func (f *fakeCheckRunner) RequireReleases(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releases
}

// entryFor builds a static entry for target, with password and dial.
func entryFor(tb testing.TB, target *rigTarget, name, password string, cv uint64, limits domremote.Limits, dial func(context.Context, string, string) (net.Conn, error)) *remote.Entry {
	tb.Helper()
	e, err := remote.NewStaticEntry(remote.StaticEntryConfig{
		Name: name, RemoteID: "rid-" + name, URL: target.server.URL, Username: rigReplUser,
		Password: domremote.NewSecret([]byte(password)), CAPEM: target.caPEM, CredentialVersion: cv,
		Limits: limits, Dial: dial,
	})
	if err != nil {
		tb.Fatalf("NewStaticEntry: %v", err)
	}
	return e
}

// register puts the remote for target in the source's registry, its
// password sealed, and its entry in the lookup.
func (s *rigSource) register(tb testing.TB, e *remote.Entry, target *rigTarget, password string) {
	tb.Helper()
	registerRemote(tb, s.store, domremote.Record{
		Name: e.Name(), ID: e.RemoteID(), URL: target.server.URL, Username: rigReplUser, CAPEM: target.caPEM,
	}, password)
	s.lookup.Set(e)
}

// attach creates the remote child "orders-to-b" of orders.
func (s *rigSource) attach(tb testing.TB, remoteName, remoteTopic, targetID string, lanes int, delayMs int64, from string) topic.Topic {
	tb.Helper()
	ctx := context.Background()
	offsets, err := s.runner.AttachOffsetsMode(ctx, "orders", from)
	if err != nil {
		tb.Fatalf("AttachOffsetsMode: %v", err)
	}
	if err := s.store.AttachRemoteChild(ctx, metastore.AttachRemoteChildOp{
		Parent: "orders", ParentID: "src-orders-id", Stub: "orders-to-b", DelayMs: delayMs, Offsets: offsets,
		Remote: topic.RemoteLink{Name: remoteName, Topic: remoteTopic, TargetID: targetID, Lanes: lanes, From: from},
	}); err != nil {
		tb.Fatalf("AttachRemoteChild: %v", err)
	}
	stub, err := s.store.GetTopic(ctx, "orders-to-b")
	if err != nil {
		tb.Fatal(err)
	}
	return stub
}

func (s *rigSource) start() {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.cancel, s.done = cancel, done
	go func() {
		defer close(done)
		s.runner.Run(ctx)
	}()
}

// stop cancels the runner and waits for it to return.
func (s *rigSource) stop() {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}
}

// running reports whether the runner is started and not stopped; a
// stop in progress holds it until the runner returned.
func (s *rigSource) running() bool {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	return s.cancel != nil
}

// produce commits n records to the parent partition p. Keys cycle over
// keyspace; keyspace 0 produces keyless records.
func (s *rigSource) produce(tb testing.TB, p, n, keyspace, seqBase int) []topic.KeyedRecord {
	tb.Helper()
	recs := make([]ingress.ProduceRecord, 0, n)
	out := make([]topic.KeyedRecord, 0, n)
	for i := range n {
		key := ""
		if keyspace > 0 {
			key = fmt.Sprintf("key-%d", (seqBase+i)%keyspace)
		}
		payload := fmt.Appendf(nil, `{"seq":%d,"p":%d}`, seqBase+i, p)
		recs = append(recs, ingress.ProduceRecord{Topic: "orders", Key: key, TargetPartition: p, Payload: payload})
		out = append(out, topic.KeyedRecord{Key: key, Payload: payload})
	}
	if _, err := s.broker.CommitAcceptedProduceBatch(context.Background(), recs); err != nil {
		tb.Fatalf("produce: %v", err)
	}
	return out
}

// cursorState reads the running remote cursor's snapshot.
func (s *rigSource) cursorState(p int) (remoteCursorSnapshot, bool) {
	c := s.runner.remoteCursorFor("orders", p, "orders-to-b")
	if c == nil {
		return remoteCursorSnapshot{}, false
	}
	return c.snapshot(), true
}

// cursorOffset reads the persisted cursor of the stub on partition p.
func (s *rigSource) cursorOffset(tb testing.TB, p int) int64 {
	tb.Helper()
	cur, ok, err := storage.ReadFanoutCursor(storage.TopicPartitionDir(s.dataDir, "orders", p), "orders-to-b")
	if err != nil || !ok {
		return -1
	}
	return cur.NextOffset
}

func rigWait(tb testing.TB, what string, d time.Duration, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("timed out after %s waiting for %s", d, what)
}

// payloadSet is the multiset of payloads.
func payloadSet(recs []topic.KeyedRecord) map[string]int {
	out := map[string]int{}
	for _, r := range recs {
		out[string(r.Payload)]++
	}
	return out
}

// missing lists the wanted payloads the target does not hold.
func missing(want []topic.KeyedRecord, got []topic.KeyedRecord) []string {
	have := payloadSet(got)
	var out []string
	for _, r := range want {
		if have[string(r.Payload)] == 0 {
			out = append(out, string(r.Payload))
		}
	}
	sort.Strings(out)
	return out
}

func brokerCreateOpts(name string, partitions int) brokertopics.CreateOpts {
	return brokertopics.CreateOpts{Name: name, Partitions: partitions}
}

func prometheusRegistry() *prometheus.Registry { return prometheus.NewRegistry() }

type userGrant = user.Grant

func (rg *remoteRig) setState(tb testing.TB, op metastore.RemoteChildStateOp) {
	tb.Helper()
	stub, err := rg.src.store.GetTopic(context.Background(), "orders-to-b")
	if err != nil {
		tb.Fatal(err)
	}
	op.Parent, op.Stub, op.Epoch = "orders", "orders-to-b", stub.AttachEpoch
	if err := rg.src.store.SetRemoteChildState(context.Background(), op); err != nil {
		tb.Fatalf("SetRemoteChildState: %v", err)
	}
}

// producePayload commits one record with the given key and payload.
func (s *rigSource) producePayload(tb testing.TB, p int, key string, payload []byte) {
	tb.Helper()
	if _, err := s.broker.CommitAcceptedProduceBatch(context.Background(), []ingress.ProduceRecord{
		{Topic: "orders", Key: key, TargetPartition: p, Payload: payload},
	}); err != nil {
		tb.Fatalf("produce: %v", err)
	}
}

func counterValue(c prometheus.Collector) float64 { return testutil.ToFloat64(c) }

func withRetention(tb testing.TB, ms *metastore.Store, name string, retentionMs int64) topic.Topic {
	tb.Helper()
	tp, err := ms.GetTopic(context.Background(), name)
	if err != nil {
		tb.Fatal(err)
	}
	tp.RetentionMs = retentionMs
	return tp
}

// countingLookup counts the lookups and the distinct entries returned.
type countingLookup struct {
	remote.Lookup
	gets    atomic.Int64
	mu      sync.Mutex
	entries map[*remote.Entry]struct{}
}

func (c *countingLookup) Get(name string) (*remote.Entry, error) {
	c.gets.Add(1)
	e, err := c.Lookup.Get(name)
	if e != nil {
		c.mu.Lock()
		if c.entries == nil {
			c.entries = map[*remote.Entry]struct{}{}
		}
		c.entries[e] = struct{}{}
		c.mu.Unlock()
	}
	return e, err
}

func (c *countingLookup) distinct() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func brokerConsumeOpts(p *int) messaging.ConsumeOpts { return messaging.ConsumeOpts{Partition: p} }

func handleOf(tb testing.TB, receipt string) consumer.Handle {
	tb.Helper()
	h, err := consumer.DecodeHandle(receipt)
	if err != nil {
		tb.Fatal(err)
	}
	return h
}

// registerRemote puts rec in the registry through Raft the way the
// remotes API does: the password sealed under the key the cluster secret
// and the cluster's salt derive, bound to the record's name, id, URL,
// username and trust anchor, with its fingerprint. The first remote of
// a store mints the salt.
func registerRemote(tb testing.TB, ms *metastore.Store, rec domremote.Record, password string) {
	tb.Helper()
	keys, err := ms.RemoteKeys()
	if err != nil {
		tb.Fatal(err)
	}
	op := metastore.PutRemoteOp{SealedAtMs: time.Now().UnixMilli(), Actor: "admin", RequestID: "req-put-" + rec.Name}
	salt := keys.Salt
	if len(salt) == 0 {
		salt = make([]byte, remotecred.SaltBytes)
		if _, err := rand.Read(salt); err != nil {
			tb.Fatal(err)
		}
		op.Salt = salt
	}
	ring, err := remotecred.NewKeyring(rigClusterSecret, "", salt)
	if err != nil {
		tb.Fatal(err)
	}
	tuple, err := domremote.TupleOf(rec)
	if err != nil {
		tb.Fatal(err)
	}
	env, err := ring.Seal([]byte(password), remotecred.ADFor(tuple))
	if err != nil {
		tb.Fatal(err)
	}
	fp, err := ring.Fingerprint(env.KV, rec.ID, []byte(password))
	if err != nil {
		tb.Fatal(err)
	}
	rec.Credential, rec.Fingerprint = env, fp
	op.Record = rec
	if err := ms.PutRemote(context.Background(), op); err != nil {
		tb.Fatalf("PutRemote(%s): %v", rec.Name, err)
	}
}
