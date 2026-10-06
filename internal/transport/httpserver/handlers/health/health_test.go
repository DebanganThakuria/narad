package health

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type fakeBroker struct {
	createTopicFn             func(context.Context, brokertopics.CreateOpts) (topic.Topic, error)
	increaseTopicPartitionsFn func(context.Context, string, int) (topic.Topic, error)
	updateTopicRetentionFn    func(context.Context, string, int64) (topic.Topic, error)
	updateTopicCapsFn         func(context.Context, string, *int64, *int64) (topic.Topic, error)
	updateTopicSchemaFn       func(context.Context, string, []byte) (topic.Topic, error)
	deleteTopicFn             func(context.Context, string) error
	getTopicFn                func(context.Context, string) (topic.Topic, error)
	getTopicDetailsFn         func(context.Context, string) (topic.Details, error)
	listTopicsFn              func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error)
	produceFn                 func(context.Context, string, string, []byte) (int64, int, error)
	consumeFn                 func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error)
	ackFn                     func(context.Context, string, consumer.Handle) error
	readyFn                   func(context.Context) error
}

func (f *fakeBroker) CreateTopic(ctx context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
	return f.createTopicFn(ctx, opts)
}

func (f *fakeBroker) IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error) {
	return f.increaseTopicPartitionsFn(ctx, name, newPartitions)
}

func (f *fakeBroker) UpdateTopicRetention(ctx context.Context, name string, retentionMs int64) (topic.Topic, error) {
	return f.updateTopicRetentionFn(ctx, name, retentionMs)
}

func (f *fakeBroker) UpdateTopicCaps(ctx context.Context, name string, maxInFlightPerPartition, maxAckedAheadPerPartition *int64) (topic.Topic, error) {
	return f.updateTopicCapsFn(ctx, name, maxInFlightPerPartition, maxAckedAheadPerPartition)
}

func (f *fakeBroker) UpdateTopicSchema(ctx context.Context, name string, schema []byte, _ int) (topic.Topic, error) {
	return f.updateTopicSchemaFn(ctx, name, schema)
}

func (f *fakeBroker) TopicSchemaHistory(context.Context, string) (topic.SchemaHistory, error) {
	return topic.SchemaHistory{}, nil
}

func (f *fakeBroker) DeleteTopic(ctx context.Context, name string) error {
	return f.deleteTopicFn(ctx, name)
}

func (f *fakeBroker) PurgeTopic(context.Context, string, string) error { return nil }

func (f *fakeBroker) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	return f.getTopicFn(ctx, name)
}

func (f *fakeBroker) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	return f.getTopicDetailsFn(ctx, name)
}

func (f *fakeBroker) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return f.listTopicsFn(ctx, opts)
}

func (f *fakeBroker) Produce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (int64, int, error) {
	return f.produceFn(ctx, topicName, key, payload)
}

func (f *fakeBroker) AcceptProduce(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
	return ingress.AcceptedProduce{}, nil
}

func (f *fakeBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	return 0, nil
}

func (f *fakeBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	return make([]int64, len(records)), nil
}

func (f *fakeBroker) Consume(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return f.consumeFn(ctx, topicName, opts)
}

func (f *fakeBroker) Ack(ctx context.Context, topicName string, handle consumer.Handle) error {
	return f.ackFn(ctx, topicName, handle)
}

func (f *fakeBroker) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }

func (f *fakeBroker) Ready(ctx context.Context) error {
	if f.readyFn == nil {
		return nil
	}
	return f.readyFn(ctx)
}

func (f *fakeBroker) Close() error { return nil }

func newTestSet(b broker.Broker) *handlers.Set {
	return newTestSetWithShutdownCtx(b, nil)
}

func newTestSetWithShutdownCtx(b broker.Broker, shutdownCtx context.Context) *handlers.Set {
	return handlers.New(handlers.Deps{
		Broker:         b,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConsumeWait: time.Second,
		ShutdownCtx:    shutdownCtx,
	})
}

func TestHealthzHandler(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()

	Healthz(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Healthz() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestHealthzHandlerReturnsUnavailableWhenShutdownStarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := newTestSetWithShutdownCtx(&fakeBroker{}, ctx)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()

	Healthz(s).ServeHTTP(res, req)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("Healthz() status = %d, want %d", res.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthzHandlerUsesBackgroundWhenShutdownCtxMissing(t *testing.T) {
	s := newTestSetWithShutdownCtx(&fakeBroker{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()

	Healthz(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Healthz() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestReadyzHandlerReturnsReady(t *testing.T) {
	s := newTestSet(&fakeBroker{readyFn: func(context.Context) error { return nil }})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	res := httptest.NewRecorder()

	Readyz(s).ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Readyz() status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestReadyzHandlerReturnsUnavailable(t *testing.T) {
	s := newTestSet(&fakeBroker{readyFn: func(context.Context) error { return errors.New("not ready") }})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	res := httptest.NewRecorder()

	Readyz(s).ServeHTTP(res, req)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("Readyz() status = %d, want %d", res.Code, http.StatusServiceUnavailable)
	}
}

func (f *fakeBroker) AttachChild(context.Context, string, string, int64) error { return nil }
func (f *fakeBroker) DetachChild(context.Context, string, string) error        { return nil }

func (f *fakeBroker) ReadFanoutSlab(context.Context, string, int, topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	return topic.FanoutSlab{}, nil
}

func (f *fakeBroker) PartitionTransferInfo(context.Context, string, int) (messaging.PartitionTransferInfo, error) {
	return messaging.PartitionTransferInfo{}, nil
}

func (f *fakeBroker) ReadPartitionSegment(context.Context, string, int, int64, int64, int64) ([]byte, error) {
	return nil, nil
}

func (f *fakeBroker) PauseProduceForHandoff(string, int, time.Duration) {}
func (f *fakeBroker) ResumeProduce(string, int)                         {}

func (f *fakeBroker) PrepareHandoff(context.Context, string, int, time.Duration) (messaging.PartitionTransferInfo, error) {
	return messaging.PartitionTransferInfo{}, nil
}

func (f *fakeBroker) ReclaimMovedPartition(context.Context, string, int) error { return nil }

func (f *fakeBroker) FanoutCursorStats(context.Context, string) ([]topic.FanoutCursorStat, error) {
	return nil, nil
}

func (f *fakeBroker) ExtendAck(context.Context, string, consumer.Handle) error { return nil }

func (f *fakeBroker) Nack(context.Context, string, consumer.Handle) error { return nil }

func (f *fakeBroker) ConsumeProbe(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, *brokermsg.ConsumeWaiter, error) {
	msg, found, err := f.Consume(ctx, topicName, opts)
	return msg, found, &brokermsg.ConsumeWaiter{}, err
}

func (f *fakeBroker) ConsumeWait(context.Context, *brokermsg.ConsumeWaiter, time.Duration, <-chan struct{}) (topic.Message, bool, bool, error) {
	return topic.Message{}, false, false, nil
}

// /readyz is a LIVE check: a node whose metastore has no leader in view
// (never admitted, removed from the voter set, quorum lost) answers 503
// even after startup finished, and a node in contact with a leader with
// a caught-up ownership view answers 200.
func TestReadyzConsultsMetastoreLiveness(t *testing.T) {
	readyBroker := &fakeBroker{readyFn: func(context.Context) error { return nil }}
	probe := func(ms *metastore.Store) int {
		t.Helper()
		s := handlers.New(handlers.Deps{
			Broker:         readyBroker,
			Metastore:      ms,
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			MaxConsumeWait: time.Second,
		})
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		res := httptest.NewRecorder()
		Readyz(s).ServeHTTP(res, req)
		return res.Code
	}

	leaderless, err := metastore.New(metastore.Config{
		NodeID: "unadmitted", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0", JoinOnly: true,
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = leaderless.Close() })
	if code := probe(leaderless); code != http.StatusServiceUnavailable {
		t.Fatalf("Readyz() on a leaderless node = %d, want %d", code, http.StatusServiceUnavailable)
	}

	solo, err := metastore.New(metastore.Config{
		NodeID: "solo", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = solo.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for solo.ClusterReady() != nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if code := probe(solo); code != http.StatusOK {
		t.Fatalf("Readyz() on an elected single node = %d, want %d", code, http.StatusOK)
	}
}

// The token protocol is a cluster-layer concern; these handler fakes
// only need to satisfy the interface.
func (f *fakeBroker) RegisterRemoteDemand(context.Context, string, brokermsg.RemoteDemand) error {
	return nil
}

func (f *fakeBroker) DropRemoteDemand(string, brokermsg.RemoteDemand) {}

func (*fakeBroker) NoteRemoteClaim(string) {}

// An expired Raft certificate is listed under "degraded" in a 200
// answer, never turned into a 503: one certificate usually serves every
// node, so it expires on all of them at once, and failing readiness
// would take every pod out of its Services. A healthy node's answer has
// no "degraded" field.
func TestReadyzListsDegradedConditionsWithoutFailing(t *testing.T) {
	readyBroker := &fakeBroker{readyFn: func(context.Context) error { return nil }}
	probe := func(ms *metastore.Store) (int, map[string]any) {
		t.Helper()
		s := handlers.New(handlers.Deps{
			Broker:         readyBroker,
			Metastore:      ms,
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			MaxConsumeWait: time.Second,
		})
		res := httptest.NewRecorder()
		Readyz(s).ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		var body map[string]any
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
			t.Fatalf("readyz body %q: %v", res.Body.String(), err)
		}
		return res.Code, body
	}
	waitReady := func(ms *metastore.Store) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for ms.ClusterReady() != nil && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
	}

	expired, err := metastore.New(metastore.Config{
		NodeID: "expired", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
		TLS: expiredRaftTLS(t),
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = expired.Close() })
	waitReady(expired)
	code, body := probe(expired)
	if code != http.StatusOK {
		t.Fatalf("readyz with an expired raft certificate = %d %v, want 200", code, body)
	}
	if degraded, _ := body["degraded"].([]any); body["status"] != "ready" || len(degraded) != 1 || degraded[0] != "raft_tls_certificate_expired" {
		t.Fatalf("readyz body with an expired raft certificate = %v, want status ready and degraded [raft_tls_certificate_expired]", body)
	}

	healthy, err := metastore.New(metastore.Config{
		NodeID: "healthy", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = healthy.Close() })
	waitReady(healthy)
	code, body = probe(healthy)
	if _, has := body["degraded"]; code != http.StatusOK || has || body["status"] != "ready" {
		t.Fatalf("readyz on a healthy node = %d %v, want 200 {\"status\":\"ready\"}", code, body)
	}
}

// expiredRaftTLS returns a Raft TLS config whose node certificate
// expired an hour ago, signed by a CA that is still valid.
func expiredRaftTLS(t *testing.T) *metastore.TLSConfig {
	t.Helper()
	now := time.Now()
	issue := func(tmpl, parent *x509.Certificate, pub *ecdsa.PublicKey, signer *ecdsa.PrivateKey) *x509.Certificate {
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
		if err != nil {
			t.Fatalf("create certificate: %v", err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("parse certificate: %v", err)
		}
		return cert
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore: now.Add(-48 * time.Hour), NotAfter: now.Add(365 * 24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	ca := issue(caTmpl, caTmpl, &caKey.PublicKey, caKey)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := issue(&x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "narad-node"},
		DNSNames:  []string{metastore.ClusterCertDNSName},
		NotBefore: now.Add(-48 * time.Hour), NotAfter: now.Add(-time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}, ca, &leafKey.PublicKey, caKey)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &metastore.TLSConfig{
		Certificate: tls.Certificate{Certificate: [][]byte{leaf.Raw}, PrivateKey: leafKey, Leaf: leaf},
		CAs:         pool,
	}
}
