package cluster

// Decrypt once, end to end: the remote child ships through the node's
// real credential cache, which opens the password the registry holds
// sealed under the cluster secret. The cache decrypts once for the
// credential version; from the target's first 202 on, any decrypt fails
// the test, and the link keeps shipping chunk after chunk.

import (
	"context"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/remote/sink"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// countingOpener counts decrypts and, once forbidden, fails the test on
// any further one.
type countingOpener struct {
	inner  remote.Opener
	opens  atomic.Int64
	forbid atomic.Pointer[testing.TB]
}

func (o *countingOpener) Open(env domremote.Envelope, ad remotecred.AssociatedData) ([]byte, error) {
	if tb := o.forbid.Load(); tb != nil {
		(*tb).Errorf("a credential was decrypted after the first 202: the send path must never decrypt")
	}
	o.opens.Add(1)
	return o.inner.Open(env, ad)
}

func (o *countingOpener) Fingerprint(kv, id string, pw []byte) (string, error) {
	return o.inner.Fingerprint(kv, id, pw)
}

func (o *countingOpener) KeyClass(kv string) string { return o.inner.KeyClass(kv) }

// decryptRig is a secure target, a source whose registry holds remote
// "b" for it sealed under the cluster secret, and the source's real
// credential cache over that registry with a counting opener.
type decryptRig struct {
	target *rigTarget
	src    *rigSource
	cache  *remote.Cache
	opener *countingOpener
	topic  topic.Topic
}

func newDecryptRig(tb testing.TB) *decryptRig {
	tb.Helper()
	target := newRigTarget(tb, true, "orders")
	tt := target.createTopic(tb, "orders", 3)
	src := newRigSource(tb, rigSourceOpts{partitions: 1})
	u, err := url.Parse(target.server.URL)
	if err != nil {
		tb.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	registerRemote(tb, src.store, domremote.Record{
		Name: "b", ID: "rid-b", URL: target.server.URL, Username: rigReplUser, CAPEM: target.caPEM,
		Limits: domremote.DefaultLimits(),
	}, rigReplPass)
	guard, err := remote.NewGuard(remote.GuardConfig{AllowedPorts: []int{port}, AllowAddresses: []string{"127.0.0.0/8"}})
	if err != nil {
		tb.Fatal(err)
	}
	opener := &countingOpener{}
	cache := remote.NewCache(remote.CacheConfig{
		Registry: src.store, Secrets: remote.Secrets{Current: rigClusterSecret}, Guard: guard,
		Posture: remote.Posture{SecurityEnabled: true}, Log: rigLogger(),
		Opener: func(salt []byte) (remote.Opener, error) {
			k, err := remotecred.NewKeyring(rigClusterSecret, "", salt)
			if err != nil {
				return nil, err
			}
			opener.inner = k
			return opener, nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	cache.Refresh()
	go cache.Run(ctx)
	if _, err := cache.Get("b"); err != nil {
		tb.Fatalf("the cache could not open the sealed record: %v", err)
	}
	return &decryptRig{target: target, src: src, cache: cache, opener: opener, topic: tt}
}

func TestRemoteChildDecryptsOnceEndToEnd(t *testing.T) {
	chunks := 10_000
	if testing.Short() {
		chunks = 500
	}
	dr := newDecryptRig(t)
	target, src, cache, opener, tt := dr.target, dr.src, dr.cache, dr.opener, dr.topic
	src.runner.SetRemotes(cache, 64<<20)
	// Eight lanes, keys spread over them: a slab splits into up to eight
	// chunks, one per lane.
	src.attach(t, "b", "orders", tt.ID, 8, 0, "")

	var tb testing.TB = t
	first := func() { opener.forbid.CompareAndSwap(nil, &tb) }
	target.faults.onAccept.Store(&first)
	src.start()
	defer src.stop()

	// Produce small commits back to back until the target accepted the
	// wanted number of chunks, so slabs stay small and chunks many.
	var want []topic.KeyedRecord
	deadline := time.Now().Add(5 * time.Minute)
	for seq := 0; target.faults.accepted.Load() < int64(chunks); seq += 32 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d chunks shipped", target.faults.accepted.Load(), chunks)
		}
		want = append(want, src.produce(t, 0, 32, 64, seq)...)
	}
	rg := &remoteRig{src: src, target: target}
	rg.waitDelivered(t, want, 60*time.Second)
	if n := opener.opens.Load(); n != 1 {
		t.Fatalf("%d decrypts for one credential version over %d chunks, want exactly 1", n, target.faults.accepted.Load())
	}
	t.Logf("%d chunks accepted with 1 decrypt", target.faults.accepted.Load())
}

// BenchmarkRemoteChunkSend is one chunk on the real send path: encode
// 100 records, look the remote up in the credential cache, send the
// batch produce to a local TLS target and read the answer. After the
// warm-up any decrypt fails the benchmark.
func BenchmarkRemoteChunkSend(b *testing.B) {
	dr := newDecryptRig(b)
	recs := make([]topic.KeyedRecord, 100)
	for i := range recs {
		recs[i] = topic.KeyedRecord{Key: "key-" + strconv.Itoa(i%13), Payload: []byte(`{"seq":` + strconv.Itoa(i) + `,"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`)}
	}
	path, err := remote.TopicPath("orders", "produce", "batch")
	if err != nil {
		b.Fatal(err)
	}
	var builder sink.ChunkBuilder
	send := func() {
		body, n := builder.Build(recs, len(recs), sink.MaxChunkBytes, func(int) bool { return false })
		if n != len(recs) {
			b.Fatalf("built %d of %d records into the chunk", n, len(recs))
		}
		e, err := dr.cache.Get("b")
		if err != nil {
			b.Fatal(err)
		}
		resp, err := e.Do(context.Background(), remote.Outbound{Method: "POST", Path: path, Body: body, ContentType: "application/json"})
		if err != nil {
			b.Fatal(err)
		}
		_, _ = remote.ReadBody(resp, remote.MaxReadAnswerBytes)
		if resp.StatusCode != 202 {
			b.Fatalf("chunk answered %d", resp.StatusCode)
		}
	}
	send()
	var tb testing.TB = b
	dr.opener.forbid.Store(&tb)
	b.ResetTimer()
	for range b.N {
		send()
	}
	b.StopTimer()
	if n := dr.opener.opens.Load(); n != 1 {
		b.Fatalf("%d decrypts, want 1", n)
	}
}
