package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP7bMetastore is a concurrency-safe metastore fake with per-topic
// versions, like *metastore.Store: the Get fast path consults
// TopicVersion on every call in production, so the benchmarks and the
// lock tests run with it. getTopicGate, when set for a name, makes
// GetTopic for that name block until the channel is closed.
type zzWP7bMetastore struct {
	metastore.Metastore
	mu           sync.Mutex
	topics       map[string]topic.Topic
	versions     map[string]*atomic.Uint64
	getTopicGate map[string]chan struct{}
	getTopicIn   chan string
}

func newZZWP7bMetastore() *zzWP7bMetastore {
	return &zzWP7bMetastore{
		topics:       map[string]topic.Topic{},
		versions:     map[string]*atomic.Uint64{},
		getTopicGate: map[string]chan struct{}{},
	}
}

func (m *zzWP7bMetastore) put(t topic.Topic) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics[t.Name] = t
	v := m.versions[t.Name]
	if v == nil {
		v = &atomic.Uint64{}
		m.versions[t.Name] = v
	}
	v.Add(1)
}

func (m *zzWP7bMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	m.mu.Lock()
	gate := m.getTopicGate[name]
	in := m.getTopicIn
	m.mu.Unlock()
	if gate != nil {
		if in != nil {
			in <- name
		}
		<-gate
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.topics[name]
	if !ok {
		return topic.Topic{}, errs.ErrNotFound
	}
	return t, nil
}

func (m *zzWP7bMetastore) TopicVersion(name string) uint64 {
	m.mu.Lock()
	v := m.versions[name]
	m.mu.Unlock()
	if v == nil {
		return 0
	}
	return v.Load()
}

// zzWP7bBenchMetastore is the read-only variant the benchmarks use: the
// version table is built once and read without a lock, like the real
// store's copy-on-write table, so the benchmark measures Logs and not
// the fake.
type zzWP7bBenchMetastore struct {
	metastore.Metastore
	topics   map[string]topic.Topic
	versions map[string]*atomic.Uint64
}

func (m *zzWP7bBenchMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	t, ok := m.topics[name]
	if !ok {
		return topic.Topic{}, errs.ErrNotFound
	}
	return t, nil
}

func (m *zzWP7bBenchMetastore) TopicVersion(name string) uint64 {
	if v := m.versions[name]; v != nil {
		return v.Load()
	}
	return 0
}

// zzWP7bBenchLogs opens parts partitions of a topic named like a real
// one (the key length matters for the string-key cost).
func zzWP7bBenchLogs(b *testing.B, parts int) (*Logs, string) {
	b.Helper()
	const name = "orders-events-v2"
	v := &atomic.Uint64{}
	v.Store(7)
	ms := &zzWP7bBenchMetastore{
		topics:   map[string]topic.Topic{name: {Name: name, ID: "0000000000000001", Partitions: parts}},
		versions: map[string]*atomic.Uint64{name: v},
	}
	g := NewLogs(b.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	b.Cleanup(func() { _ = g.CloseAll() })
	for p := range parts {
		if _, err := g.Get(name, p); err != nil {
			b.Fatal(err)
		}
	}
	return g, name
}

// BenchmarkZZWP7bGet is the Get fast path a consume scan runs once per
// local partition and a produce commit once per batch.
func BenchmarkZZWP7bGet(b *testing.B) {
	g, name := zzWP7bBenchLogs(b, 8)
	b.ReportAllocs()
	p := 0
	for b.Loop() {
		if _, err := g.Get(name, p); err != nil {
			b.Fatal(err)
		}
		p = (p + 1) & 7
	}
}

func BenchmarkZZWP7bGetParallel(b *testing.B) {
	g, name := zzWP7bBenchLogs(b, 8)
	var ctr atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		p := int(ctr.Add(1)) & 7
		for pb.Next() {
			if _, err := g.Get(name, p); err != nil {
				b.Error(err)
				return
			}
			p = (p + 1) & 7
		}
	})
}

// BenchmarkZZWP7bWithProduceLock is the per-commit-batch overhead of
// the produce lock plus the Get it makes, with an empty critical section.
func BenchmarkZZWP7bWithProduceLock(b *testing.B) {
	g, name := zzWP7bBenchLogs(b, 8)
	b.ReportAllocs()
	p := 0
	noop := func(*storage.Log) error { return nil }
	for b.Loop() {
		if err := g.WithProduceLock(name, p, noop); err != nil {
			b.Fatal(err)
		}
		p = (p + 1) & 7
	}
}

func BenchmarkZZWP7bWithProduceLockParallel(b *testing.B) {
	g, name := zzWP7bBenchLogs(b, 64)
	var ctr atomic.Int64
	noop := func(*storage.Log) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// One partition per goroutine, like one committer per partition.
		p := int(ctr.Add(1)) & 63
		for pb.Next() {
			if err := g.WithProduceLock(name, p, noop); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkZZWP7bPeekHighWatermark is the dispatcher's per-pump
// consumable estimate on an open log.
func BenchmarkZZWP7bPeekHighWatermark(b *testing.B) {
	g, name := zzWP7bBenchLogs(b, 8)
	b.ReportAllocs()
	p := 0
	for b.Loop() {
		if _, ok := g.PeekHighWatermark(name, p); !ok {
			b.Fatal("not open")
		}
		p = (p + 1) & 7
	}
}

// BenchmarkZZWP7bOpenClose is one lazy open of an empty partition and
// its close through ClosePartition: the slow path cost a reclaim, a
// retention change or an eviction pays per partition.
func BenchmarkZZWP7bOpenClose(b *testing.B) {
	g, name := zzWP7bBenchLogs(b, 1)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		idx := 1 + i%1024
		i++
		if _, err := g.Get(name, idx); err != nil {
			b.Fatal(err)
		}
		if err := g.ClosePartition(name, idx); err != nil {
			b.Fatal(err)
		}
	}
}
