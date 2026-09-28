package messaging

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// wp4bMissStore is a real metastore whose topic and schema versions are
// per-name counters the benchmark advances. With free set its topic and
// latest schema reads cost nothing, so a lookup after an advance
// measures only the cache's miss machinery (the load bookkeeping, the
// version checks and the store); without it they are the real reads.
type wp4bMissStore struct {
	*metastore.Store
	versions map[string]*atomic.Uint64 // read-only once built
	free     bool
}

func newWP4BMissStore(tb testing.TB, names []string) *wp4bMissStore {
	m := &wp4bMissStore{Store: wp4bNewStore(tb), versions: make(map[string]*atomic.Uint64, len(names))}
	for _, name := range names {
		m.versions[name] = new(atomic.Uint64)
	}
	return m
}

func (m *wp4bMissStore) TopicVersion(name string) uint64       { return m.versions[name].Load() }
func (m *wp4bMissStore) SchemaVersion(topicName string) uint64 { return m.versions[topicName].Load() }

func (m *wp4bMissStore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	if !m.free {
		return m.Store.GetTopic(ctx, name)
	}
	return topic.Topic{Name: name, Partitions: 3}, nil
}

func (m *wp4bMissStore) LatestSchema(ctx context.Context, topicName string) (int, []byte, error) {
	if !m.free {
		return m.Store.LatestSchema(ctx, topicName)
	}
	return 0, nil, nil
}

// BenchmarkWP4BMetadataCacheMiss is a topic lookup and a schema sync
// that miss on every call (the version moved since the entry was
// stored): what each request pays right after a metadata change. The
// parallel cases give every goroutine its own topic, the shape of a
// busy node reloading many topics at once.
func BenchmarkWP4BMetadataCacheMiss(b *testing.B) {
	names := make([]string, 64)
	for i := range names {
		names[i] = fmt.Sprintf("t-%02d", i)
	}
	store := newWP4BMissStore(b, names)
	store.free = true
	e := wp4bNewEngine(b, store, schema.NewAlwaysValid())
	ctx := context.Background()

	b.Run("getTopic", func(b *testing.B) {
		b.ReportAllocs()
		v := store.versions[names[0]]
		for i := 0; i < b.N; i++ {
			v.Add(1)
			if _, err := e.getTopic(ctx, names[0]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("getTopic-parallel", func(b *testing.B) {
		b.ReportAllocs()
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			name := names[int(next.Add(1)-1)%len(names)]
			v := store.versions[name]
			for pb.Next() {
				v.Add(1)
				if _, err := e.getTopic(ctx, name); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("schemaSync", func(b *testing.B) {
		b.ReportAllocs()
		v := store.versions[names[0]]
		for i := 0; i < b.N; i++ {
			v.Add(1)
			if err := e.validateProducePayload(ctx, names[0], wp4bDriverPayload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("schemaSync-parallel", func(b *testing.B) {
		b.ReportAllocs()
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			name := names[int(next.Add(1)-1)%len(names)]
			v := store.versions[name]
			for pb.Next() {
				v.Add(1)
				if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// BenchmarkWP4BMetadataCacheMissReal is BenchmarkWP4BMetadataCacheMiss
// with the real metastore reads behind the miss (a topic record, and
// the driver schema's latest version read and compiled): what a miss
// costs a request in production.
func BenchmarkWP4BMetadataCacheMissReal(b *testing.B) {
	names := make([]string, 64)
	for i := range names {
		names[i] = fmt.Sprintf("t-%02d", i)
	}
	store := newWP4BMissStore(b, names)
	ctx := context.Background()
	for _, name := range names {
		if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
			b.Fatal(err)
		}
		if err := store.PutSchema(ctx, name, 1, []byte(wp4bDriverSchema)); err != nil {
			b.Fatal(err)
		}
	}
	e := wp4bNewEngine(b, store, schema.NewJSONSchema())

	b.Run("getTopic", func(b *testing.B) {
		b.ReportAllocs()
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			name := names[int(next.Add(1)-1)%len(names)]
			v := store.versions[name]
			for pb.Next() {
				v.Add(1)
				if _, err := e.getTopic(ctx, name); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("schemaSync", func(b *testing.B) {
		b.ReportAllocs()
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			name := names[int(next.Add(1)-1)%len(names)]
			v := store.versions[name]
			for pb.Next() {
				v.Add(1)
				if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
