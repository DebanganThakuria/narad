package messaging

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

type wp4bSlowReaderKey struct{}

// wp4bGatedStore is a real metastore whose schema reads, for the one
// goroutine whose context carries wp4bSlowReaderKey, see the history as
// it was at that goroutine's first read and then stall until the test
// releases them. It models a produce that read the schema history,
// was descheduled (or compiled a big schema) and reached ReplaceTopic
// only after the history had moved on.
type wp4bGatedStore struct {
	*metastore.Store
	entered chan struct{}
	release chan struct{}

	mu       sync.Mutex
	gated    bool           // the gated read has happened
	snapshot map[int][]byte // what that read saw; nil once used up
}

func (g *wp4bGatedStore) slow(ctx context.Context) bool {
	return ctx.Value(wp4bSlowReaderKey{}) != nil
}

// takeSnapshot records the history as the gated reader sees it and
// stalls it until released. It reports false for every later read.
func (g *wp4bGatedStore) takeSnapshot(ctx context.Context, topicName string) bool {
	g.mu.Lock()
	if g.gated {
		g.mu.Unlock()
		return false
	}
	g.gated = true
	g.snapshot = map[int][]byte{}
	for n := 1; ; n++ {
		raw, err := g.Store.GetSchema(ctx, topicName, n)
		if err != nil {
			break
		}
		g.snapshot[n] = raw
	}
	g.mu.Unlock()
	close(g.entered)
	<-g.release
	return true
}

func (g *wp4bGatedStore) GetSchema(ctx context.Context, topicName string, version int) ([]byte, error) {
	if g.slow(ctx) {
		g.takeSnapshot(ctx, topicName)
		g.mu.Lock()
		snap := g.snapshot
		if snap != nil {
			raw, ok := snap[version]
			if !ok {
				g.snapshot = nil // the walk ends here; later reads are live
			}
			g.mu.Unlock()
			if !ok {
				return nil, errs.ErrNotFound
			}
			return raw, nil
		}
		g.mu.Unlock()
	}
	return g.Store.GetSchema(ctx, topicName, version)
}

// LatestSchema serves the latest-version read the same way, so the
// test holds whichever read path Hydrate takes.
func (g *wp4bGatedStore) LatestSchema(ctx context.Context, topicName string) (int, []byte, error) {
	if g.slow(ctx) && g.takeSnapshot(ctx, topicName) {
		g.mu.Lock()
		defer g.mu.Unlock()
		latest := len(g.snapshot)
		raw := g.snapshot[latest]
		g.snapshot = nil
		return latest, raw, nil
	}
	latest := 0
	var raw []byte
	for n := 1; ; n++ {
		v, err := g.Store.GetSchema(ctx, topicName, n)
		if errors.Is(err, errs.ErrNotFound) {
			return latest, raw, nil
		}
		if err != nil {
			return 0, nil, err
		}
		latest, raw = n, v
	}
}

// TestWP4BSchemaHydrateRaceKeepsNewestSchema is the stale-overwrite
// race: produce A misses the schema cache at s1 and reads history [v1];
// v2 lands; produce B loads [v1, v2] and caches s2; A's ReplaceTopic of
// [v1] then lands last. Before the fix A found B's s2 entry on its
// retry and returned, so the node validated against v1 until the next
// schema change while its cache said s2 was loaded.
func TestWP4BSchemaHydrateRaceKeepsNewestSchema(t *testing.T) {
	store := wp4bNewStore(t)
	gs := &wp4bGatedStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	e := wp4bNewEngine(t, gs, schema.NewJSONSchema())

	ctx := context.Background()
	const name = "orders"
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSchema(ctx, name, 1, []byte(`{"type":"object","properties":{"qty":{"type":"string"}}}`)); err != nil {
		t.Fatal(err)
	}

	aDone := make(chan error, 1)
	go func() {
		actx := context.WithValue(ctx, wp4bSlowReaderKey{}, true)
		aDone <- e.validateProducePayload(actx, name, []byte(`{"qty":"1"}`))
	}()
	<-gs.entered

	// v2 widens qty to string or integer.
	if err := store.PutSchema(ctx, name, 2, []byte(`{"type":"object","properties":{"qty":{"type":["string","integer"]}}}`)); err != nil {
		t.Fatal(err)
	}
	bDone := make(chan error, 1)
	go func() { bDone <- e.validateProducePayload(ctx, name, []byte(`{"qty":1}`)) }()

	// Let B finish first if it can (the losing order); a B that waits
	// for A's hydrate instead is released with it.
	select {
	case err := <-bDone:
		bDone <- err
	case <-time.After(200 * time.Millisecond):
	}
	close(gs.release)
	if err := <-aDone; err != nil {
		t.Fatalf("A: %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("B: v2-valid payload rejected while v2 is current: %v", err)
	}

	for i := range 3 {
		if err := e.validateProducePayload(ctx, name, []byte(`{"qty":1}`)); err != nil {
			t.Fatalf("produce %d after the race: payload valid under the current v2 rejected: %v", i+1, err)
		}
	}
}

// wp4bCountingStore counts the metastore's schema reads.
type wp4bCountingStore struct {
	*metastore.Store
	gets atomic.Int64
}

func (c *wp4bCountingStore) GetSchema(ctx context.Context, topicName string, version int) ([]byte, error) {
	c.gets.Add(1)
	return c.Store.GetSchema(ctx, topicName, version)
}

// wp4bCountingRegistry counts ReplaceTopic calls, each one a compile of
// the topic's latest schema.
type wp4bCountingRegistry struct {
	*schema.JSONSchema
	replaces atomic.Int64
}

func (c *wp4bCountingRegistry) ReplaceTopic(ctx context.Context, topicName string, history []schema.Version) error {
	c.replaces.Add(1)
	return c.JSONSchema.ReplaceTopic(ctx, topicName, history)
}

// TestWP4BSchemaHydrateHerdSharesOneLoad fires 200 produces at a topic
// right after its schema changed. Before the fix every one of them read
// the whole history and compiled the schema itself (200 compiles, 200 x
// versions reads); now one hydrate serves them all.
func TestWP4BSchemaHydrateHerdSharesOneLoad(t *testing.T) {
	const versions, herd = 20, 200
	base := wp4bNewStore(t)
	store := &wp4bCountingStore{Store: base}
	reg := &wp4bCountingRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)

	ctx := context.Background()
	const name = "orders"
	if err := base.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	for v := 1; v < versions; v++ {
		if err := base.PutSchema(ctx, name, v, wp4bBigSchema(400, v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.validateProducePayload(ctx, name, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := base.PutSchema(ctx, name, versions, wp4bBigSchema(400, versions)); err != nil {
		t.Fatal(err)
	}
	store.gets.Store(0)
	reg.replaces.Store(0)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range herd {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := e.validateProducePayload(ctx, name, []byte(`{"field_00001_20":"x"}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := reg.replaces.Load(); got != 1 {
		t.Errorf("schema compiles after one version change = %d, want 1", got)
	}
	if got := store.gets.Load(); got > versions+1 {
		t.Errorf("GetSchema calls after one version change = %d, want at most one history walk (%d)", got, versions+1)
	}
	// The shared load is the current schema: v20 caps field_00001_20 at
	// 101 characters.
	long := `{"field_00001_20":"` + strings.Repeat("x", 102) + `"}`
	if err := e.validateProducePayload(ctx, name, []byte(long)); err == nil {
		t.Error("payload over the current schema's maxLength accepted")
	}
}
