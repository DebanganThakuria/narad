package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// wp4bFenceRegistry is a JSONSchema registry that counts ReplaceTopic
// calls (one per hydrate), can stretch each one to model a big compile,
// and runs onReplace after each.
type wp4bFenceRegistry struct {
	*schema.JSONSchema
	replaces  atomic.Int64
	delay     time.Duration
	onReplace func()
}

func (r *wp4bFenceRegistry) ReplaceTopic(ctx context.Context, topicName string, history []schema.Version) error {
	r.replaces.Add(1)
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	err := r.JSONSchema.ReplaceTopic(ctx, topicName, history)
	if r.onReplace != nil {
		r.onReplace()
	}
	return err
}

// wp4bSchemaTopic creates name with the driver schema as version 1.
func wp4bSchemaTopic(t *testing.T, store *metastore.Store, name string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSchema(ctx, name, 1, []byte(wp4bDriverSchema)); err != nil {
		t.Fatal(err)
	}
}

// TestWP4BUnrelatedForgetDoesNotRedoSchemaLoad: a ForgetTopic of another
// topic landing while a topic's schema flight loads must not throw that
// load away. The fence used to be one engine-wide counter, so every
// delete of any topic anywhere in the cluster redid every schema load in
// flight on every node.
func TestWP4BUnrelatedForgetDoesNotRedoSchemaLoad(t *testing.T) {
	store := wp4bNewStore(t)
	reg := &wp4bFenceRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	const name = "orders"
	wp4bSchemaTopic(t, store, name)

	// Unrelated deletes during the first three loads (bounded, so a
	// fence that redoes the load on each one still terminates).
	var forgets atomic.Int64
	reg.onReplace = func() {
		if forgets.Add(1) <= 3 {
			e.ForgetTopic(fmt.Sprintf("other-%d", forgets.Load()))
		}
	}
	if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
		t.Fatal(err)
	}
	if got := reg.replaces.Load(); got != 1 {
		t.Errorf("schema loads for one version with unrelated forgets in flight = %d, want 1", got)
	}
	e.cacheMu.RLock()
	entry, ok := e.schemaLoadCache[name]
	e.cacheMu.RUnlock()
	if !ok || entry.version != store.SchemaVersion(name) || !entry.value {
		t.Fatalf("schema cache entry = %+v, %v; want loaded at version %d", entry, ok, store.SchemaVersion(name))
	}
	if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
		t.Fatal("payload invalid under the schema accepted")
	}
	if got := reg.replaces.Load(); got != 1 {
		t.Errorf("schema loads after a warm produce = %d, want 1", got)
	}
}

// TestWP4BUnrelatedForgetChurnDoesNotStallProduce is the reviewer's
// starvation repro: deletes of other topics arriving faster than one
// schema load (a bulk teardown of ephemeral topics) while a produce
// needs its topic's schema loaded. With the engine-wide fence every load
// overlapped a forget and was redone, so the produce waited out the
// whole teardown; it must now finish after one load.
func TestWP4BUnrelatedForgetChurnDoesNotStallProduce(t *testing.T) {
	store := wp4bNewStore(t)
	// Each load takes 20 ms, the compile time of a schema of a few
	// hundred KB; a forget lands every millisecond.
	reg := &wp4bFenceRegistry{JSONSchema: schema.NewJSONSchema(), delay: 20 * time.Millisecond}
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	const name = "orders"
	wp4bSchemaTopic(t, store, name)

	stop := make(chan struct{})
	churned := make(chan int)
	go func() {
		n := 0
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				churned <- n
				return
			case <-tick.C:
				n++
				e.ForgetTopic(fmt.Sprintf("other-%d", n))
			}
		}
	}()

	done := make(chan error, 1)
	go func() { done <- e.validateProducePayload(ctx, name, wp4bDriverPayload) }()
	var (
		err     error
		stalled bool
	)
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		stalled = true
	}
	close(stop)
	n := <-churned
	if stalled {
		err = <-done
	}
	if err != nil {
		t.Fatal(err)
	}
	if stalled {
		t.Fatalf("produce stalled for the whole churn of %d unrelated forgets (%d schema loads)", n, reg.replaces.Load())
	}
	if got := reg.replaces.Load(); got > 2 {
		t.Fatalf("schema loads for one produce under unrelated churn = %d, want at most 2", got)
	}
}

// TestWP4BForgetFenceTokens walks overlapping loads through forgets: a
// load is clean exactly when no forget of its own key landed after it
// began, and past fenceWindow forgets it sees the latest fenceWindow.
func TestWP4BForgetFenceTokens(t *testing.T) {
	var f forgetFence
	f.forget("a") // before any load: fences nothing below
	t1 := f.begin()
	t2 := f.begin()
	f.forget("b")
	tb := f.begin()
	f.forget("a")
	t3 := f.begin() // begun after the forget of a
	if f.clean("a", t1) || f.clean("a", t2) {
		t.Error("load begun before a forget of its key is clean")
	}
	if !f.clean("b", tb) {
		t.Error("load of b fenced by a forget of b that landed before it began")
	}
	if !f.clean("a", t3) || !f.clean("c", t1) {
		t.Error("load fenced by a forget of another key or one that landed before it began")
	}

	// A forget of the key within the latest fenceWindow is seen however
	// many forgets of other keys the load overlapped.
	t4 := f.begin()
	for i := range 3 * fenceWindow {
		f.forget(fmt.Sprintf("other-%d", i))
	}
	f.forget("a")
	for i := range fenceWindow - 1 {
		f.forget(fmt.Sprintf("more-%d", i))
	}
	if f.clean("a", t4) {
		t.Error("forget of the key within the window not seen past the window's worth of other forgets")
	}
	// One more pushes it out: the load counts as clean (see forgetFence).
	f.forget("last")
	if !f.clean("a", t4) {
		t.Error("forget older than the window still fences")
	}

	var nilFence *forgetFence
	if !nilFence.clean("a", nilFence.begin()) {
		t.Error("a nil fence fenced a load")
	}
}

// TestWP4BSameTopicForgetsAreRedoneThenStored: forgets of the flight's
// own topic do redo its load, once per forget, and the flight then
// records the schema as loaded with the registry holding it.
func TestWP4BSameTopicForgetsAreRedoneThenStored(t *testing.T) {
	store := wp4bNewStore(t)
	reg := &wp4bFenceRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	const name = "orders"
	wp4bSchemaTopic(t, store, name)

	var forgets atomic.Int64
	reg.onReplace = func() {
		if forgets.Add(1) <= 3 {
			_ = reg.DropTopic(ctx, name)
			e.ForgetTopic(name)
		}
	}
	if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
		t.Fatal("payload invalid under the schema accepted")
	}
	if got := reg.replaces.Load(); got != 4 {
		t.Errorf("schema loads with three forgets of the topic in flight = %d, want 4", got)
	}
	if err := reg.Validate(ctx, name, []byte(`{"id":1}`)); err == nil || errors.Is(err, schema.ErrSchemaNotFound) {
		t.Fatalf("registry after the flight = %v, want the schema loaded and rejecting", err)
	}
}

// TestWP4BFenceWindowOverflowStillValidates: a schema flight that more
// than fenceWindow forgets overlapped cannot see a forget of its own
// topic among the older ones, so it records the schema as loaded over a
// registry that lost it. That must cost a reload, not a payload let
// through unvalidated, and never a stalled flight.
func TestWP4BFenceWindowOverflowStillValidates(t *testing.T) {
	store := wp4bNewStore(t)
	reg := &wp4bFenceRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	const name = "orders"
	wp4bSchemaTopic(t, store, name)

	var once sync.Once
	reg.onReplace = func() {
		once.Do(func() {
			_ = reg.DropTopic(ctx, name)
			e.ForgetTopic(name)
			for i := range fenceWindow {
				e.ForgetTopic(fmt.Sprintf("other-%d", i))
			}
		})
	}
	if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
		t.Fatal("payload invalid under the schema accepted after the fence window overflowed")
	}
	if got := reg.replaces.Load(); got != 2 {
		t.Errorf("schema loads = %d, want 2 (the overflowed one, then the reload)", got)
	}
	if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}
}

// wp4bHookStore is a real metastore that runs hooks inside the topic,
// assignment and member reads the engine's caches load through.
type wp4bHookStore struct {
	*metastore.Store
	onGetTopic        func()
	onListAssignments func()
	onGetMember       func()
}

func (h *wp4bHookStore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	if h.onGetTopic != nil {
		h.onGetTopic()
	}
	return h.Store.GetTopic(ctx, name)
}

func (h *wp4bHookStore) ListAssignments(topicName string) ([]metastore.Assignment, error) {
	if h.onListAssignments != nil {
		h.onListAssignments()
	}
	return h.Store.ListAssignments(topicName)
}

func (h *wp4bHookStore) GetMember(podID string) (metastore.Member, error) {
	if h.onGetMember != nil {
		h.onGetMember()
	}
	return h.Store.GetMember(podID)
}

// TestWP4BForgetFencesOnlyItsTopic: a metadata load that overlapped a
// ForgetTopic of its own topic returns its value without caching it (it
// may be putting back what a delete just dropped), but one that
// overlapped a ForgetTopic of any other topic, or a member load (no
// topic at all), caches as usual. The engine-wide fence left every
// topic, assignment and member load that overlapped any topic's delete
// uncached, so each such request paid a metastore read again.
func TestWP4BForgetFencesOnlyItsTopic(t *testing.T) {
	base := wp4bNewStore(t)
	store := &wp4bHookStore{Store: base}
	e := wp4bNewEngine(t, store, schema.NewAlwaysValid())
	ctx := context.Background()
	const name = "orders"
	if err := base.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := base.AssignPartition(ctx, name, 0, "node-self"); err != nil {
		t.Fatal(err)
	}
	if err := base.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}
	cachedNames := func() (topicHit, assignmentHit, memberHit bool) {
		e.cacheMu.RLock()
		defer e.cacheMu.RUnlock()
		_, topicHit = e.topicCache[name]
		_, assignmentHit = e.assignmentCache[name]
		_, memberHit = e.memberCache["node-self"]
		return
	}
	forgetEach := func(target string) {
		store.onGetTopic = func() { e.ForgetTopic(target) }
		store.onListAssignments = func() { e.ForgetTopic(target) }
		store.onGetMember = func() { e.ForgetTopic(target) }
	}
	load := func() {
		t.Helper()
		if _, err := e.getTopic(ctx, name); err != nil {
			t.Fatal(err)
		}
		if _, err := e.getAssignment(name, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := e.getRoutingMember("node-self"); err != nil {
			t.Fatal(err)
		}
	}

	forgetEach("other")
	load()
	if topicHit, assignmentHit, memberHit := cachedNames(); !topicHit || !assignmentHit || !memberHit {
		t.Fatalf("loads that overlapped another topic's forget cached: topic=%v assignment=%v member=%v, want all true",
			topicHit, assignmentHit, memberHit)
	}

	e.ForgetTopic(name)
	forgetEach(name)
	load()
	if topicHit, assignmentHit, _ := cachedNames(); topicHit || assignmentHit {
		t.Fatalf("loads that overlapped their own topic's forget cached: topic=%v assignment=%v, want false", topicHit, assignmentHit)
	}

	forgetEach("other")
	load()
	if topicHit, assignmentHit, _ := cachedNames(); !topicHit || !assignmentHit {
		t.Fatalf("clean reload after the forget not cached: topic=%v assignment=%v", topicHit, assignmentHit)
	}
}
