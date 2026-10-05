package topics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// newStoreManager returns a Manager over a single-node metastore.Store,
// whose state machine copies schema histories into fan-out children
// the way a cluster does.
func newStoreManager(t *testing.T) (*Manager, *metastore.Store) {
	t.Helper()
	store, err := metastore.New(metastore.Config{NodeID: "node-self", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for deadline := time.Now().Add(5 * time.Second); ; {
		err := store.CreateTopic(context.Background(), topic.Topic{Name: "probe", Partitions: 3})
		if err == nil {
			_ = store.DeleteTopic(context.Background(), "probe")
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no leader: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return newTestManagerForMetastore(t, store, nil, &fakeSchemaRegistry{}, ""), store
}

// TestSchemaWritesStopAtTheClusterBudget: the cluster's schemas as a
// whole are bounded too (256 MiB), so many topics cannot together grow
// what every node snapshots and restores. A create with a schema or a
// schema change that would pass the budget gets a 409 that names it,
// and nothing is created.
func TestSchemaWritesStopAtTheClusterBudget(t *testing.T) {
	m, store := newStoreManager(t)
	ctx := context.Background()
	m.schemaBudget.cluster = 64 << 10

	const size = 10 << 10
	created := 0
	var refused error
	for i := 0; i < 20 && refused == nil; i++ {
		name := "t" + string(rune('a'+i))
		_, err := m.CreateTopic(ctx, CreateOpts{Name: name, Partitions: 3, Schema: paddedSchema(0, size)})
		switch {
		case err == nil:
			created++
		case errors.Is(err, errs.ErrSchemaHistoryFull):
			refused = err
			if _, err := store.GetTopic(ctx, name); !errors.Is(err, errs.ErrNotFound) {
				t.Errorf("refused create left topic %q behind: %v", name, err)
			}
		default:
			t.Fatal(err)
		}
	}
	if refused == nil || created != 6 {
		t.Fatalf("created %d topics of %d bytes under a 64 KiB budget (refusal: %v), want 6 then a refusal", created, size, refused)
	}
	if _, err := m.UpdateTopicSchema(ctx, "ta", paddedSchema(1, size), 0); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("schema change past the cluster budget: err = %v, want ErrSchemaHistoryFull", err)
	}
	// A schema-less create is unaffected.
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: "plain", Partitions: 3}); err != nil {
		t.Fatalf("schema-less create on a full cluster: %v", err)
	}
	have, err := store.ClusterSchemaBytes(ctx)
	if err != nil || have > m.schemaBudget.cluster {
		t.Fatalf("cluster stores %d schema bytes (err %v), over the %d budget", have, err, m.schemaBudget.cluster)
	}
}

// TestBudgetCountsCopiesIntoChildren: a parent's schema history is
// copied into every attached child, and each new version is appended
// to every copy, so the cluster budget counts each copy. Adopting the
// parent's history on attach or create-as-child counts too.
func TestBudgetCountsCopiesIntoChildren(t *testing.T) {
	m, store := newStoreManager(t)
	ctx := context.Background()
	const size = 10 << 10
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: "parent", Partitions: 3, Schema: paddedSchema(0, size)}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{"c1", "c2", "c3"} {
		if _, err := m.CreateTopic(ctx, CreateOpts{Name: c, Partitions: 3}); err != nil {
			t.Fatal(err)
		}
		if err := m.AttachChild(ctx, "parent", c, 0); err != nil {
			t.Fatal(err)
		}
	}
	have, err := store.ClusterSchemaBytes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := store.SchemaBytes(ctx, "c2"); n != int64(size) {
		t.Fatalf("child c2 stores %d schema bytes, want the adopted %d", n, size)
	}

	// Room for one more copy of a version, not four.
	m.schemaBudget.cluster = have + 2*size
	_, err = m.UpdateTopicSchema(ctx, "parent", paddedSchema(1, size), 0)
	if !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("a version copied into three children past the budget: err = %v, want ErrSchemaHistoryFull", err)
	}
	if n, _ := store.SchemaBytes(ctx, "parent"); n != int64(size) {
		t.Fatalf("refused update stored something: parent holds %d bytes", n)
	}

	// Adopting the parent's history counts against the budget too.
	m.schemaBudget.cluster = have + size/2
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: "c4", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := m.AttachChild(ctx, "parent", "c4", 0); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("attach adopting %d bytes past the budget: err = %v, want ErrSchemaHistoryFull", size, err)
	}
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: "c5", Partitions: 3, Parent: "parent"}); !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("create-as-child adopting %d bytes past the budget: err = %v, want ErrSchemaHistoryFull", size, err)
	}
	if _, err := store.GetTopic(ctx, "c5"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("refused create-as-child left c5 behind: %v", err)
	}

	// With room for all four copies the version lands everywhere.
	m.schemaBudget.cluster = have + 4*size
	if _, err := m.UpdateTopicSchema(ctx, "parent", paddedSchema(1, size), 0); err != nil {
		t.Fatalf("a version within the budget: %v", err)
	}
	if after, _ := store.ClusterSchemaBytes(ctx); after != have+4*int64(size) {
		t.Fatalf("cluster stores %d schema bytes, want %d", after, have+4*int64(size))
	}
}
