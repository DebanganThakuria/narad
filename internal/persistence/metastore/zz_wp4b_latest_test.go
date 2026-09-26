package metastore

import (
	"bytes"
	"context"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// The produce path's hydrate finds the latest version through this.
var _ schema.LatestSource = (*Store)(nil)

func wp4bNewStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{NodeID: "wp4b-0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = s.DeleteTopic(context.Background(), "__probe__")
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return nil
}

// TestWP4BLatestSchemaMatchesPersistedHistory: LatestSchema returns what
// schema.PersistedHistory ends with, as a copy, for every history
// shape: none, several versions (including past 9, where the keys stop
// sorting numerically), after a delete, and a history with a hole,
// which only data written before the append-only rule can have.
func TestWP4BLatestSchemaMatchesPersistedHistory(t *testing.T) {
	s := wp4bNewStore(t)
	ctx := context.Background()
	check := func(name string) {
		t.Helper()
		history, err := schema.PersistedHistory(ctx, s, name)
		if err != nil {
			t.Fatal(err)
		}
		version, raw, err := s.LatestSchema(ctx, name)
		if err != nil {
			t.Fatalf("LatestSchema(%s): %v", name, err)
		}
		if len(history) == 0 {
			if version != 0 || raw != nil {
				t.Fatalf("LatestSchema(%s) = %d, %q; want 0, nil", name, version, raw)
			}
			return
		}
		last := history[len(history)-1]
		if version != last.Number || !bytes.Equal(raw, last.Raw) {
			t.Fatalf("LatestSchema(%s) = %d, %q; want %d, %q", name, version, raw, last.Number, last.Raw)
		}
		// A copy: bbolt values are only valid inside their transaction.
		raw[0] = 'X'
		if _, again, _ := s.LatestSchema(ctx, name); again[0] == 'X' {
			t.Fatal("LatestSchema returned bbolt's buffer instead of a copy")
		}
	}

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	check("orders")
	check("never-created")
	for v := 1; v <= 12; v++ {
		doc := []byte(`{"type":"object","maxProperties":` + string(rune('0'+v%10)) + `}`)
		if err := s.PutSchema(ctx, "orders", v, doc); err != nil {
			t.Fatal(err)
		}
		check("orders")
	}
	if version, _, _ := s.LatestSchema(ctx, "orders"); version != 12 {
		t.Fatalf("LatestSchema version = %d, want 12", version)
	}
	if err := s.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	check("orders")

	// Versions 1, 2 and 4: the history ends at 2.
	err := s.fsm.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSchemas)
		for _, v := range []int{1, 2, 4} {
			if err := b.Put(schemaKey("holey", v), []byte(`{"v":`+string(rune('0'+v))+`}`)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	check("holey")
	if version, _, _ := s.LatestSchema(ctx, "holey"); version != 2 {
		t.Fatalf("LatestSchema of a history with a hole = %d, want 2", version)
	}
}
