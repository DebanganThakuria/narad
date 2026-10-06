package metastore_test

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// SchemaBytes sums one topic's stored versions and nothing of a topic
// whose name merely starts with it; ClusterSchemaBytes sums every
// topic's, and both drop a deleted topic's history.
func TestSchemaBytesCountsStoredVersions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, name := range []string{"t", "t1"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
			t.Fatal(err)
		}
	}
	put := func(name string, version int, raw string) {
		t.Helper()
		if err := s.PutSchema(ctx, name, version, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	put("t", 1, `{"type":"object"}`)           // 17 bytes
	put("t", 2, `{"type":"object","a":1}`)     // 23 bytes
	put("t1", 1, `{"type":"array","x-pad":1}`) // 26 bytes

	if n, err := s.SchemaBytes(ctx, "t"); err != nil || n != 40 {
		t.Errorf("SchemaBytes(t) = %d, %v; want 40", n, err)
	}
	if n, err := s.SchemaBytes(ctx, "t1"); err != nil || n != 26 {
		t.Errorf("SchemaBytes(t1) = %d, %v; want 26", n, err)
	}
	if n, err := s.SchemaBytes(ctx, "none"); err != nil || n != 0 {
		t.Errorf("SchemaBytes(none) = %d, %v; want 0", n, err)
	}
	if n, err := s.ClusterSchemaBytes(ctx); err != nil || n != 66 {
		t.Errorf("ClusterSchemaBytes() = %d, %v; want 66", n, err)
	}
	if err := s.DeleteTopic(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if n, err := s.ClusterSchemaBytes(ctx); err != nil || n != 26 {
		t.Errorf("ClusterSchemaBytes() after deleting t = %d, %v; want 26", n, err)
	}
}
