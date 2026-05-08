package metastore

import (
	"context"
	"path/filepath"
	"testing"
)

// TestGetSchemaReturnsDefensiveCopy is a regression test: a previous
// version of GetSchema returned the cached []byte directly, so a caller
// mutating the slice could poison every subsequent reader. The fix
// copies on both miss and hit; this test verifies the hit path.
func TestGetSchemaReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewSQLiteStore(filepath.Join(dir, "metadata.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	original := []byte(`{"type":"object"}`)
	if err := store.PutSchema(ctx, "orders", 1, original); err != nil {
		t.Fatalf("PutSchema: %v", err)
	}

	// First read — populates cache from DB.
	first, err := store.GetSchema(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("GetSchema #1: %v", err)
	}
	// Mutate the returned slice.
	for i := range first {
		first[i] = 'X'
	}

	// Second read — cache hit. Must return original bytes, not 'X'-poisoned.
	second, err := store.GetSchema(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("GetSchema #2: %v", err)
	}
	if string(second) != string(original) {
		t.Fatalf("cache poisoned: got %q, want %q", second, original)
	}

	// Third read — also poison this one and confirm the cache is still clean.
	for i := range second {
		second[i] = 'Y'
	}
	third, err := store.GetSchema(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("GetSchema #3: %v", err)
	}
	if string(third) != string(original) {
		t.Fatalf("cache poisoned across two mutations: got %q, want %q", third, original)
	}
}
