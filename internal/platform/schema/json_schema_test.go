package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestAlwaysValidAcceptsAnyPayload(t *testing.T) {
	registry := NewAlwaysValid()

	if err := registry.CheckCompatible(context.Background(), "orders", []byte(`{"type":"object"}`), []byte(`{"type":"string"}`)); err != nil {
		t.Fatalf("CheckCompatible() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`not-json`)); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestJSONSchemaValidateReturnsNotFoundWhenTopicHasNoSchema(t *testing.T) {
	registry := NewJSONSchema()

	err := registry.Validate(context.Background(), "missing", []byte(`{"id":1}`))
	if !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("Validate() error = %v, want %v", err, ErrSchemaNotFound)
	}
}

func TestJSONSchemaLoadAndValidateSuccess(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)

	if err := registry.Load(context.Background(), "orders", 1, schemaBytes); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":"o_123"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":1}`)); err == nil {
		t.Fatal("Validate() error = nil, want schema violation")
	}
}

func TestJSONSchemaLoadRejectsNonPositiveVersion(t *testing.T) {
	registry := NewJSONSchema()
	if err := registry.Load(context.Background(), "orders", 0, []byte(`{"type":"object"}`)); err == nil {
		t.Fatal("Load(v0) error = nil, want error")
	}
}

func TestJSONSchemaValidateDefinitionDoesNotRegister(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)

	if err := registry.ValidateDefinition(context.Background(), "orders", schemaBytes); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":"o_123"}`)); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("Validate() after dry-run error = %v, want %v", err, ErrSchemaNotFound)
	}
}

func TestJSONSchemaValidateDefinitionRejectsInvalidSchema(t *testing.T) {
	registry := NewJSONSchema()

	err := registry.ValidateDefinition(context.Background(), "orders", []byte(`{"type":`))
	if err == nil {
		t.Fatal("ValidateDefinition() error = nil, want invalid schema error")
	}
}

func TestJSONSchemaCheckCompatibleWrapsErrIncompatible(t *testing.T) {
	registry := NewJSONSchema()
	original := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)
	updated := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"number"}},
		"required":["id"]
	}`)

	err := registry.CheckCompatible(context.Background(), "orders", original, updated)
	if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("CheckCompatible() error = %v, want %v", err, ErrIncompatible)
	}
	if err := registry.CheckCompatible(context.Background(), "orders", original, original); err != nil {
		t.Fatalf("CheckCompatible(same) error = %v, want nil", err)
	}
}

func TestJSONSchemaLoadKeepsLatestPointerMovingForward(t *testing.T) {
	registry := NewJSONSchema()
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	v3 := []byte(`{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer"}},"required":["id"]}`)

	if err := registry.Load(context.Background(), "orders", 3, v3); err != nil {
		t.Fatalf("Load(v3) error = %v", err)
	}
	if err := registry.Load(context.Background(), "orders", 1, v1); err != nil {
		t.Fatalf("Load(v1) error = %v", err)
	}
	if got := registry.versions["orders"]; got != 3 {
		t.Fatalf("versions[orders] = %d, want 3 (loading an older version must not move latest back)", got)
	}
}

// ReplaceTopic is how the produce path follows the metastore: a
// delete-and-recreate under the same name yields a v1 with different
// content, and a version registered elsewhere extends the history.
// Both must take effect and nothing from the old history may survive.
func TestJSONSchemaReplaceTopicSwapsWholeHistory(t *testing.T) {
	registry := NewJSONSchema()
	ctx := context.Background()
	oldV1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	oldV2 := []byte(`{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer"}},"required":["id"]}`)
	if err := registry.ReplaceTopic(ctx, "orders", []Version{{1, oldV1}, {2, oldV2}}); err != nil {
		t.Fatalf("ReplaceTopic(old) error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"id":"x","n":"not-int"}`)); err == nil {
		t.Fatal("Validate() accepted a payload v2 rejects")
	}

	// Recreated topic: a single v1 with unrelated content.
	newV1 := []byte(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)
	if err := registry.ReplaceTopic(ctx, "orders", []Version{{1, newV1}}); err != nil {
		t.Fatalf("ReplaceTopic(new) error = %v", err)
	}
	if got := registry.versions["orders"]; got != 1 {
		t.Fatalf("versions[orders] = %d, want 1 after replace", got)
	}
	if len(registry.schemas["orders"]) != 1 {
		t.Fatalf("schemas[orders] has %d versions, want 1 (old v2 must not survive)", len(registry.schemas["orders"]))
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"name":"x"}`)); err != nil {
		t.Fatalf("Validate() under the new v1 error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"id":"x"}`)); err == nil {
		t.Fatal("Validate() still accepts a payload only the old schema allowed")
	}

	// Empty history drops the topic.
	if err := registry.ReplaceTopic(ctx, "orders", nil); err != nil {
		t.Fatalf("ReplaceTopic(nil) error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{}`)); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("Validate() after empty replace error = %v, want %v", err, ErrSchemaNotFound)
	}
}

func TestJSONSchemaReplaceTopicRejectsBadHistoryWithoutTouchingState(t *testing.T) {
	registry := NewJSONSchema()
	ctx := context.Background()
	good := []byte(`{"type":"object"}`)
	if err := registry.ReplaceTopic(ctx, "orders", []Version{{1, good}}); err != nil {
		t.Fatalf("ReplaceTopic() error = %v", err)
	}
	if err := registry.ReplaceTopic(ctx, "orders", []Version{{1, good}, {2, []byte(`{"type":`)}}); err == nil {
		t.Fatal("ReplaceTopic() with an uncompilable version error = nil")
	}
	if err := registry.ReplaceTopic(ctx, "orders", []Version{{0, good}}); err == nil {
		t.Fatal("ReplaceTopic() with version 0 error = nil")
	}
	if got := registry.versions["orders"]; got != 1 {
		t.Fatalf("versions[orders] = %d after failed replaces, want 1 (state untouched)", got)
	}
}

// A produce racing a reload must never observe the window between
// "old history gone" and "new history in": that window would answer
// ErrSchemaNotFound and let the payload through unvalidated.
func TestJSONSchemaReplaceTopicNeverExposesEmptyTopic(t *testing.T) {
	registry := NewJSONSchema()
	ctx := context.Background()
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	v2 := []byte(`{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer"}},"required":["id"]}`)
	if err := registry.ReplaceTopic(ctx, "orders", []Version{{1, v1}}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var notFound int
	var mu sync.Mutex
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := registry.Validate(ctx, "orders", []byte(`{"id":1}`))
				if errors.Is(err, ErrSchemaNotFound) {
					mu.Lock()
					notFound++
					mu.Unlock()
				}
			}
		})
	}
	for i := range 200 {
		history := []Version{{1, v1}}
		if i%2 == 1 {
			history = append(history, Version{2, v2})
		}
		if err := registry.ReplaceTopic(ctx, "orders", history); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if notFound != 0 {
		t.Fatalf("Validate() returned ErrSchemaNotFound %d times during replaces; the swap must be atomic", notFound)
	}
}

func TestJSONSchemaDropTopicRemovesAllVersions(t *testing.T) {
	registry := NewJSONSchema()
	ctx := context.Background()
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	v2 := []byte(`{"type":"object","properties":{"id":{"type":"string"},"count":{"type":"integer"}},"required":["id"]}`)

	if err := registry.Load(ctx, "orders", 1, v1); err != nil {
		t.Fatalf("Load(v1) error = %v", err)
	}
	if err := registry.Load(ctx, "orders", 2, v2); err != nil {
		t.Fatalf("Load(v2) error = %v", err)
	}
	if err := registry.DropTopic(ctx, "orders"); err != nil {
		t.Fatalf("DropTopic() error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{}`)); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("Validate() after drop error = %v, want %v", err, ErrSchemaNotFound)
	}
	if _, ok := registry.schemas["orders"]; ok {
		t.Fatal("schemas[orders] still present after DropTopic")
	}
}

func TestJSONSchemaValidateRejectsInvalidPayloadJSON(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)
	if err := registry.Load(context.Background(), "orders", 1, schemaBytes); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	err := registry.Validate(context.Background(), "orders", []byte(`{"id":`))
	if err == nil {
		t.Fatal("Validate() error = nil, want invalid JSON payload error")
	}
}

// Validation errors are sent to clients verbatim. The schema is
// registered under an opaque narad:// URL so the message never carries
// the broker's working directory (the relative resource name the
// compiler used to get was resolved to file:///<cwd>/...).
func TestJSONSchemaValidationErrorCarriesOpaqueResourceURL(t *testing.T) {
	registry := NewJSONSchema()
	if err := registry.Load(context.Background(), "orders", 2, []byte(`{"type":"object","properties":{"qty":{"type":"string"}}}`)); err != nil {
		t.Fatal(err)
	}
	err := registry.Validate(context.Background(), "orders", []byte(`{"qty":1}`))
	if err == nil {
		t.Fatal("Validate() error = nil, want violation")
	}
	msg := err.Error()
	if strings.Contains(msg, "file://") || strings.Contains(msg, "/Users/") || strings.Contains(msg, "/home/") {
		t.Fatalf("validation error leaks a filesystem path: %s", msg)
	}
	if !strings.Contains(msg, "narad://schema/orders/2") {
		t.Fatalf("validation error = %q, want the opaque narad://schema/orders/2 resource URL", msg)
	}
}

func TestJSONSchemaInDocumentRefStillResolves(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"qty":{"$ref":"#/$defs/count"}},
		"$defs":{"count":{"type":"integer","minimum":0}}
	}`)
	if err := registry.Load(context.Background(), "orders", 1, schemaBytes); err != nil {
		t.Fatalf("Load() with in-document $ref error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"qty":3}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"qty":-1}`)); err == nil {
		t.Fatal("Validate() accepted a payload the $ref target rejects")
	}
}

func TestJSONSchemaExternalMetaschemaIsRefused(t *testing.T) {
	registry := NewJSONSchema()
	err := registry.ValidateDefinition(context.Background(), "orders", []byte(`{"$schema":"https://example.invalid/meta","type":"object"}`))
	if err == nil {
		t.Fatal("ValidateDefinition() with a remote $schema error = nil, want refusal")
	}
	if strings.Contains(err.Error(), "example.invalid") {
		t.Fatalf("error echoes the client-supplied URL: %v", err)
	}
	// The embedded drafts still work.
	if err := registry.ValidateDefinition(context.Background(), "orders", []byte(`{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`)); err != nil {
		t.Fatalf("ValidateDefinition() with the embedded draft-07 metaschema error = %v", err)
	}
}

// BenchmarkValidatePayloadDecode documents what the exact-number decode
// costs: Validate uses jsonschema.UnmarshalJSON (json.Number, so
// integers beyond 2^53 keep their value), which runs about a fifth
// slower than a float64 json.Unmarshal because it goes through a
// json.Decoder. The difference is well under a microsecond per produce
// and buys the documented number contract.
func BenchmarkValidatePayloadDecode(b *testing.B) {
	payload := []byte(`{"id":"o_12345","qty":3,"price":19.99,"tags":["a","b","c"],"customer":{"name":"Ada","email":"ada@example.com","tier":2},"lines":[{"sku":"x1","n":1},{"sku":"x2","n":4}]}`)
	registry := NewJSONSchema()
	schemaBytes := []byte(`{"type":"object","properties":{"id":{"type":"string"},"qty":{"type":"integer"},"price":{"type":"number"},"tags":{"type":"array","items":{"type":"string"}}},"required":["id"]}`)
	if err := registry.Load(context.Background(), "orders", 1, schemaBytes); err != nil {
		b.Fatal(err)
	}
	b.Run("Validate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := registry.Validate(context.Background(), "orders", payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode-only/json.Unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var v any
			if err := json.Unmarshal(payload, &v); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode-only/jsonschema.UnmarshalJSON", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
