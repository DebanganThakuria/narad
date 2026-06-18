package schema

import (
	"context"
	"errors"
	"testing"
)

func TestAlwaysValidAcceptsAnyPayload(t *testing.T) {
	registry := NewAlwaysValid()

	version, err := registry.Register(context.Background(), "orders", []byte(`{"type":"object"}`))
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if version != 1 {
		t.Fatalf("Register() version = %d, want 1", version)
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

func TestJSONSchemaRegisterAndValidateSuccess(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)

	version, err := registry.Register(context.Background(), "orders", schemaBytes)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if version != 1 {
		t.Fatalf("Register() version = %d, want 1", version)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":"o_123"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
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

func TestJSONSchemaRegisterRejectsIncompatibleSchema(t *testing.T) {
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

	if _, err := registry.Register(context.Background(), "orders", original); err != nil {
		t.Fatalf("Register() original error = %v", err)
	}
	_, err := registry.Register(context.Background(), "orders", updated)
	if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("Register() incompatible error = %v, want %v", err, ErrIncompatible)
	}
}

func TestJSONSchemaLoadRestoresPersistedSchema(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)

	if err := registry.Load(context.Background(), "orders", 3, schemaBytes); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":"o_123"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got := registry.versions["orders"]; got != 3 {
		t.Fatalf("versions[orders] = %d, want 3", got)
	}
}

func TestJSONSchemaUnloadRestoresPreviousLatestVersion(t *testing.T) {
	registry := NewJSONSchema()
	v1 := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)
	v2 := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"},"count":{"type":"integer"}},
		"required":["id"]
	}`)

	if err := registry.Load(context.Background(), "orders", 1, v1); err != nil {
		t.Fatalf("Load(v1) error = %v", err)
	}
	if err := registry.Load(context.Background(), "orders", 2, v2); err != nil {
		t.Fatalf("Load(v2) error = %v", err)
	}
	if err := registry.Unload(context.Background(), "orders", 2); err != nil {
		t.Fatalf("Unload(v2) error = %v", err)
	}
	if got := registry.versions["orders"]; got != 1 {
		t.Fatalf("versions[orders] = %d, want 1", got)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":"o_123"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestJSONSchemaUnloadLastVersionRemovesTopic(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)

	if err := registry.Load(context.Background(), "orders", 1, schemaBytes); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := registry.Unload(context.Background(), "orders", 1); err != nil {
		t.Fatalf("Unload() error = %v", err)
	}
	if err := registry.Validate(context.Background(), "orders", []byte(`{"id":"o_123"}`)); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("Validate() error = %v, want %v", err, ErrSchemaNotFound)
	}
}

func TestJSONSchemaValidateRejectsInvalidPayloadJSON(t *testing.T) {
	registry := NewJSONSchema()
	schemaBytes := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)
	if _, err := registry.Register(context.Background(), "orders", schemaBytes); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	err := registry.Validate(context.Background(), "orders", []byte(`{"id":`))
	if err == nil {
		t.Fatal("Validate() error = nil, want invalid JSON payload error")
	}
}

func TestCheckCompatible(t *testing.T) {
	cases := []struct {
		name    string
		oldRaw  []byte
		newRaw  []byte
		wantErr bool
	}{
		{
			name: "additive schema stays compatible",
			oldRaw: []byte(`{
				"properties":{"id":{"type":"string"}},
				"required":["id"]
			}`),
			newRaw: []byte(`{
				"properties":{
					"id":{"type":"string"},
					"region":{"type":"string"}
				},
				"required":["id"]
			}`),
		},
		{
			name: "removed property is incompatible",
			oldRaw: []byte(`{
				"properties":{"id":{"type":"string"}},
				"required":["id"]
			}`),
			newRaw: []byte(`{
				"properties":{"region":{"type":"string"}}
			}`),
			wantErr: true,
		},
		{
			name: "widening union types stays compatible",
			oldRaw: []byte(`{
					"properties":{"id":{"type":["string","null"]}}
				}`),
			newRaw: []byte(`{
					"properties":{"id":{"type":["string","null","number"]}}
				}`),
		},
		{
			name: "narrowing union types is incompatible",
			oldRaw: []byte(`{
					"properties":{"id":{"type":["string","null"]}}
				}`),
			newRaw: []byte(`{
					"properties":{"id":{"type":["string"]}}
				}`),
			wantErr: true,
		},
		{
			name: "adding required property is incompatible",
			oldRaw: []byte(`{
					"properties":{"id":{"type":"string"}}
				}`),
			newRaw: []byte(`{
					"properties":{
						"id":{"type":"string"},
						"region":{"type":"string"}
					},
					"required":["region"]
				}`),
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCompatible(tc.oldRaw, tc.newRaw)
			if tc.wantErr && err == nil {
				t.Fatal("checkCompatible() error = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("checkCompatible() error = %v, want nil", err)
			}
		})
	}
}
