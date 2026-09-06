package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// JSONSchema is a Registry backed by santhosh-tekuri/jsonschema.
// Schemas are compiled once on Load/ReplaceTopic so repeated Validate
// calls pay no compilation cost. Safe for concurrent use.
type JSONSchema struct {
	mu       sync.RWMutex
	versions map[string]int                        // topic → latest loaded version number
	schemas  map[string]map[int]*jsonschema.Schema // topic → version → compiled schema
}

// NewJSONSchema returns an empty JSONSchema registry.
func NewJSONSchema() *JSONSchema {
	return &JSONSchema{
		versions: map[string]int{},
		schemas:  map[string]map[int]*jsonschema.Schema{},
	}
}

// ValidateDefinition compiles schemaBytes without registering it.
func (r *JSONSchema) ValidateDefinition(_ context.Context, topic string, schemaBytes []byte) error {
	_, err := compileSchema(topic, 0, schemaBytes)
	return err
}

// CheckCompatible reports whether every document accepted by previous
// is accepted by next, wrapping the reason in ErrIncompatible when not.
// The check is structural subsumption over an explicit allowlist of
// keywords and fails closed on anything else; see checkCompatible for
// the exact rules and docs/client/topics.md for the user-facing list.
func (r *JSONSchema) CheckCompatible(_ context.Context, _ string, previous, next []byte) error {
	if err := checkCompatible(previous, next); err != nil {
		return fmt.Errorf("%w: %w", ErrIncompatible, err)
	}
	return nil
}

// Load compiles and stores one persisted schema version. Other loaded
// versions of the topic are untouched; the latest pointer moves only
// forward.
func (r *JSONSchema) Load(_ context.Context, topic string, version int, schemaBytes []byte) error {
	if version <= 0 {
		return fmt.Errorf("schema: %s: invalid version %d", topic, version)
	}
	compiled, err := compileSchema(topic, version, schemaBytes)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.schemas[topic] == nil {
		r.schemas[topic] = map[int]*jsonschema.Schema{}
	}
	r.schemas[topic][version] = compiled
	if version > r.versions[topic] {
		r.versions[topic] = version
	}
	return nil
}

// ReplaceTopic swaps the topic's loaded history for history in one
// step. Everything is compiled before the lock is taken, so a
// concurrent Validate sees either the old history or the new one,
// never an empty topic in between (which would let a payload through
// unvalidated). An empty history drops the topic.
func (r *JSONSchema) ReplaceTopic(_ context.Context, topic string, history []Version) error {
	compiled := make(map[int]*jsonschema.Schema, len(history))
	latest := 0
	for _, v := range history {
		if v.Number <= 0 {
			return fmt.Errorf("schema: %s: invalid version %d", topic, v.Number)
		}
		s, err := compileSchema(topic, v.Number, v.Raw)
		if err != nil {
			return fmt.Errorf("v%d: %w", v.Number, err)
		}
		compiled[v.Number] = s
		if v.Number > latest {
			latest = v.Number
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if latest == 0 {
		delete(r.schemas, topic)
		delete(r.versions, topic)
		return nil
	}
	r.schemas[topic] = compiled
	r.versions[topic] = latest
	return nil
}

// DropTopic removes every schema version and the latest-version pointer
// for the topic. Called on topic delete/purge so a recreated topic
// starts schema-less instead of inheriting phantom versions.
func (r *JSONSchema) DropTopic(_ context.Context, topic string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.schemas, topic)
	delete(r.versions, topic)
	return nil
}

// errExternalRef is the client-facing reason for any $ref or $schema
// that points outside the document being compiled. It deliberately
// names nothing about the target: the broker never opens it, and the
// message must not echo a path or URL the caller could use to probe
// the filesystem.
var errExternalRef = errors.New("external $ref is not allowed; only references into the schema document itself (\"#/$defs/...\") resolve")

// noExternalRefs is the URL loader installed on every compiler. The
// library's default is FileLoader, which would open any file:// URL a
// client puts in $ref on the broker's filesystem (and echo the OS error
// for a missing one). Refusing every load means only in-document
// references and the embedded draft metaschemas resolve.
type noExternalRefs struct{}

func (noExternalRefs) Load(string) (any, error) { return nil, errExternalRef }

// schemaResourceURL is the opaque absolute URL a topic's schema is
// registered under. A relative name would be resolved by the compiler
// against the process working directory and leak it (as file:///...)
// in every validation error sent to clients.
func schemaResourceURL(topic string, version int) string {
	return fmt.Sprintf("narad://schema/%s/%d", topic, version)
}

func compileSchema(topic string, version int, schemaBytes []byte) (*jsonschema.Schema, error) {
	schemaDoc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("schema: invalid JSON: %w", err)
	}

	c := jsonschema.NewCompiler()
	c.UseLoader(noExternalRefs{})
	resource := schemaResourceURL(topic, version)
	if err := c.AddResource(resource, schemaDoc); err != nil {
		return nil, clientSafeCompileError(err)
	}
	compiled, err := c.Compile(resource)
	if err != nil {
		return nil, clientSafeCompileError(err)
	}
	return compiled, nil
}

// clientSafeCompileError strips the target URL from a refused load so
// the 400 body carries the policy, not the caller's probe echoed back.
func clientSafeCompileError(err error) error {
	var loadErr *jsonschema.LoadURLError
	if errors.As(err, &loadErr) {
		return fmt.Errorf("schema: %w", errExternalRef)
	}
	return fmt.Errorf("schema: %w", err)
}

// Validate decodes the payload and checks it against the latest loaded
// schema for the topic. Returns ErrSchemaNotFound if none is loaded.
func (r *JSONSchema) Validate(_ context.Context, topic string, payload []byte) error {
	r.mu.RLock()
	version, ok := r.versions[topic]
	if !ok {
		r.mu.RUnlock()
		return ErrSchemaNotFound
	}
	compiled := r.schemas[topic][version]
	r.mu.RUnlock()

	var instance any
	if err := json.Unmarshal(payload, &instance); err != nil {
		return fmt.Errorf("schema: invalid JSON payload: %w", err)
	}

	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	return nil
}
