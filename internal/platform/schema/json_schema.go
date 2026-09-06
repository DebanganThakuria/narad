package schema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Registration limits. They bound what a topic owner can make every
// node compile: compile time grows roughly cubically with nesting
// depth (64 levels compile in a few milliseconds, 1000 levels in over
// a second, 3000 levels in half a minute), and the compiled schema is
// rebuilt on every node the first time it validates a produce after
// the history changes. Size is bounded separately because a wide
// schema compiles linearly but is copied into every fan-out child's
// history and every snapshot.
const (
	// MaxSchemaBytes is the largest schema document accepted at
	// registration.
	MaxSchemaBytes = 256 << 10
	// MaxSchemaDepth is the deepest nesting of objects and arrays a
	// schema document may have at registration. It matches the
	// compatibility check's recursion bound, so every registrable
	// schema can also be evolved.
	MaxSchemaDepth = 64
	// maxValidationErrorBytes caps the message of a rejected payload. A
	// large enum otherwise turns a six-byte produce into a 400 body
	// listing every allowed value (half a megabyte for 50k values).
	maxValidationErrorBytes = 2048
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

// ValidateDefinition checks that schemaBytes is a schema this registry
// will accept at registration: within MaxSchemaBytes and
// MaxSchemaDepth, an object or true at the root (false would reject
// every message, and anything else is not a schema), and compilable.
// Nothing is registered. Persisted schemas are never re-checked
// against the limits, so tightening a limit cannot make an existing
// topic's history fail to load.
func (r *JSONSchema) ValidateDefinition(_ context.Context, topic string, schemaBytes []byte) error {
	if err := checkDefinitionLimits(schemaBytes); err != nil {
		return err
	}
	_, err := compileSchema(topic, 0, schemaBytes)
	return err
}

// checkDefinitionLimits enforces the registration limits with one
// pass over the raw bytes, before any decoding. Malformed JSON is
// left for the compiler to report.
func checkDefinitionLimits(schemaBytes []byte) error {
	if len(schemaBytes) > MaxSchemaBytes {
		return fmt.Errorf("schema: document is %d bytes; the maximum is %d", len(schemaBytes), MaxSchemaBytes)
	}
	trimmed := bytes.TrimLeft(schemaBytes, " \t\r\n")
	switch {
	case len(trimmed) == 0:
		return errors.New("schema: document is empty")
	case trimmed[0] == '{':
	case bytes.HasPrefix(trimmed, []byte("true")):
	case bytes.HasPrefix(trimmed, []byte("false")):
		return errors.New("schema: false would reject every message; use true or {} to accept any JSON value")
	default:
		return errors.New("schema: document must be a JSON object (or true, which accepts any JSON value)")
	}
	if depth := jsonNestingDepth(schemaBytes); depth > MaxSchemaDepth {
		return fmt.Errorf("schema: document nests %d levels deep; the maximum is %d", depth, MaxSchemaDepth)
	}
	return nil
}

// jsonNestingDepth returns the deepest nesting of objects and arrays
// in doc, ignoring brackets inside strings. Malformed input yields
// some number; the compiler reports the syntax error afterwards.
func jsonNestingDepth(doc []byte) int {
	depth, deepest := 0, 0
	inString, escaped := false, false
	for _, b := range doc {
		if inString {
			switch {
			case escaped:
				escaped = false
			case b == '\\':
				escaped = true
			case b == '"':
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > deepest {
				deepest = depth
			}
		case '}', ']':
			depth--
		}
	}
	return deepest
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
// step. Only the latest version is compiled: Validate never consults
// an older one, and compiling every version made a hydrate cost as
// much as the whole history instead of one schema. The compile happens
// before the lock is taken, so a concurrent Validate sees either the
// old history or the new one, never an empty topic in between (which
// would let a payload through unvalidated). An empty history drops the
// topic.
func (r *JSONSchema) ReplaceTopic(_ context.Context, topic string, history []Version) error {
	latest := Version{}
	for _, v := range history {
		if v.Number <= 0 {
			return fmt.Errorf("schema: %s: invalid version %d", topic, v.Number)
		}
		if v.Number > latest.Number {
			latest = v
		}
	}
	var compiled *jsonschema.Schema
	if latest.Number > 0 {
		s, err := compileSchema(topic, latest.Number, latest.Raw)
		if err != nil {
			return fmt.Errorf("v%d: %w", latest.Number, err)
		}
		compiled = s
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if latest.Number == 0 {
		delete(r.schemas, topic)
		delete(r.versions, topic)
		return nil
	}
	r.schemas[topic] = map[int]*jsonschema.Schema{latest.Number: compiled}
	r.versions[topic] = latest.Number
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
	// The library asserts "format" only for draft-07 and earlier and
	// treats it as an annotation from 2019-09 on (the default draft).
	// One contract for every draft: format is always asserted. The
	// compatibility check already treats it as a constraint.
	c.AssertFormat()
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
//
// The payload must be a single JSON text in valid UTF-8: encoding/json
// silently replaces invalid bytes inside strings, which would let a
// schema topic store bytes that its consume response then emits
// verbatim inside a JSON envelope. Numbers are decoded as json.Number,
// so integers beyond 2^53 and exponents beyond float64 keep their
// exact value for type, multipleOf and bound checks; the library does
// the arithmetic in big.Rat. That costs about a fifth more than a
// float64 decode (BenchmarkValidatePayloadDecode) and is the
// documented contract.
func (r *JSONSchema) Validate(_ context.Context, topic string, payload []byte) error {
	r.mu.RLock()
	version, ok := r.versions[topic]
	if !ok {
		r.mu.RUnlock()
		return ErrSchemaNotFound
	}
	compiled := r.schemas[topic][version]
	r.mu.RUnlock()

	if !utf8.Valid(payload) {
		return errors.New("schema: invalid JSON payload: not valid UTF-8")
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("schema: invalid JSON payload: %w", err)
	}

	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("schema: %w", boundedError(err, maxValidationErrorBytes))
	}
	return nil
}

// boundedError caps err's message at limit bytes (on a rune boundary)
// while keeping the original reachable through Unwrap.
func boundedError(err error, limit int) error {
	msg := err.Error()
	if len(msg) <= limit {
		return err
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return &truncatedError{msg: msg[:cut] + fmt.Sprintf("... (%d more bytes)", len(msg)-cut), cause: err}
}

type truncatedError struct {
	msg   string
	cause error
}

func (e *truncatedError) Error() string { return e.msg }
func (e *truncatedError) Unwrap() error { return e.cause }

// Equal reports whether a and b are the same JSON value (numbers
// compared numerically, object key order ignored), which is what
// makes re-registering the current schema a no-op.
func Equal(a, b []byte) bool {
	da, err := jsonschema.UnmarshalJSON(bytes.NewReader(a))
	if err != nil {
		return false
	}
	db, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return false
	}
	return jsonEqual(da, db)
}
