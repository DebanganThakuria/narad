package schema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
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
	// MaxNumberExponent bounds the exponent part of a number literal,
	// in payloads at validation and in schemas at registration. Numbers
	// are validated exactly, which means expanding 1eN into an N-digit
	// integer: at N = 999999 that costs about 12 ms per literal (a
	// 1 MiB payload holds 130k of them), and beyond 10^6 the big.Rat
	// parser refuses the literal and the validator dereferences the nil
	// result. Every literal that fits in a float64 (and far more: 1e400
	// is fine) stays well within this bound.
	MaxNumberExponent = 1000
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
	schemaDoc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaBytes))
	if err != nil {
		return fmt.Errorf("schema: invalid JSON: %w", err)
	}
	// Checked after decoding so malformed JSON is reported as such.
	if err := checkNumberExponents(schemaBytes); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	if err := checkInDocumentRefs(schemaDoc); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	if _, err := compileDecoded(topic, 0, schemaDoc); err != nil {
		return err
	}
	// After compiling, so the analysis sees a well-formed document.
	if err := checkRevalidation(schemaDoc); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	return nil
}

// checkInDocumentRefs refuses every reference that is not a fragment
// ("#...") anywhere a subschema can sit, including $defs nobody
// points at yet. The compiler only compiles what the root reaches, so
// an external $ref in an unreferenced definition would otherwise
// register (nothing loads it, but the documented rule is that it is
// refused, and a later version that references it would fail at
// registration for a reason its author did not introduce). An empty
// reference and an absolute URL that happens to equal the schema's own
// $id resolve in-document for the compiler, but they are the relative
// and http(s) forms the contract refuses.
func checkInDocumentRefs(doc any) error {
	var walk func(v any, depth int) error
	walk = func(v any, depth int) error {
		if depth > MaxSchemaDepth {
			return nil // the depth limit reports this
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		for _, k := range []string{"$ref", "$dynamicRef", "$recursiveRef"} {
			if ref, ok := obj[k].(string); ok && !strings.HasPrefix(ref, "#") {
				return errExternalRef
			}
		}
		for k, child := range obj {
			var err error
			switch k {
			case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas", "dependencies":
				if m, ok := child.(map[string]any); ok {
					for _, sub := range m {
						if err = walk(sub, depth+1); err != nil {
							return err
						}
					}
				}
			case "allOf", "anyOf", "oneOf", "prefixItems", "items":
				if arr, ok := child.([]any); ok {
					for _, sub := range arr {
						if err = walk(sub, depth+1); err != nil {
							return err
						}
					}
				} else {
					err = walk(child, depth+1)
				}
			case "additionalProperties", "additionalItems", "not", "if", "then", "else", "contains",
				"propertyNames", "unevaluatedProperties", "unevaluatedItems", "contentSchema":
				err = walk(child, depth+1)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	return walk(doc, 0)
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

// checkNumberExponents scans a JSON text (already known to decode) and
// refuses any number literal whose exponent part lies outside
// ±MaxNumberExponent. Strings are skipped; outside strings an 'e' or
// 'E' followed by an optional sign and digits can only be a number's
// exponent (the 'e' in true and false is followed by a delimiter).
func checkNumberExponents(doc []byte) error {
	inString, escaped := false, false
	for i := 0; i < len(doc); i++ {
		b := doc[i]
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
		case 'e', 'E':
			j := i + 1
			if j < len(doc) && (doc[j] == '+' || doc[j] == '-') {
				j++
			}
			first := j
			for j < len(doc) && doc[j] == '0' {
				j++
			}
			significant := j
			for j < len(doc) && doc[j] >= '0' && doc[j] <= '9' {
				j++
			}
			if j == first {
				continue // not an exponent
			}
			digits := doc[significant:j]
			over := len(digits) > 4
			if !over && len(digits) == 4 {
				n, _ := strconv.Atoi(string(digits))
				over = n > MaxNumberExponent
			}
			if over {
				lit := doc[i:j]
				if len(lit) > 24 {
					lit = append(append([]byte{}, lit[:21]...), "..."...)
				}
				return fmt.Errorf("number exponent %q is outside ±%d; numbers are validated exactly and a larger exponent is unbounded work", lit, MaxNumberExponent)
			}
			i = j - 1
		}
	}
	return nil
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
	return compileDecoded(topic, version, schemaDoc)
}

// compileDecoded compiles an already decoded schema document. It
// applies no registration limit: persisted schemas go through here on
// every hydrate and must keep loading.
func compileDecoded(topic string, version int, schemaDoc any) (*jsonschema.Schema, error) {
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
	if err := checkNumberExponents(payload); err != nil {
		return fmt.Errorf("schema: invalid JSON payload: %w", err)
	}

	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("schema: %w", boundedError(err, maxValidationErrorBytes))
	}
	return nil
}

// boundedError caps err's message at limit bytes (on a rune boundary)
// while keeping the original reachable through Unwrap. The message is
// rendered under the cap rather than rendered whole and then cut: the
// library's Error() writes one line per failing value, each listing
// every allowed enum value, so a 1 MiB array against a 20k-value enum
// produced a multi-gigabyte string (and took minutes) before the cap
// could apply.
func boundedError(err error, limit int) error {
	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return truncateMessage(err, err.Error(), limit)
	}
	var sb strings.Builder
	omitted := renderBounded(&sb, verr, 0, limit)
	msg := sb.String()
	if omitted == 0 && len(msg) <= limit {
		return &truncatedError{msg: msg, cause: err}
	}
	if len(msg) > limit {
		return truncateMessage(err, msg, limit)
	}
	return &truncatedError{msg: msg + fmt.Sprintf("... (%d more errors)", omitted), cause: err}
}

// renderBounded writes err and its causes in the library's own layout
// ("at '/loc': reason", nested causes indented with "- "), stopping
// once the builder holds limit bytes. It returns the number of errors
// left unrendered.
func renderBounded(sb *strings.Builder, e *jsonschema.ValidationError, indent, limit int) int {
	if sb.Len() >= limit {
		return countErrors(e)
	}
	// A reference error with a single cause is transparent in the
	// library's rendering; keep that so messages stay familiar.
	if _, isRef := e.ErrorKind.(*kind.Reference); !(isRef && len(e.Causes) == 1) {
		if indent > 0 {
			sb.WriteByte('\n')
			for i := 0; i < indent-1; i++ {
				sb.WriteString("  ")
			}
			sb.WriteString("- ")
		}
		indent++
		if _, isSchema := e.ErrorKind.(*kind.Schema); isSchema {
			sb.WriteString(e.ErrorKind.LocalizedString(errorPrinter))
		} else {
			fmt.Fprintf(sb, "at '%s': %s", instancePointer(e.InstanceLocation), e.ErrorKind.LocalizedString(errorPrinter))
		}
	}
	omitted := 0
	for _, cause := range e.Causes {
		omitted += renderBounded(sb, cause, indent, limit)
	}
	return omitted
}

func countErrors(e *jsonschema.ValidationError) int {
	n := 1
	for _, c := range e.Causes {
		n += countErrors(c)
	}
	return n
}

func instancePointer(loc []string) string {
	var sb strings.Builder
	for _, tok := range loc {
		sb.WriteByte('/')
		sb.WriteString(strings.ReplaceAll(strings.ReplaceAll(tok, "~", "~0"), "/", "~1"))
	}
	return sb.String()
}

var errorPrinter = message.NewPrinter(language.English)

func truncateMessage(cause error, msg string, limit int) error {
	if len(msg) <= limit {
		return cause
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return &truncatedError{msg: msg[:cut] + fmt.Sprintf("... (%d more bytes)", len(msg)-cut), cause: cause}
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
