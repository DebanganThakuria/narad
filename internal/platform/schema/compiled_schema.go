package schema

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// A detailed validation report is built only when it stays under
// detailBudgetBytes. The validator builds an error for every failing
// value and every subschema that failed on it, each with a copy of the
// path to the value; errorNodeBytes and pathTokenBytes are what one
// error and one token of such a path hold (measured on the library's
// ValidationError with its kind and slice headers).
const (
	detailBudgetBytes = 32 << 20
	errorNodeBytes    = 160
	pathTokenBytes    = 16
)

// A schema whose validations always run under the validation limit,
// whatever the payload size: one value receives more than
// expensiveFanout subschema applications, or the schema's required,
// dependentRequired and dependencies lists name more than
// expensiveRequiredNames properties in all (each object is checked
// against every name), or it has more than expensivePatternProperties
// patternProperties keys in all (each member name is matched against
// every key). Past these, one small payload costs time in proportion
// to the schema's size, not only its own.
const (
	expensiveFanout            = 64
	expensiveRequiredNames     = 1024
	expensivePatternProperties = 64
)

// compiledSchema is one topic schema version ready to validate against.
type compiledSchema struct {
	// full reports every violation, with its location.
	full *jsonschema.Schema
	// cost is what the cost analyses found in the document.
	cost schemaCost

	// The yes-or-no form, compiled from raw on first use (checkForm):
	// most topics never receive a payload that needs it.
	topic     string
	version   int
	raw       []byte
	checkOnce sync.Once
	check     *jsonschema.Schema
	checkErr  error
}

// schemaCost is what the registration cost checks found in a schema
// document. ValidateDefinition refuses a document with either error;
// a persisted document with one keeps loading and is validated under
// the node's validation limit.
type schemaCost struct {
	paths   error // a subschema reached through too many validation paths, or the analysis gave up
	pattern error // a pattern too costly to match
	fanout  int   // most subschema applications one value receives

	// longestList is the most names one required, dependentRequired or
	// dependencies list holds: an error on that keyword lists every
	// missing one of them.
	longestList int
	// wide is set when the schema's name lists or patternProperties
	// keys pass expensiveRequiredNames or expensivePatternProperties.
	wide bool
}

// analyzeCost runs the cost analyses on the compiled schema root (doc
// compiled under resource with c): the validation path count and the
// pattern bound on the subschemas the compiler resolved, and the name
// list and patternProperties totals.
func analyzeCost(c *jsonschema.Compiler, resource string, doc any, root *jsonschema.Schema) schemaCost {
	var cost schemaCost
	g, err := buildCompiledGraph(c, resource, doc, root)
	if err != nil {
		cost.fanout, cost.paths = pathCountCap, err
		return cost
	}
	cost.fanout, cost.paths = countValidationPaths(g.schemaGraph)
	cost.pattern = checkPatternCosts(g)
	names, patterns := 0, 0
	list := func(l []string) {
		names += len(l)
		cost.longestList = max(cost.longestList, len(l))
	}
	for _, s := range g.schemas {
		list(s.Required)
		for _, l := range s.DependentRequired {
			list(l)
		}
		for _, d := range s.Dependencies {
			if l, ok := d.([]string); ok {
				list(l)
			}
		}
		patterns += len(s.PatternProperties)
	}
	cost.wide = names > expensiveRequiredNames || patterns > expensivePatternProperties
	return cost
}

// expensive reports whether every validation against the schema runs
// under the node's validation limit.
func (cs *compiledSchema) expensive() bool {
	return cs.cost.paths != nil || cs.cost.pattern != nil || cs.cost.fanout > expensiveFanout || cs.cost.wide
}

// detailCost estimates the bytes a detailed report on a payload with
// stats could hold: every value failing on every subschema applied to
// it, each error carrying the path to its value and, on a required,
// dependentRequired or dependencies keyword, every name it misses.
func (cs *compiledSchema) detailCost(st payloadStats) int64 {
	fanout := int64(min(max(cs.cost.fanout, 1), pathCountCap))
	perError := int64(errorNodeBytes) + int64(cs.cost.longestList)*pathTokenBytes
	return fanout * (int64(st.values)*perError + int64(st.pathLen)*pathTokenBytes)
}

// schemaCheckURL is the resource the yes-or-no form of a topic schema
// is compiled under.
func schemaCheckURL(topic string, version int) string {
	return fmt.Sprintf("narad://schema-check/%s/%d", topic, version)
}

// newSchemaCompiler returns a compiler configured the way every topic
// schema is compiled.
func newSchemaCompiler() *jsonschema.Compiler {
	c := jsonschema.NewCompiler()
	c.UseLoader(noExternalRefs{})
	// The library asserts "format" only for draft-07 and earlier and
	// treats it as an annotation from 2019-09 on (the default draft).
	// One contract for every draft: format is always asserted. The
	// compatibility check already treats it as a constraint.
	c.AssertFormat()
	return c
}

// compileTopic compiles a decoded schema document, swaps large enums
// for hash lookups, and runs the cost analyses. raw is the document's
// bytes, kept (copied) to compile the yes-or-no form on demand; nil
// when that form will never be needed. It applies no registration
// limit: persisted schemas go through here on every hydrate and must
// keep loading.
func compileTopic(topic string, version int, schemaDoc any, raw []byte) (*compiledSchema, error) {
	c := newSchemaCompiler()
	resource := schemaResourceURL(topic, version)
	if err := c.AddResource(resource, schemaDoc); err != nil {
		return nil, clientSafeCompileError(err)
	}
	full, err := c.Compile(resource)
	if err != nil {
		return nil, clientSafeCompileError(err)
	}
	// Before the enum swap, which the analyses do not look at; the
	// dynamic anchor lookup compiles more locations with c.
	cost := analyzeCost(c, resource, schemaDoc, full)
	walkCompiled(full, useEnumSet)
	return &compiledSchema{
		full:    full,
		cost:    cost,
		topic:   topic,
		version: version,
		raw:     slices.Clone(raw),
	}, nil
}

// checkForm returns the yes-or-no form of the schema: it accepts
// exactly what full accepts and builds no report. It applies the schema
// under "if", which the validator evaluates for a yes or no only (every
// error it builds is an empty placeholder, and objects and allOf stop at
// their first failure), and its arrays also stop at their first failing
// item, so a failing payload costs a path's worth of placeholders, not
// one per failing value. It is a graph of its own, compiled on first
// use, because the array change would reorder full's reports.
func (cs *compiledSchema) checkForm() (*jsonschema.Schema, error) {
	cs.checkOnce.Do(func() {
		cs.check, cs.checkErr = compileCheckForm(cs.topic, cs.version, cs.raw)
		cs.raw = nil
	})
	return cs.check, cs.checkErr
}

func compileCheckForm(topic string, version int, raw []byte) (*jsonschema.Schema, error) {
	if raw == nil {
		return nil, errors.New("schema: no document to compile the check form from")
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("schema: invalid JSON: %w", err)
	}
	c := newSchemaCompiler()
	resource := schemaResourceURL(topic, version)
	if err := c.AddResource(resource, doc); err != nil {
		return nil, clientSafeCompileError(err)
	}
	checkURL := schemaCheckURL(topic, version)
	wrapper := map[string]any{"if": map[string]any{"$ref": resource}, "then": true, "else": false}
	if err := c.AddResource(checkURL, wrapper); err != nil {
		return nil, clientSafeCompileError(err)
	}
	check, err := c.Compile(checkURL)
	if err != nil {
		return nil, clientSafeCompileError(err)
	}
	walkCompiled(check, func(s *jsonschema.Schema) {
		useEnumSet(s)
		useFirstFailure(s)
	})
	return check, nil
}
