package schema

import (
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// useFirstFailure moves s's array keywords into extensions that stop
// at the first failing item. It is applied only to the yes-or-no form
// (compileCheckForm), whose result one failure decides: the library
// validates every item even there and keeps a placeholder error for
// each failing one until the whole validation returns.
//
// The keywords are cleared after compiling, so what the compiler
// derived from them (which items count as evaluated for
// unevaluatedItems) stands, and the extensions reproduce the library's
// rules exactly: before draft 2020-12 an items array is a tuple, an
// items schema covers every item, and additionalItems applies past the
// tuple (to every item when items is absent, as the library does); from
// 2020-12 on prefixItems is the tuple and items the rest.
func useFirstFailure(s *jsonschema.Schema) {
	var ext *itemsFirstFailure
	if s.DraftVersion < 2020 {
		switch items := s.Items.(type) {
		case *jsonschema.Schema:
			ext = &itemsFirstFailure{rest: items}
		case []*jsonschema.Schema:
			ext = &itemsFirstFailure{prefix: items}
			ext.tail(s.AdditionalItems)
		case nil:
			if s.AdditionalItems != nil {
				ext = &itemsFirstFailure{}
				ext.tail(s.AdditionalItems)
			}
		}
		if ext != nil {
			s.Items, s.AdditionalItems = nil, nil
		}
	} else if s.PrefixItems != nil || s.Items2020 != nil {
		ext = &itemsFirstFailure{prefix: s.PrefixItems, rest: s.Items2020}
		s.PrefixItems, s.Items2020 = nil, nil
	}
	if ext != nil {
		s.Extensions = append(s.Extensions, ext)
	}
	if s.Contains != nil {
		s.Extensions = append(s.Extensions, &containsCount{
			sch: s.Contains, min: s.MinContains, max: s.MaxContains, marks: s.DraftVersion >= 2020,
		})
		s.Contains = nil
	}
}

// itemsFirstFailure validates items against the tuple schemas in
// prefix, then every later item against rest, and refuses items past
// the tuple when closed; it stops at the first failure.
type itemsFirstFailure struct {
	prefix []*jsonschema.Schema
	rest   *jsonschema.Schema
	closed bool
}

func (x *itemsFirstFailure) tail(additional any) {
	switch t := additional.(type) {
	case bool:
		x.closed = !t
	case *jsonschema.Schema:
		x.rest = t
	}
}

// Validate implements jsonschema.SchemaExt.
func (x *itemsFirstFailure) Validate(ctx *jsonschema.ValidatorContext, v any) {
	arr, ok := v.([]any)
	if !ok {
		return
	}
	for i, item := range arr {
		sch := x.rest
		if i < len(x.prefix) {
			sch = x.prefix[i]
		} else if x.closed {
			ctx.AddError(&kind.AdditionalItems{Count: len(arr) - len(x.prefix)})
			return
		}
		if sch == nil {
			return
		}
		if err := ctx.Validate(sch, item, []string{strconv.Itoa(i)}); err != nil {
			ctx.AddErr(err)
			return
		}
	}
}

// containsCount is the library's contains, minContains and maxContains
// without the error it keeps for every item that does not match.
type containsCount struct {
	sch      *jsonschema.Schema
	min, max *int
	marks    bool // 2020-12: a matching item counts as evaluated
}

// Validate implements jsonschema.SchemaExt.
func (x *containsCount) Validate(ctx *jsonschema.ValidatorContext, v any) {
	arr, ok := v.([]any)
	if !ok {
		return
	}
	var matched []int
	for i, item := range arr {
		if ctx.Validate(x.sch, item, []string{strconv.Itoa(i)}) == nil {
			matched = append(matched, i)
			if x.marks {
				ctx.EvaluatedItem(i)
			}
		}
	}
	switch {
	case x.min != nil:
		if len(matched) < *x.min {
			ctx.AddError(&kind.MinContains{Got: matched, Want: *x.min})
		}
	case len(matched) == 0:
		ctx.AddError(&kind.Contains{})
	}
	if x.max != nil && len(matched) > *x.max {
		ctx.AddError(&kind.MaxContains{Got: matched, Want: *x.max})
	}
}

// walkCompiled calls fn on every compiled subschema reachable from
// root through the schema's keywords, each once. It never visits a
// draft metaschema: those are shared by every compiler, and a user
// schema never reaches them (external references are refused), but
// fn must never change one if it did.
func walkCompiled(root *jsonschema.Schema, fn func(*jsonschema.Schema)) {
	seen := map[*jsonschema.Schema]bool{root: true}
	stack := []*jsonschema.Schema{root}
	push := func(s *jsonschema.Schema) {
		if s != nil && !seen[s] {
			seen[s] = true
			stack = append(stack, s)
		}
	}
	pushAny := func(v any) {
		switch t := v.(type) {
		case *jsonschema.Schema:
			push(t)
		case []*jsonschema.Schema:
			for _, s := range t {
				push(s)
			}
		}
	}
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if strings.HasPrefix(s.Location, "http://json-schema.org/") || strings.HasPrefix(s.Location, "https://json-schema.org/") {
			continue
		}
		// Collect the children before fn, which may move keywords
		// into extensions.
		push(s.Ref)
		push(s.RecursiveRef)
		if s.DynamicRef != nil {
			push(s.DynamicRef.Ref)
		}
		push(s.Not)
		push(s.If)
		push(s.Then)
		push(s.Else)
		for _, l := range [][]*jsonschema.Schema{s.AllOf, s.AnyOf, s.OneOf, s.PrefixItems} {
			for _, c := range l {
				push(c)
			}
		}
		push(s.PropertyNames)
		for _, c := range s.Properties {
			push(c)
		}
		for _, c := range s.PatternProperties {
			push(c)
		}
		pushAny(s.AdditionalProperties)
		for _, d := range s.Dependencies {
			pushAny(d)
		}
		for _, c := range s.DependentSchemas {
			push(c)
		}
		push(s.UnevaluatedProperties)
		push(s.Contains)
		pushAny(s.Items)
		pushAny(s.AdditionalItems)
		push(s.Items2020)
		push(s.UnevaluatedItems)
		push(s.ContentSchema)
		fn(s)
	}
}
