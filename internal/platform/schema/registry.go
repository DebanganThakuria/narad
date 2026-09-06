// Package schema validates message payloads against per-topic JSON
// Schemas.
//
// JSONSchema is the production Registry, backed by
// github.com/santhosh-tekuri/jsonschema. AlwaysValid is a permissive
// stub for tests and for topics without schema enforcement.
//
// Version numbers are NOT assigned here. The metastore is the source of
// truth for a topic's schema history: the topics manager reads the
// persisted history, checks the new schema against the persisted
// latest, and proposes max+1 through Raft, whose state machine refuses
// anything else. The registry only holds compiled copies of persisted
// versions so the produce path can validate without touching the
// metastore; PersistedHistory and Hydrate are the one way those copies
// are (re)built.
package schema

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Registry stores compiled per-topic JSON Schemas and validates
// payloads against the latest loaded version.
//
//   - ValidateDefinition checks that a schema compiles without storing it.
//   - CheckCompatible reports whether next accepts every document
//     previous accepts (see JSONSchema.CheckCompatible for the exact
//     rules).
//   - Load compiles and stores one (topic, version, schema); it never
//     touches other versions.
//   - ReplaceTopic atomically swaps the topic's whole loaded history for
//     the given one (an empty history drops the topic). Validate never
//     observes a half-replaced topic.
//   - DropTopic removes all of a topic's versions so a recreated topic
//     starts schema-less.
//   - Validate checks a payload against the topic's latest loaded
//     version and returns ErrSchemaNotFound when none is loaded.
type Registry interface {
	ValidateDefinition(ctx context.Context, topic string, schema []byte) error
	CheckCompatible(ctx context.Context, topic string, previous, next []byte) error
	Load(ctx context.Context, topic string, version int, schema []byte) error
	ReplaceTopic(ctx context.Context, topic string, history []Version) error
	DropTopic(ctx context.Context, topic string) error
	Validate(ctx context.Context, topic string, payload []byte) error
}

// Version is one persisted schema version of a topic.
type Version struct {
	Number int
	Raw    []byte
}

// Source is the read side of the metastore the registry hydrates from.
// GetSchema returns errs.ErrNotFound for a version that does not exist.
type Source interface {
	GetSchema(ctx context.Context, topic string, version int) ([]byte, error)
}

// PersistedHistory returns every persisted schema version of topic in
// ascending order, or an empty slice when the topic has none. Versions
// are contiguous from 1 (the metastore's state machine enforces it), so
// the scan stops at the first missing number.
func PersistedHistory(ctx context.Context, src Source, topic string) ([]Version, error) {
	var history []Version
	for number := 1; ; number++ {
		raw, err := src.GetSchema(ctx, topic, number)
		if errors.Is(err, errs.ErrNotFound) {
			return history, nil
		}
		if err != nil {
			return nil, fmt.Errorf("schema: read %s v%d: %w", topic, number, err)
		}
		history = append(history, Version{Number: number, Raw: raw})
	}
}

// Hydrate makes reg's copy of topic's schema history identical to what
// src holds, replacing whatever was loaded before, and reports whether
// the topic has any schema at all. It is the only correct way to bring
// a registry up to date: a version registered by another node, a
// delete-and-recreate under the same name, or an attach-time adoption
// can all change the history in ways an incremental Load would miss.
func Hydrate(ctx context.Context, src Source, reg Registry, topic string) (bool, error) {
	history, err := PersistedHistory(ctx, src, topic)
	if err != nil {
		return false, err
	}
	if err := reg.ReplaceTopic(ctx, topic, history); err != nil {
		return false, fmt.Errorf("schema: load %s: %w", topic, err)
	}
	return len(history) > 0, nil
}
