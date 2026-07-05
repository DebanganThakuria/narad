package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// validateProducePayload validates a payload against the topic's
// schema. A registry miss is not a rejection: persisted schemas are
// loaded lazily (a peer may have registered one this node hasn't seen)
// and only then is validation retried; a topic with no schema at all
// accepts any payload.
func (e *Engine) validateProducePayload(ctx context.Context, topicName string, payload []byte) error {
	err := e.schemas.Validate(ctx, topicName, payload)
	if err == nil {
		return nil
	}
	if !errors.Is(err, errs.ErrSchemaNotFound) {
		return schemaValidationError(err)
	}

	loaded, err := e.loadPersistedSchemasCached(ctx, topicName)
	if err != nil {
		return err
	}
	if !loaded {
		return nil
	}
	if err := e.schemas.Validate(ctx, topicName, payload); err != nil {
		return schemaValidationError(err)
	}
	return nil
}

// loadPersistedSchemasCached memoizes loadPersistedSchemas per topic,
// keyed by the metastore's schema version, so the common "topic has no
// schema" produce path doesn't rescan the metastore on every request.
func (e *Engine) loadPersistedSchemasCached(ctx context.Context, topicName string) (bool, error) {
	version, ok := e.schemaVersion(topicName)
	if !ok {
		return e.loadPersistedSchemas(ctx, topicName)
	}
	return lookupCached(&e.cacheMu, e.schemaLoadCache, topicName, version,
		func() uint64 { v, _ := e.schemaVersion(topicName); return v },
		func() (bool, error) { return e.loadPersistedSchemas(ctx, topicName) },
		nil,
	)
}

// loadPersistedSchemas loads every persisted schema version for the
// topic into the local registry, in order, and reports whether any
// existed.
func (e *Engine) loadPersistedSchemas(ctx context.Context, topicName string) (bool, error) {
	loaded := false
	for version := 1; ; version++ {
		raw, err := e.metastore.GetSchema(ctx, topicName, version)
		if errors.Is(err, errs.ErrNotFound) {
			return loaded, nil
		}
		if err != nil {
			return false, fmt.Errorf("messaging: load schema %s v%d: %w", topicName, version, err)
		}
		if err := e.schemas.Load(ctx, topicName, version, raw); err != nil {
			return false, fmt.Errorf("messaging: load schema %s v%d: %w", topicName, version, err)
		}
		loaded = true
	}
}

func schemaValidationError(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalid, err)
}
