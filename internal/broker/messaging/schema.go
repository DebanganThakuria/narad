package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
)

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

func (e *Engine) loadPersistedSchemasCached(ctx context.Context, topicName string) (bool, error) {
	version, ok := e.schemaVersion(topicName)
	if !ok {
		return e.loadPersistedSchemas(ctx, topicName)
	}

	for {
		e.cacheMu.RLock()
		cached, hit := e.schemaLoadCache[topicName]
		e.cacheMu.RUnlock()
		if hit && cached.version == version {
			if current, _ := e.schemaVersion(topicName); current == version {
				return cached.loaded, nil
			}
			version, _ = e.schemaVersion(topicName)
			continue
		}

		loaded, err := e.loadPersistedSchemas(ctx, topicName)
		current, _ := e.schemaVersion(topicName)
		if current != version {
			version = current
			continue
		}
		if err != nil {
			return false, err
		}
		e.cacheMu.Lock()
		e.schemaLoadCache[topicName] = cachedSchemaLoad{
			loaded:  loaded,
			version: version,
		}
		e.cacheMu.Unlock()
		return loaded, nil
	}
}

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
