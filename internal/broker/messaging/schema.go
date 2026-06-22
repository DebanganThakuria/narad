package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	now := time.Now()
	e.cacheMu.RLock()
	cached, hit := e.schemaLoadCache[topicName]
	e.cacheMu.RUnlock()
	if hit && now.Before(cached.expires) {
		return cached.loaded, nil
	}

	loaded, err := e.loadPersistedSchemas(ctx, topicName)
	if err != nil {
		return false, err
	}
	e.cacheMu.Lock()
	e.schemaLoadCache[topicName] = cachedSchemaLoad{
		loaded:  loaded,
		expires: now.Add(e.cacheTTL),
	}
	e.cacheMu.Unlock()
	return loaded, nil
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
