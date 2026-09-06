package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// validateProducePayload validates a payload against the topic's
// current schema. The local registry is brought in line with the
// metastore first (see syncTopicSchemas); a topic with no persisted
// schema at all accepts any payload.
func (e *Engine) validateProducePayload(ctx context.Context, topicName string, payload []byte) error {
	if err := e.syncTopicSchemas(ctx, topicName); err != nil {
		return err
	}
	err := e.schemas.Validate(ctx, topicName, payload)
	if err == nil || errors.Is(err, errs.ErrSchemaNotFound) {
		return nil
	}
	return schemaValidationError(err)
}

// syncTopicSchemas keys the registry's loaded history for the topic by
// the metastore's schema version. The hot path is one atomic version
// read plus a cache lookup; only when the version differs from the one
// the registry was last loaded at (a version registered on another
// node, a delete-and-recreate under the same name, an attach-time
// adoption) is the whole persisted history reloaded, replacing
// whatever was there. Loading incrementally on ErrSchemaNotFound, as
// this used to, meant a node that had ever validated a topic kept its
// first-loaded version forever.
func (e *Engine) syncTopicSchemas(ctx context.Context, topicName string) error {
	version, _ := e.schemaVersion(topicName)
	e.cacheMu.RLock()
	entry, hit := e.schemaLoadCache[topicName]
	e.cacheMu.RUnlock()
	if hit && entry.version == version {
		return nil
	}
	_, err := lookupCached(&e.cacheMu, e.schemaLoadCache, topicName, version,
		func() uint64 { v, _ := e.schemaVersion(topicName); return v },
		func() (bool, error) { return schema.Hydrate(ctx, e.metastore, e.schemas, topicName) },
		nil,
	)
	return err
}

func schemaValidationError(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalid, err)
}
