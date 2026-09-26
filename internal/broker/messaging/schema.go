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
// adoption) is the schema reloaded, replacing whatever was there.
// Loading incrementally on ErrSchemaNotFound, as this used to, meant a
// node that had ever validated a topic kept its first-loaded version
// forever.
//
// A reload runs as one flight per topic (hydrateTopicSchemas), shared
// by every produce that misses meanwhile, so a schema change costs one
// metastore read and one compile per node rather than one per produce
// in flight. The flight's result carries the version it loaded at; a
// produce that joined a flight begun before the version it observed
// moved again runs another.
func (e *Engine) syncTopicSchemas(ctx context.Context, topicName string) error {
	version, _ := e.schemaVersion(topicName)
	e.cacheMu.RLock()
	entry, hit := e.schemaLoadCache[topicName]
	e.cacheMu.RUnlock()
	if hit && entry.version == version {
		return nil
	}
	// The flight is shared, so one caller's cancellation must not fail
	// it for the others; the work is bounded either way.
	flightCtx := context.WithoutCancel(ctx)
	for {
		loaded, err, _ := e.schemaFlights.Do(topicName, func() (any, error) {
			return e.hydrateTopicSchemas(flightCtx, topicName)
		})
		if current, _ := e.schemaVersion(topicName); loaded.(uint64) == current {
			return err
		}
	}
}

// hydrateTopicSchemas is the body of a topic's schema flight. It
// reloads the registry until the schema version it read before loading
// is still current after the registry write, records that version in
// the cache and returns it. Flights for one topic never overlap, so the
// registry's last write always comes from the newest history read: a
// load that raced a schema change is redone at the new version rather
// than trusted, because a slower, older load finishing after a newer
// one would otherwise leave the node validating against the older
// schema while the cache said the newer one was loaded.
func (e *Engine) hydrateTopicSchemas(ctx context.Context, topicName string) (uint64, error) {
	for {
		version, _ := e.schemaVersion(topicName)
		e.cacheMu.RLock()
		entry, hit := e.schemaLoadCache[topicName]
		e.cacheMu.RUnlock()
		if hit && entry.version == version {
			// An earlier flight loaded this version already.
			return version, nil
		}
		has, err := schema.Hydrate(ctx, e.metastore, e.schemas, topicName)
		if current, _ := e.schemaVersion(topicName); current != version {
			continue
		}
		if err != nil {
			return version, err
		}
		e.cacheMu.Lock()
		e.schemaLoadCache[topicName] = cached[bool]{value: has, version: version}
		e.cacheMu.Unlock()
		return version, nil
	}
}

func schemaValidationError(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalid, err)
}
