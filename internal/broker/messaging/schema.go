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
	hasSchema, err := e.syncTopicSchemas(ctx, topicName)
	if err != nil {
		return err
	}
	err = e.schemas.Validate(ctx, topicName, payload)
	if hasSchema && errors.Is(err, errs.ErrSchemaNotFound) {
		// The registry lost a schema that was loaded (the topic manager
		// drops a retired incarnation's compiled schemas by name, and a
		// same-named successor may be live): load it again rather than
		// let the payload through unvalidated.
		e.forgetSchemaLoad(topicName)
		if _, err = e.syncTopicSchemas(ctx, topicName); err != nil {
			return err
		}
		err = e.schemas.Validate(ctx, topicName, payload)
	}
	if err == nil || errors.Is(err, errs.ErrSchemaNotFound) {
		return nil
	}
	return schemaValidationError(err)
}

// schemaFlightResult is a schema flight's result: the schema version the
// registry was loaded at and whether the topic has a schema.
type schemaFlightResult struct {
	version   uint64
	hasSchema bool
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
// moved again runs another. hasSchema reports whether the topic has a
// persisted schema as of the loaded version.
func (e *Engine) syncTopicSchemas(ctx context.Context, topicName string) (hasSchema bool, err error) {
	version, _ := e.schemaVersion(topicName)
	e.cacheMu.RLock()
	entry, hit := e.schemaLoadCache[topicName]
	e.cacheMu.RUnlock()
	if hit && entry.version == version {
		return entry.value, nil
	}
	// The flight is shared, so one caller's cancellation must not fail
	// it for the others; the work is bounded either way.
	flightCtx := context.WithoutCancel(ctx)
	for {
		result, err, _ := e.schemaFlights.Do(topicName, func() (any, error) {
			return e.hydrateTopicSchemas(flightCtx, topicName)
		})
		loaded := result.(schemaFlightResult)
		if current, _ := e.schemaVersion(topicName); loaded.version == current {
			return loaded.hasSchema, err
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
//
// A forget of this topic that overlaps the load (ForgetTopic or
// forgetSchemaLoad, both after the registry lost the topic's schemas)
// means the registry's copy may have been dropped after this load wrote
// it, so the load is redone before anything is recorded. Only a forget
// of this topic does that: the fence is per topic, so deletes of other
// topics never make a flight redo its load.
func (e *Engine) hydrateTopicSchemas(ctx context.Context, topicName string) (schemaFlightResult, error) {
	for {
		version, _ := e.schemaVersion(topicName)
		e.cacheMu.RLock()
		entry, hit := e.schemaLoadCache[topicName]
		e.cacheMu.RUnlock()
		if hit && entry.version == version {
			// An earlier flight loaded this version already.
			return schemaFlightResult{version: version, hasSchema: entry.value}, nil
		}
		token := e.cacheForgets.begin()
		has, err := schema.Hydrate(ctx, e.metastore, e.schemas, topicName)
		if current, _ := e.schemaVersion(topicName); current != version {
			continue
		}
		if err != nil {
			return schemaFlightResult{version: version}, err
		}
		e.cacheMu.Lock()
		stored := e.cacheForgets.clean(topicName, token)
		if stored {
			e.schemaLoadCache[topicName] = cached[bool]{value: has, version: version}
		}
		e.cacheMu.Unlock()
		if stored {
			return schemaFlightResult{version: version, hasSchema: has}, nil
		}
	}
}

func schemaValidationError(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalid, err)
}
