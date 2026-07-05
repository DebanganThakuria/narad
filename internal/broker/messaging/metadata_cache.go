package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// The hot path (produce/consume/ack) would otherwise hit the metastore
// on every request for topic records, assignments, member liveness,
// and schema presence. Each of those is cached here, keyed by the
// metastore's monotonically increasing metadata versions: a cache
// entry is valid exactly while its version matches the live one.
//
// Metastores that expose per-domain versions (topicVersioner et al.)
// get fine-grained invalidation; ones that only expose a global
// MetadataVersion invalidate everything together; ones with no
// version at all bypass the cache entirely.

// cached pairs a cache value with the metadata version it was loaded
// at.
type cached[V any] struct {
	value   V
	version uint64
}

// lookupCached is the versioned read-through protocol shared by every
// metadata cache. version is the live metadata version observed by
// the caller; currentVersion re-reads it.
//
// Correctness hinges on two re-validations:
//
//   - a cache hit is only served after confirming the version has not
//     moved since the entry was stored;
//   - a freshly loaded value (or load error) is only used after
//     confirming the version did not move DURING the load — a load
//     that raced a metadata change may have seen either side of it,
//     so the result is discarded and the lookup retries.
//
// dropOnError, when non-nil and true for the load error, evicts the
// stale entry so the next lookup doesn't keep serving a value for a
// key that now fails to load.
func lookupCached[V any](
	mu *sync.RWMutex,
	cache map[string]cached[V],
	key string,
	version uint64,
	currentVersion func() uint64,
	load func() (V, error),
	dropOnError func(error) bool,
) (V, error) {
	for {
		mu.RLock()
		entry, hit := cache[key]
		mu.RUnlock()
		if hit && entry.version == version {
			current := currentVersion()
			if current == version {
				return entry.value, nil
			}
			version = current
			continue
		}

		value, err := load()
		if current := currentVersion(); current != version {
			version = current
			continue
		}
		if err != nil {
			if dropOnError != nil && dropOnError(err) {
				mu.Lock()
				delete(cache, key)
				mu.Unlock()
			}
			var zero V
			return zero, err
		}

		mu.Lock()
		cache[key] = cached[V]{value: value, version: version}
		mu.Unlock()
		return value, nil
	}
}

// assignmentSet holds a topic's partition assignments in both list and
// by-partition form so lookups don't rescan the list.
type assignmentSet struct {
	values []metastore.Assignment
	byPart map[int]metastore.Assignment
}

func newAssignmentSet(rows []metastore.Assignment) assignmentSet {
	set := assignmentSet{
		values: rows,
		byPart: make(map[int]metastore.Assignment, len(rows)),
	}
	for _, row := range rows {
		set.byPart[row.Partition] = row
	}
	return set
}

// routingMember is the slice of metastore.Member the routing path
// cares about.
type routingMember struct {
	Status metastore.MemberStatus
	Addr   string
}

// Optional metastore capabilities for cache invalidation. A metastore
// may expose per-domain versions, fall back to a single global
// version, or expose none (in which case caching is bypassed).
type (
	metadataVersioner interface {
		MetadataVersion() uint64
	}
	topicVersioner interface {
		TopicVersion(name string) uint64
	}
	assignmentVersioner interface {
		AssignmentVersion(topicName string) uint64
	}
	schemaVersioner interface {
		SchemaVersion(topicName string) uint64
	}
	routingMembersVersioner interface {
		RoutingMembersVersion() uint64
	}
)

func (e *Engine) getTopic(ctx context.Context, name string) (topic.Topic, error) {
	version, ok := e.topicVersion(name)
	if !ok {
		return e.loadTopic(ctx, name)
	}
	return lookupCached(&e.cacheMu, e.topicCache, name, version,
		func() uint64 { v, _ := e.topicVersion(name); return v },
		func() (topic.Topic, error) { return e.loadTopic(ctx, name) },
		func(err error) bool { return errors.Is(err, ErrTopicNotFound) },
	)
}

func (e *Engine) loadTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := e.metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, fmt.Errorf("messaging: get topic: %w", err)
	}
	return t, nil
}

func (e *Engine) listAssignments(topicName string) ([]metastore.Assignment, error) {
	set, ok, err := e.assignmentsForTopic(topicName)
	if err != nil || !ok {
		return nil, err
	}
	return set.values, nil
}

func (e *Engine) getAssignment(topicName string, partition int) (metastore.Assignment, error) {
	set, ok, err := e.assignmentsForTopic(topicName)
	if err != nil {
		return metastore.Assignment{}, err
	}
	if !ok {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	assignment, ok := set.byPart[partition]
	if !ok {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	return assignment, nil
}

// assignmentsForTopic returns the topic's assignments. ok is false
// when the metastore has no assignment support at all.
func (e *Engine) assignmentsForTopic(topicName string) (assignmentSet, bool, error) {
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return assignmentSet{}, false, nil
	}
	load := func() (assignmentSet, error) {
		rows, err := assignments.ListAssignments(topicName)
		if err != nil {
			return assignmentSet{}, err
		}
		return newAssignmentSet(rows), nil
	}

	version, versioned := e.assignmentVersion(topicName)
	if !versioned {
		set, err := load()
		return set, true, err
	}
	set, err := lookupCached(&e.cacheMu, e.assignmentCache, topicName, version,
		func() uint64 { v, _ := e.assignmentVersion(topicName); return v },
		load,
		nil,
	)
	return set, true, err
}

func (e *Engine) getRoutingMember(id string) (routingMember, error) {
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return routingMember{}, errs.ErrNotFound
	}

	version, versioned := e.routingMembersVersion()
	if !versioned {
		return loadRoutingMember(assignments, id)
	}
	return lookupCached(&e.cacheMu, e.memberCache, id, version,
		func() uint64 { v, _ := e.routingMembersVersion(); return v },
		func() (routingMember, error) { return loadRoutingMember(assignments, id) },
		func(error) bool { return true },
	)
}

func loadRoutingMember(assignments assignmentReader, id string) (routingMember, error) {
	member, err := assignments.GetMember(id)
	if err != nil {
		return routingMember{}, err
	}
	return routingMember{Status: member.Status, Addr: member.Addr}, nil
}

func (e *Engine) topicVersion(name string) (uint64, bool) {
	if versioner, ok := e.metastore.(topicVersioner); ok {
		return versioner.TopicVersion(name), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) assignmentVersion(topicName string) (uint64, bool) {
	if versioner, ok := e.metastore.(assignmentVersioner); ok {
		return versioner.AssignmentVersion(topicName), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) schemaVersion(topicName string) (uint64, bool) {
	if versioner, ok := e.metastore.(schemaVersioner); ok {
		return versioner.SchemaVersion(topicName), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) routingMembersVersion() (uint64, bool) {
	if versioner, ok := e.metastore.(routingMembersVersioner); ok {
		return versioner.RoutingMembersVersion(), true
	}
	return e.globalMetadataVersion()
}

func (e *Engine) globalMetadataVersion() (uint64, bool) {
	versioner, ok := e.metastore.(metadataVersioner)
	if !ok {
		return 0, false
	}
	return versioner.MetadataVersion(), true
}
