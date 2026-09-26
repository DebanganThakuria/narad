package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
//
// fence, when non-nil, records the forgets of the cache's keys (it is
// written under mu). A load that overlapped a forget of its own key
// still returns its value but does not cache it, so a request racing a
// topic delete cannot put back an entry the delete just dropped.
func lookupCached[V any](
	mu *sync.RWMutex,
	cache map[string]cached[V],
	fence *forgetFence,
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

		token := fence.begin()
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
		if fence.clean(key, token) {
			cache[key] = cached[V]{value: value, version: version}
		}
		mu.Unlock()
		return value, nil
	}
}

// fenceWindow is how many of the latest forgets a forgetFence remembers.
const fenceWindow = 256

// forgetFence tells a cache load whether a forget of its own key landed
// while it ran. It numbers the forgets and remembers the keys of the
// latest fenceWindow of them; a load notes the count when it begins
// and, before storing, looks for its key among the forgets numbered
// since. So a forget of one topic never fences a load of another: the
// engine-wide counter this replaces fenced every load in flight on any
// forget, which let a churn of deletes of other topics keep a topic's
// schema flight redoing its load, and every produce waiting on it
// blocked, for as long as the churn lasted. Its size is fixed, however
// many names are forgotten.
//
// A load that more than fenceWindow forgets overlapped sees only the
// latest of them and is clean if its key is not among those. Both
// users stay correct when that happens, at the cost of the fence's
// tidiness only: lookupCached stores only a value whose version did not
// move during the load, so the worst it can put back is an entry a
// delete dropped, which the delete's version bump keeps from ever being
// served; and a schema marker stored over a registry that lost the
// topic's schemas meanwhile is reloaded by validateProducePayload.
//
// forget and clean run under the fenced cache's write lock, which
// orders them, so a forget cannot land between a load's check and its
// store; begin is one atomic load.
type forgetFence struct {
	count atomic.Uint64
	// keys[n%fenceWindow] is forget n's key; allocated by the first
	// forget, so an engine that never forgets does not carry it.
	keys *[fenceWindow]string
}

// begin returns the token a load hands to clean. A nil fence fences
// nothing.
func (f *forgetFence) begin() uint64 {
	if f == nil {
		return 0
	}
	return f.count.Load()
}

// clean reports whether no forget of key is known to have landed since
// the load that got token began. The caller holds the cache's write
// lock and stores the load's value under it.
func (f *forgetFence) clean(key string, token uint64) bool {
	if f == nil {
		return true
	}
	n := f.count.Load()
	if n-token > fenceWindow {
		token = n - fenceWindow
	}
	for ; n > token; n-- {
		if f.keys[n%fenceWindow] == key {
			return false
		}
	}
	return true
}

// forget records a forget of key. The caller holds the cache's write
// lock and drops key's entry under it.
func (f *forgetFence) forget(key string) {
	if f.keys == nil {
		f.keys = new([fenceWindow]string)
	}
	n := f.count.Load() + 1
	f.keys[n%fenceWindow] = key
	f.count.Store(n)
}

// ForgetTopic drops every cached view this engine holds of topicName:
// its record, its assignments, its schema load marker and its consume
// cursor. The topic manager calls it when a topic incarnation's local
// state is retired, after dropping the topic's compiled schemas, so a
// deleted topic stops costing memory on every node it was used on. Any
// of it reloads from the metastore on the next use, so forgetting a
// live same-named successor only costs a reload.
func (e *Engine) ForgetTopic(topicName string) {
	e.cacheMu.Lock()
	e.cacheForgets.forget(topicName)
	delete(e.topicCache, topicName)
	delete(e.assignmentCache, topicName)
	delete(e.schemaLoadCache, topicName)
	e.cacheMu.Unlock()
	e.consumeCursors.Delete(topicName)
}

// forgetSchemaLoad drops the topic's schema load marker so the next
// sync reloads the registry.
func (e *Engine) forgetSchemaLoad(topicName string) {
	e.cacheMu.Lock()
	e.cacheForgets.forget(topicName)
	delete(e.schemaLoadCache, topicName)
	e.cacheMu.Unlock()
}

// assignmentSet holds a topic's partition assignments in both list and
// by-partition form so lookups don't rescan the list.
type assignmentSet struct {
	values []metastore.Assignment
	byPart map[int]metastore.Assignment
	// owned is the sorted list of partitions assigned to this node,
	// computed once per cached set so a consume does not allocate and
	// sort it on every request. Read-only for callers.
	owned []int
}

func newAssignmentSet(rows []metastore.Assignment, selfID string) assignmentSet {
	set := assignmentSet{
		values: rows,
		byPart: make(map[int]metastore.Assignment, len(rows)),
	}
	for _, row := range rows {
		set.byPart[row.Partition] = row
	}
	if selfID != "" {
		set.owned = sortPartitions(ownerPartitions(rows, selfID))
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
	return lookupCached(&e.cacheMu, e.topicCache, &e.cacheForgets, name, version,
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

// errNoAssignments is the assignment loader's way of keeping an empty
// row set out of the cache (see assignmentsForTopic); it never reaches
// a caller.
var errNoAssignments = errors.New("messaging: topic has no partition assignments")

// assignmentsForTopic returns the topic's assignments. ok is false
// when the metastore has no assignment support at all.
//
// An empty row set is returned but not cached: it is what a deleted
// topic reads as (ListAssignments of an unknown topic is nil, nil), and
// caching it let every straggling request for a deleted name put back
// the entry its delete had dropped. A live topic has rows from the
// moment its partitions are placed.
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
		if len(rows) == 0 {
			return assignmentSet{}, errNoAssignments
		}
		return newAssignmentSet(rows, e.selfID), nil
	}

	version, versioned := e.assignmentVersion(topicName)
	var (
		set assignmentSet
		err error
	)
	if versioned {
		set, err = lookupCached(&e.cacheMu, e.assignmentCache, &e.cacheForgets, topicName, version,
			func() uint64 { v, _ := e.assignmentVersion(topicName); return v },
			load,
			func(err error) bool { return errors.Is(err, errNoAssignments) },
		)
	} else {
		set, err = load()
	}
	if errors.Is(err, errNoAssignments) {
		return assignmentSet{}, true, nil
	}
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
	return lookupCached(&e.cacheMu, e.memberCache, nil, id, version,
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
