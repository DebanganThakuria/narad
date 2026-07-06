package metastore

import (
	"sync"
	"sync/atomic"
)

// metadataDomainVersions hands out monotonically increasing versions,
// scoped per metadata domain and per key within a domain, so readers can
// cache and invalidate precisely (e.g. only when a specific topic's
// assignments change).
//
// Each domain has a whole-domain floor (the *All fields, bumped by
// snapshot restores) and a per-key map; a key's effective version is the
// max of the two. All versions are drawn from the shared next counter,
// and mu guards the per-key maps.
type metadataDomainVersions struct {
	next atomic.Uint64

	topicsAll      atomic.Uint64
	assignmentsAll atomic.Uint64
	schemasAll     atomic.Uint64
	routingMembers atomic.Uint64
	users          atomic.Uint64

	mu          sync.RWMutex
	topics      map[string]uint64
	assignments map[string]uint64
	schemas     map[string]uint64
}

func newMetadataDomainVersions() metadataDomainVersions {
	return metadataDomainVersions{
		topics:      make(map[string]uint64),
		assignments: make(map[string]uint64),
		schemas:     make(map[string]uint64),
	}
}

func (v *metadataDomainVersions) bumpTopic(name string) {
	v.bumpKey(v.topics, name)
}

func (v *metadataDomainVersions) bumpAssignment(topicName string) {
	v.bumpKey(v.assignments, topicName)
}

func (v *metadataDomainVersions) bumpSchema(topicName string) {
	v.bumpKey(v.schemas, topicName)
}

func (v *metadataDomainVersions) bumpRoutingMembers() {
	v.routingMembers.Store(v.next.Add(1))
}

// bumpUsers advances the whole users domain. Auth caches re-validate
// per user by re-reading the record, so per-key versions are not needed.
func (v *metadataDomainVersions) bumpUsers() {
	v.users.Store(v.next.Add(1))
}

// bumpAll advances every domain at once and clears the per-key maps;
// used after a snapshot restore, when any cached read may be stale.
func (v *metadataDomainVersions) bumpAll() {
	version := v.next.Add(1)
	v.topicsAll.Store(version)
	v.assignmentsAll.Store(version)
	v.schemasAll.Store(version)
	v.routingMembers.Store(version)
	v.users.Store(version)

	v.mu.Lock()
	clear(v.topics)
	clear(v.assignments)
	clear(v.schemas)
	v.mu.Unlock()
}

func (v *metadataDomainVersions) topicVersion(name string) uint64 {
	return v.keyVersion(v.topicsAll.Load(), v.topics, name)
}

func (v *metadataDomainVersions) assignmentVersion(topicName string) uint64 {
	return v.keyVersion(v.assignmentsAll.Load(), v.assignments, topicName)
}

func (v *metadataDomainVersions) schemaVersion(topicName string) uint64 {
	return v.keyVersion(v.schemasAll.Load(), v.schemas, topicName)
}

func (v *metadataDomainVersions) routingMembersVersion() uint64 {
	return v.routingMembers.Load()
}

func (v *metadataDomainVersions) usersVersion() uint64 {
	return v.users.Load()
}

func (v *metadataDomainVersions) bumpKey(values map[string]uint64, key string) {
	version := v.next.Add(1)
	v.mu.Lock()
	values[key] = version
	v.mu.Unlock()
}

func (v *metadataDomainVersions) keyVersion(all uint64, values map[string]uint64, key string) uint64 {
	v.mu.RLock()
	version := values[key]
	v.mu.RUnlock()
	if version > all {
		return version
	}
	return all
}
