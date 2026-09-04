package metastore

import (
	"maps"
	"sync"
	"sync/atomic"
)

// metadataDomainVersions hands out monotonically increasing versions,
// scoped per metadata domain and per key within a domain, so readers can
// cache and invalidate precisely (e.g. only when a specific topic's
// assignments change).
//
// Each domain has a whole-domain floor (the *All fields, bumped by
// snapshot restores) and a per-key table; a key's effective version is
// the max of the two. All versions are drawn from the shared next
// counter.
//
// Reads are lock-free: the hot paths (produce, consume, routing) read
// several versions per request, and a shared RWMutex around the per-key
// maps was measurable on every one of them. Each per-key table is an
// immutable map from key to a *atomic.Uint64 cell, published through an
// atomic pointer; readers do one pointer load, one map lookup, and one
// cell load. Writers hold mu: bumping a known key stores into its cell in
// place, bumping a new key copies the table (O(keys), paid once per key,
// on topic creation), and bumpAll swaps in empty tables.
type metadataDomainVersions struct {
	// mu serialises writers only. Every bump draws its version from next
	// and publishes it while holding mu, so per-key versions are
	// monotonic even under concurrent bumps and a bumpAll can never be
	// overtaken by a per-key bump that drew a lower version.
	mu   sync.Mutex
	next atomic.Uint64

	topics         keyedVersions
	assignments    keyedVersions
	schemas        keyedVersions
	routingMembers atomic.Uint64
	users          atomic.Uint64
}

// keyedVersions is one domain's versions: the whole-domain floor and the
// immutable per-key table.
type keyedVersions struct {
	all   atomic.Uint64
	table atomic.Pointer[map[string]*atomic.Uint64]
}

func newMetadataDomainVersions() metadataDomainVersions {
	return metadataDomainVersions{}
}

// version returns max(floor, per-key) for key, lock-free.
func (k *keyedVersions) version(key string) uint64 {
	// Load the table before the floor. bumpAll stores the raised floor
	// first and swaps in the empty table second, so a reader that sees
	// the new table is guaranteed to see the raised floor, and one that
	// still sees the old table returns max(old key, floor), which is
	// never below what it returned before.
	var v uint64
	if t := k.table.Load(); t != nil {
		if cell := (*t)[key]; cell != nil {
			v = cell.Load()
		}
	}
	return max(v, k.all.Load())
}

// set publishes version for key. Caller holds metadataDomainVersions.mu.
func (k *keyedVersions) set(key string, version uint64) {
	old := k.table.Load()
	if old != nil {
		if cell := (*old)[key]; cell != nil {
			cell.Store(version)
			return
		}
	}
	size := 1
	if old != nil {
		size += len(*old)
	}
	next := make(map[string]*atomic.Uint64, size)
	if old != nil {
		maps.Copy(next, *old)
	}
	cell := new(atomic.Uint64)
	cell.Store(version)
	next[key] = cell
	k.table.Store(&next)
}

// reset raises the floor to version and drops the per-key table. Caller
// holds metadataDomainVersions.mu.
func (k *keyedVersions) reset(version uint64) {
	k.all.Store(version)
	empty := make(map[string]*atomic.Uint64)
	k.table.Store(&empty)
}

func (v *metadataDomainVersions) bumpTopic(name string) {
	v.bumpKey(&v.topics, name)
}

func (v *metadataDomainVersions) bumpAssignment(topicName string) {
	v.bumpKey(&v.assignments, topicName)
}

func (v *metadataDomainVersions) bumpSchema(topicName string) {
	v.bumpKey(&v.schemas, topicName)
}

func (v *metadataDomainVersions) bumpRoutingMembers() {
	v.mu.Lock()
	v.routingMembers.Store(v.next.Add(1))
	v.mu.Unlock()
}

// bumpUsers advances the whole users domain. Auth caches re-validate
// per user by re-reading the record, so per-key versions are not needed.
func (v *metadataDomainVersions) bumpUsers() {
	v.mu.Lock()
	v.users.Store(v.next.Add(1))
	v.mu.Unlock()
}

// bumpAll advances every domain at once and drops the per-key tables;
// used after a snapshot restore, when any cached read may be stale.
func (v *metadataDomainVersions) bumpAll() {
	v.mu.Lock()
	defer v.mu.Unlock()
	version := v.next.Add(1)
	v.topics.reset(version)
	v.assignments.reset(version)
	v.schemas.reset(version)
	v.routingMembers.Store(version)
	v.users.Store(version)
}

func (v *metadataDomainVersions) topicVersion(name string) uint64 {
	return v.topics.version(name)
}

func (v *metadataDomainVersions) assignmentVersion(topicName string) uint64 {
	return v.assignments.version(topicName)
}

func (v *metadataDomainVersions) schemaVersion(topicName string) uint64 {
	return v.schemas.version(topicName)
}

func (v *metadataDomainVersions) routingMembersVersion() uint64 {
	return v.routingMembers.Load()
}

func (v *metadataDomainVersions) usersVersion() uint64 {
	return v.users.Load()
}

func (v *metadataDomainVersions) bumpKey(domain *keyedVersions, key string) {
	v.mu.Lock()
	domain.set(key, v.next.Add(1))
	v.mu.Unlock()
}
