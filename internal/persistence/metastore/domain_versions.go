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
// Each keyed domain publishes an immutable table: a cell per key it has
// seen, and a floor, the version every key without a cell reads. A
// snapshot restore swaps in an empty table whose floor is above every
// version handed out so far. All versions are drawn from the shared next
// counter.
//
// Reads are lock-free: the hot paths (produce, consume, routing) read
// several versions per request, and a shared RWMutex around the per-key
// maps was measurable on every one of them. The table is published
// through an atomic pointer; readers do one pointer load, one map
// lookup, and one cell load. Writers hold mu: bumping a known key stores
// into its cell in place, bumping a new key copies the table (O(keys),
// paid once per key, on topic creation), and bumpAll swaps in empty
// tables.
//
// retireTopic, which the FSM's topic delete calls in place of bumping
// the three domains, keeps a deleted topic's cells as tombstones and
// prunes them maxRetiredKeys at a time, so the tables track live names
// rather than every name ever used.
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

// maxRetiredKeys is how many deleted names a domain keeps as tombstones
// before pruning them. A prune raises the floor, which moves the version
// of every key without a cell (never seen, or not bumped since the last
// snapshot restore) and so makes their cached reads reload once; doing
// it in batches keeps that to once per this many topic deletes while
// bounding what unique-name churn can leave behind.
const maxRetiredKeys = 1024

// keyedVersions is one domain's versions.
type keyedVersions struct {
	table atomic.Pointer[versionTable]
	// retired holds the keys whose cells are tombstones of deleted
	// names, awaiting a prune. Guarded by metadataDomainVersions.mu.
	retired map[string]struct{}
}

// versionTable is one immutable snapshot of a domain's per-key versions.
// The map is never written after publication; the cells are, in place.
type versionTable struct {
	cells map[string]*atomic.Uint64
	// floor is the version of every key without a cell.
	floor uint64
}

func newMetadataDomainVersions() metadataDomainVersions {
	return metadataDomainVersions{}
}

// version returns key's version, lock-free.
//
// A key's reads never go backwards. Tables are published in order under
// the writers' lock, and each read uses one table: a cell only ever
// stores newer versions; a cell that replaces a pruned or reset one
// starts from a version drawn after the old cell's last store; a table's
// floor is copied forward or raised, never lowered; and a key that gains
// a cell gets a version drawn after the floor it read before. So a
// cached (value, version) pair can only look current while the key has
// not changed.
func (k *keyedVersions) version(key string) uint64 {
	t := k.table.Load()
	if t == nil {
		return 0
	}
	if cell := t.cells[key]; cell != nil {
		return cell.Load()
	}
	return t.floor
}

// set publishes version for key. Caller holds metadataDomainVersions.mu.
func (k *keyedVersions) set(key string, version uint64) {
	old := k.table.Load()
	if old != nil {
		if cell := old.cells[key]; cell != nil {
			cell.Store(version)
			// A bump means the name is in use again (a recreate).
			delete(k.retired, key)
			return
		}
	}
	next := &versionTable{}
	size := 1
	if old != nil {
		size += len(old.cells)
		next.floor = old.floor
	}
	next.cells = make(map[string]*atomic.Uint64, size)
	if old != nil {
		maps.Copy(next.cells, old.cells)
	}
	cell := new(atomic.Uint64)
	cell.Store(version)
	next.cells[key] = cell
	k.table.Store(next)
}

// retire publishes version for a deleted key and marks its cell a
// tombstone. When maxRetiredKeys tombstones have piled up, they go in
// one step: a table without them whose floor, drawn from next, is newer
// than any cell, so each pruned name still reads a version above
// everything it read before. Caller holds metadataDomainVersions.mu.
func (k *keyedVersions) retire(key string, version uint64, next *atomic.Uint64) {
	k.set(key, version)
	if k.retired == nil {
		k.retired = make(map[string]struct{})
	}
	k.retired[key] = struct{}{}
	if len(k.retired) < maxRetiredKeys {
		return
	}
	old := k.table.Load()
	pruned := &versionTable{
		cells: make(map[string]*atomic.Uint64, len(old.cells)-len(k.retired)),
		floor: next.Add(1),
	}
	for name, cell := range old.cells {
		if _, gone := k.retired[name]; !gone {
			pruned.cells[name] = cell
		}
	}
	k.table.Store(pruned)
	clear(k.retired)
}

// reset drops every cell and raises the floor to version. Caller holds
// metadataDomainVersions.mu.
func (k *keyedVersions) reset(version uint64) {
	k.table.Store(&versionTable{cells: map[string]*atomic.Uint64{}, floor: version})
	clear(k.retired)
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

// latest returns the newest version handed out to any domain. Every
// bump, retire, prune and bumpAll draws from next, so it moves whenever
// any domain's version does and at no other time.
func (v *metadataDomainVersions) latest() uint64 {
	return v.next.Load()
}

// retireTopic advances a deleted topic's topic, assignment and schema
// versions, exactly as bumping all three would, and marks its cells as
// tombstones so that a churn of uniquely named topics does not keep a
// cell per name ever created (see keyedVersions.retire). applyDeleteTopic
// calls it for the deleted name; the fan-out partners that delete also
// bumps stay live and keep bumpTopic.
func (v *metadataDomainVersions) retireTopic(name string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.topics.retire(name, v.next.Add(1), &v.next)
	v.assignments.retire(name, v.next.Add(1), &v.next)
	v.schemas.retire(name, v.next.Add(1), &v.next)
}

// retireAssignments advances a deleted topic's assignment version and
// keeps its cell a tombstone (see retireTopic): a prune of a row left
// behind under a deleted name must not bring the name's cell back.
func (v *metadataDomainVersions) retireAssignments(name string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.assignments.retire(name, v.next.Add(1), &v.next)
}

func (v *metadataDomainVersions) bumpKey(domain *keyedVersions, key string) {
	v.mu.Lock()
	domain.set(key, v.next.Add(1))
	v.mu.Unlock()
}
