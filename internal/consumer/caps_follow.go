package consumer

// Caps alters on every owner.
//
// A shard resolves its caps once, when it is created. An alter of a
// topic's caps is forwarded to the Raft leader, and only there did
// RefreshCaps reach the live shards; every other owner applied the
// replicated record and kept its shards at the old caps for as long as
// they lived. Raising max_in_flight_per_partition to unstick consumers
// had no effect on those partitions, and lowering it none either.
//
// With a version source wired (SetCapsVersions), InFlight notices that a
// topic's record moved and re-resolves its caps. The check rides on
// every reserve but costs one atomic load and a compare while nothing in
// the metadata changed: the replica's metadata version is compared with
// the one the last pass covered. When it moved, one caller (the others
// go on) compares each topic's version with the version its shards' caps
// were resolved at, and refreshes the topics that moved.
//
// The gate is the metadata version, not the latest domain version: an
// apply publishes its per-topic versions before it advances the metadata
// version, so a pass that read the metadata version first sees every
// topic version that apply moved. The metadata version also moves on
// member heartbeats, so the pass runs every few seconds in a live
// cluster; it costs one version lookup per topic with shards on this
// node.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CapsVersions is the version source InFlight uses to notice caps
// alters; *metastore.Store implements it. TopicVersion must advance
// whenever the named topic's record changes, and MetadataVersion after
// every change, once the topic versions it moved are published.
type CapsVersions interface {
	TopicVersion(topic string) uint64
	MetadataVersion() uint64
}

// capsRetryAfter is how long a pass that could not read a topic's
// record waits before the next attempt, so a failing store is not asked
// on every reserve.
const capsRetryAfter = time.Second

// capsVersion is the topic version a topic's shards' caps were last
// resolved at.
type capsVersion struct {
	version atomic.Uint64
}

// capsFollowState is the InFlight state the caps check keeps; embedded
// in InFlight.
type capsFollowState struct {
	capsVersions CapsVersions
	// capsChecked is the metadata version the last complete pass
	// covered.
	capsChecked atomic.Uint64
	// capsRetryAt holds passes back (unix nanoseconds) after one could
	// not read a record.
	capsRetryAt atomic.Int64
	capsMu      sync.Mutex
	// capsSeen maps a topic name to its *capsVersion.
	capsSeen sync.Map
}

// SetCapsVersions registers the version source that lets live shards
// follow caps alters applied through the local replica. Call once during
// wiring, before serving. Without it caps are resolved at shard creation
// and changed only by RefreshCaps.
func (f *InFlight) SetCapsVersions(v CapsVersions) {
	f.capsVersions = v
}

// checkCaps is the per-reserve check. It is small enough to inline into
// the reserve path, so an InFlight with no version source pays one nil
// check and no call.
func (f *InFlight) checkCaps(ctx context.Context) {
	if f.capsVersions != nil {
		f.checkCapsVersioned(ctx)
	}
}

// checkCapsVersioned is checkCaps with a version source: nothing to do
// while the replica's metadata version is the one the last pass covered.
func (f *InFlight) checkCapsVersioned(ctx context.Context) {
	v := f.capsVersions
	latest := v.MetadataVersion()
	if latest == f.capsChecked.Load() {
		return
	}
	f.followCaps(ctx, v, latest)
}

// followCaps refreshes the caps of every topic whose version moved past
// the one its shards were resolved at, then records latest (a metadata
// version) as covered. One caller runs it at a time; the others carry
// on with the caps they have. latest is read before any topic version,
// so a change that lands during the pass moves the metadata version past
// it and the next reserve runs the pass again.
func (f *InFlight) followCaps(ctx context.Context, v CapsVersions, latest uint64) {
	if time.Now().UnixNano() < f.capsRetryAt.Load() {
		return
	}
	if !f.capsMu.TryLock() {
		return
	}
	defer f.capsMu.Unlock()
	failed := false
	f.capsSeen.Range(func(k, val any) bool {
		topic, seen := k.(string), val.(*capsVersion)
		cur := v.TopicVersion(topic)
		if seen.version.Load() == cur {
			return true
		}
		if err := f.RefreshCaps(ctx, topic); err != nil {
			// Deleted meanwhile (DropTopic forgets it), or a read that
			// failed: tried again after capsRetryAfter.
			failed = true
			return true
		}
		seen.version.Store(cur)
		return true
	})
	if failed {
		f.capsRetryAt.Store(time.Now().Add(capsRetryAfter).UnixNano())
		return
	}
	f.capsChecked.Store(latest)
}

// noteShardCaps records the topic version a new shard's caps were
// resolved at (read before resolving them). A topic's first shard sets
// it. When the topic's shards were already refreshed at a newer version,
// that refresh may have run before this shard was stored, so the topic
// is refreshed again; an older one is caught by the next pass, since the
// metadata version moved past the last one covered.
func (f *InFlight) noteShardCaps(ctx context.Context, topic string, version uint64) {
	if f.capsVersions == nil {
		return
	}
	if cur, ok := f.capsSeen.Load(topic); ok {
		if cur.(*capsVersion).version.Load() > version {
			_ = f.RefreshCaps(ctx, topic)
		}
		return
	}
	seen := &capsVersion{}
	seen.version.Store(version)
	if cur, loaded := f.capsSeen.LoadOrStore(topic, seen); loaded && cur.(*capsVersion).version.Load() > version {
		_ = f.RefreshCaps(ctx, topic)
	}
}

// forgetCaps drops a topic's recorded version with its shards.
func (f *InFlight) forgetCaps(topic string) {
	f.capsSeen.Delete(topic)
}
