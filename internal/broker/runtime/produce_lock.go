package runtime

import (
	"sync"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Produce serialization: every produce commit for a (topic, partition)
// runs under a keyed mutex so append + fsync + high-watermark advance
// is a single atomic section. The mutexes are minted lazily and
// retired on topic close so topic churn doesn't grow the map forever.

// WithProduceLock runs fn on the (topic, partition) log while holding
// its produce-serialization mutex.
func (g *Logs) WithProduceLock(topicName string, idx int, fn func(*storage.Log) error) error {
	mu := g.lockProduce(topicName, idx)
	defer mu.Unlock()

	log, err := g.Get(topicName, idx)
	if err != nil {
		return err
	}
	return fn(log)
}

// WithProduceLockIncarnation is WithProduceLock for a caller that must
// know which incarnation of the topic the log belongs to: fn also gets
// the topic ID the log was opened under ("" for a topic record without
// one, or when the Logs has no metastore). The log map re-checks an
// open log against the topic record only on a Get, so a caller that
// commits records which arrived after this Get pairs it with Current.
func (g *Logs) WithProduceLockIncarnation(topicName string, idx int, fn func(log *storage.Log, incarnation string) error) error {
	mu := g.lockProduce(topicName, idx)
	defer mu.Unlock()

	log, incarnation, err := g.get(topicName, idx)
	if err != nil {
		return err
	}
	return fn(log, incarnation)
}

// ProduceSyncCount reports the number of live produce-serialization
// mutexes. Used by tests to assert topic churn doesn't leak entries.
func (g *Logs) ProduceSyncCount() int {
	g.produceMu.Lock()
	defer g.produceMu.Unlock()
	return len(g.produceSync)
}

// lockProduce acquires the produce-serialization mutex for (topic,
// partition), minting one on first use, and returns it locked; the
// caller unlocks it. Returning the mutex rather than an unlock func
// keeps the per-commit-batch call free of allocations.
//
// Keyed-mutex revalidation: CloseTopic/CloseAll retire map entries,
// and a goroutine may have fetched a mutex from the map
// just before its entry was retired. If it then acquired that orphaned
// mutex while a later producer minted a fresh one for the same key, two
// produce commits could run concurrently. So after acquiring, re-check
// that the acquired mutex is still the one installed in the map; if
// not, release it and retry against the current map state. Retirement
// itself only deletes an entry while holding that entry's mutex (see
// retireProduceMutex), so a holder inside its critical section is never
// invalidated mid-flight.
func (g *Logs) lockProduce(topicName string, idx int) *sync.Mutex {
	key := keyOf(topicName, idx)
	for {
		g.produceMu.Lock()
		mu, ok := g.produceSync[key]
		if !ok {
			mu = &sync.Mutex{}
			g.produceSync[key] = mu
		}
		g.produceMu.Unlock()

		mu.Lock()

		g.produceMu.Lock()
		current := g.produceSync[key] == mu
		g.produceMu.Unlock()
		if current {
			return mu
		}
		// The entry was retired (and possibly replaced) between our map
		// fetch and the acquisition — this mutex no longer serializes
		// anything. Drop it and retry.
		mu.Unlock()
	}
}

// retireProduceMutex removes a produceSync entry, but only while
// holding that entry's mutex: a produce commit currently inside its
// critical section blocks the retirement until it finishes, so a
// producer that mints a replacement mutex afterwards can never run
// concurrently with the old holder. Goroutines still queued on the
// retired mutex fail lockProduce's revalidation and retry. The map
// re-check under produceMu makes retirement idempotent against a
// concurrent retire of the same key.
func (g *Logs) retireProduceMutex(key logKey, mu *sync.Mutex) {
	mu.Lock()
	g.produceMu.Lock()
	if g.produceSync[key] == mu {
		delete(g.produceSync, key)
	}
	g.produceMu.Unlock()
	mu.Unlock()
}

// retireProduceEntries retires every produceSync entry whose key
// matches. The map is snapshotted under produceMu, then each entry is
// retired individually under its own mutex.
func (g *Logs) retireProduceEntries(match func(key logKey) bool) {
	g.produceMu.Lock()
	retire := make(map[logKey]*sync.Mutex)
	for k, mu := range g.produceSync {
		if match(k) {
			retire[k] = mu
		}
	}
	g.produceMu.Unlock()

	for k, mu := range retire {
		g.retireProduceMutex(k, mu)
	}
}
