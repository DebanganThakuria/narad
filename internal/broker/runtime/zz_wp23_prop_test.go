package runtime

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestZZWP23RandomizedCrashes interleaves acks, ticks, flushes, sync
// errors, Forgets with the shard reloaded, installs of another
// lineage, process crashes with a restart, and ends every run in a
// power loss. Every image of every partition must recover only offsets
// acked in its current lineage, and (on an honest disk) at least the
// newest anchor the committer made durable in that lineage.
func TestZZWP23RandomizedCrashes(t *testing.T) {
	const parts = 3
	seeds := 80
	if testing.Short() {
		seeds = 15
	}
	for seed := range seeds {
		darwin := seed%2 == 1
		interval := time.Second
		if seed%3 == 0 {
			interval = 100 * time.Millisecond
		}
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 2023))
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: parts, interval: interval, darwin: darwin, maxFDs: 2})
			truth := make([]zzWP23Acked, parts)
			floor := make([]int64, parts)
			shards := make([]*zzWP23Shard, parts)
			for p := range parts {
				shards[p] = newZZWP23Shard(-1)
				r.shards.set(p, shards[p])
				truth[p] = zzWP23Acked{frontier: -1, ahead: map[int64]bool{}}
				floor[p] = -1
			}
			ack := func(p int, off int64) {
				shards[p].ack(off)
				if off > truth[p].frontier {
					truth[p].ahead[off] = true
				}
				for truth[p].ahead[truth[p].frontier+1] {
					delete(truth[p].ahead, truth[p].frontier+1)
					truth[p].frontier++
				}
			}
			raise := func() {
				for p := range parts {
					floor[p] = max(floor[p], r.durableFrontier(p))
				}
			}
			for step := range 70 {
				p := rng.IntN(parts)
				switch op := rng.IntN(20); {
				case op < 8:
					ack(p, shards[p].state().frontier+1+rng.Int64N(5))
					r.c.Commit("t", p, shards[p].state().frontier)
				case op < 12:
					_ = r.tick()
				case op < 13:
					_ = r.c.flush()
				case op < 14:
					var once bool
					r.disk.failWriteOut = func(string) error {
						if once {
							return nil
						}
						once = true
						return fmt.Errorf("injected: %w", os.ErrInvalid)
					}
					_ = r.tick()
					r.disk.failWriteOut = nil
				case op < 15:
					r.disk.failFlush = func() error { return fmt.Errorf("injected flush: %w", os.ErrInvalid) }
					_ = r.c.flush()
					r.disk.failFlush = nil
				case op < 16:
					// The shard is dropped and reloaded from disk (the
					// partition moved away and straight back), with a late
					// commit of the dropped one in between.
					old := shards[p]
					r.shards.set(p, nil)
					r.c.Forget("t", p)
					r.c.Commit("t", p, old.state().frontier)
					_ = r.tick()
					shards[p] = zzWP23RecoveredShard(t, r.dir(p))
					r.shards.set(p, shards[p])
				case op < 17:
					// Another lineage is installed (G2 order: Forget before
					// and after the install).
					old := shards[p]
					r.shards.set(p, nil)
					r.c.Forget("t", p)
					at := rng.Int64N(old.state().frontier + 20)
					zzWP23InstallDurable(t, r, p, at)
					r.c.Forget("t", p)
					r.c.Commit("t", p, old.state().frontier)
					_ = r.tick()
					shards[p] = zzWP23RecoveredShard(t, r.dir(p))
					r.shards.set(p, shards[p])
					truth[p] = zzWP23Acked{frontier: at, ahead: map[int64]bool{}}
					floor[p] = at
				case op < 18:
					// Process crash and restart over the same disk.
					raise()
					r.abandon()
					r.c = newConsumerOffsetCommitter(r.dataDir, interval, nil, committerOptions{
						io: r.disk.io(), manual: true, maxFDs: 2,
					})
					r.c.SetAheadSource(r.shards.source)
					for q := range parts {
						shards[q] = zzWP23RecoveredShard(t, r.dir(q))
						r.shards.set(q, shards[q])
					}
				default:
					_ = r.tick()
				}
				raise()
				if step%10 == 9 {
					r.checkAnchors()
				}
			}
			for p := range parts {
				r.checkImages(p, truth[p], floor[p])
			}
		})
	}
}

// zzWP23InstallDurable installs another lineage's copy at frontier
// over partition p, the files durable as a move leaves them (the
// mover syncs what it stages).
func zzWP23InstallDurable(t *testing.T, r *zzWP23Rig, p int, frontier int64) {
	t.Helper()
	r.install(p, frontier)
	r.disk.mu.Lock()
	defer r.disk.mu.Unlock()
	for _, name := range []string{storage.ConsumerAheadFileName, storage.ConsumerOffsetFileName} {
		path := filepath.Join(r.dir(p), name)
		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			delete(r.disk.durable, path)
			delete(r.disk.named, path)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		r.disk.durable[path] = b
		r.disk.named[path] = true
		delete(r.disk.staged, path)
	}
}
