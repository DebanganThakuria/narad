// Package faulttest drives the syncfile fault-injection seam from tests:
// fail the Nth matching file operation, fail a named file for good, or
// make every fsync lie (report success, sync nothing) while recording
// the last honest durability point of every file so a test can later
// "pull the plug" and truncate the data directory back to what a real
// disk would have kept.
//
// Only tests import it. The hook is process-global, so a test that
// installs an Injector must not run in parallel with another that does.
package faulttest

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Rule is one injected fault: the Op it applies to, which paths it
// matches, and from which matching call it fires.
type Rule struct {
	op      syncfile.Op
	match   string
	matchFn func(path string) bool
	from    int // 1-based ordinal of the first matching call that fails
	once    bool
	err     error

	seen  int
	fired int
}

// Fired reports how many operations this rule failed.
func (r *Rule) Fired() int { return r.fired }

// Event is one fault the Injector delivered.
type Event struct {
	Op   syncfile.Op
	Path string
	Err  error
	Lie  bool
}

// Snapshot is the last honest durability point of a file: its length at
// the last real fsync and, for single-sector files (the 8-byte
// watermarks, the checkpoint), the exact bytes, since an in-place
// overwrite that was never really synced loses its new value without
// changing the file's length.
type Snapshot struct {
	Len     int64
	Content []byte // nil when the file was larger than snapshotContentMax
}

// snapshotContentMax bounds the files whose content is captured at an
// honest sync. One sector is the atomicity unit the watermark files
// rely on; anything larger is modelled by its length alone.
const snapshotContentMax = 4096

// Injector is a FaultHook with bookkeeping. Zero value is not usable;
// call New.
type Injector struct {
	t testing.TB

	mu     sync.Mutex
	rules  []*Rule
	events []Event
	calls  map[syncfile.Op]int

	// Lying fsync state, all under mu.
	lieRoot      string // paths under it have lying syncs; "" = off
	honestWindow int    // honest syncs per alternation; 0 = never honest
	lyingWindow  int    // lying syncs per alternation
	dirsHonest   bool   // directory fsyncs never lie (journaled metadata)
	syncCalls    int    // syncs seen while lying
	honest       map[string]Snapshot
	dirs         map[string]map[string]bool // dir -> names durable at its last honest sync
	lies         atomic.Int64
	honestSyncs  atomic.Int64
}

// New installs an Injector as the process-wide fault hook and restores
// the previous hook when the test ends.
func New(t testing.TB) *Injector {
	t.Helper()
	inj := &Injector{
		t:      t,
		calls:  make(map[syncfile.Op]int),
		honest: make(map[string]Snapshot),
		dirs:   make(map[string]map[string]bool),
	}
	restore := syncfile.SetFaultHook(inj.hook)
	t.Cleanup(restore)
	return inj
}

// FailNth makes the n-th (1-based) call of op on a path containing
// match fail with err, once. An empty match matches every path.
func (inj *Injector) FailNth(op syncfile.Op, match string, n int, err error) *Rule {
	return inj.addRule(&Rule{op: op, match: match, from: n, once: true, err: err})
}

// FailFrom makes every call of op on a matching path fail with err from
// the n-th matching call on: a disk that stays broken.
func (inj *Injector) FailFrom(op syncfile.Op, match string, n int, err error) *Rule {
	return inj.addRule(&Rule{op: op, match: match, from: n, err: err})
}

// FailNthFunc is FailNth with a path predicate instead of a substring.
func (inj *Injector) FailNthFunc(op syncfile.Op, match func(path string) bool, n int, err error) *Rule {
	return inj.addRule(&Rule{op: op, matchFn: match, from: n, once: true, err: err})
}

// Remove retires a rule.
func (inj *Injector) Remove(r *Rule) {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	for i, cur := range inj.rules {
		if cur == r {
			inj.rules = append(inj.rules[:i], inj.rules[i+1:]...)
			return
		}
	}
}

func (inj *Injector) addRule(r *Rule) *Rule {
	if r.from <= 0 {
		r.from = 1
	}
	inj.mu.Lock()
	inj.rules = append(inj.rules, r)
	inj.mu.Unlock()
	return r
}

// Calls reports how many operations of op the hook has seen.
func (inj *Injector) Calls(op syncfile.Op) int {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	return inj.calls[op]
}

// Events returns a copy of the faults delivered so far.
func (inj *Injector) Events() []Event {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	return append([]Event(nil), inj.events...)
}

// LieSyncs makes every SyncData and Sync on a path under root report
// success without syncing, in alternating windows: honest syncs are
// real, then lying syncs lie, and so on (honest 0: every sync lies).
// The windows let a multi-sync operation (a commit syncs its segment and
// then the high-watermark) land entirely inside an honest stretch, so a
// test can classify it. With dirsHonest set, directory fsyncs are always real: the
// model of a drive that drops data writes but a file system whose
// metadata journal is honest, which is what lets a test decide per
// record whether its own syncs were honest. The current state of every
// file and directory under root is recorded as durable (the disk was
// honest until now), and every honest sync afterwards records the
// file's length (and single-sector content) as its new durability
// point; Crash later restores exactly those points.
func (inj *Injector) LieSyncs(root string, honest, lying int, dirsHonest bool) {
	inj.t.Helper()
	root = filepath.Clean(root)
	inj.mu.Lock()
	defer inj.mu.Unlock()
	inj.lieRoot = root
	inj.honestWindow = honest
	inj.lyingWindow = max(lying, 1)
	inj.dirsHonest = dirsHonest
	inj.syncCalls = 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			inj.recordDirLocked(path)
			return nil
		}
		if d.Type().IsRegular() {
			inj.recordFileLocked(path)
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		inj.t.Fatalf("faulttest: baseline %s: %v", root, err)
	}
}

// HonestSyncs reports the current honest-lengths mode: the syncs that
// really ran while lying was on.
func (inj *Injector) HonestSyncs() int64 { return inj.honestSyncs.Load() }

// Lies reports how many syncs have lied so far. A test compares the
// count before and after an operation to learn whether that operation
// was fully honest.
func (inj *Injector) Lies() int64 { return inj.lies.Load() }

// Honest returns the recorded durability point of path.
func (inj *Injector) Honest(path string) (Snapshot, bool) {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	s, ok := inj.honest[filepath.Clean(path)]
	return s, ok
}

// StopLying turns lying off (later syncs are real) without touching the
// recorded durability points, so a test can Close a log honestly after
// a lying run when it wants a clean shutdown rather than a crash.
func (inj *Injector) StopLying() {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	inj.lieRoot = ""
}

// Crash rewrites the files under root to the state a power loss would
// have left: every file goes back to its last honest durability point
// (its length, or its exact bytes for a single-sector file), a file
// whose name was made durable by a directory sync but whose data never
// was becomes empty, and a file that no honest directory sync ever
// listed disappears. Files outside root are untouched. Call it after
// the writers are stopped.
func (inj *Injector) Crash(root string) error {
	root = filepath.Clean(root)
	inj.mu.Lock()
	defer inj.mu.Unlock()
	var remove []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		if snap, ok := inj.honest[path]; ok {
			return restoreSnapshot(path, snap)
		}
		dir, name := filepath.Split(path)
		if names := inj.dirs[filepath.Clean(dir)]; names[name] {
			return os.Truncate(path, 0)
		}
		remove = append(remove, path)
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range remove {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func restoreSnapshot(path string, snap Snapshot) error {
	if snap.Content != nil {
		return os.WriteFile(path, snap.Content, 0o600)
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > snap.Len {
		return os.Truncate(path, snap.Len)
	}
	return nil
}

// hook is the installed syncfile.FaultHook.
func (inj *Injector) hook(op syncfile.Op, path string) error {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	inj.calls[op]++
	for _, r := range inj.rules {
		if r.op != op || (r.match != "" && !strings.Contains(path, r.match)) || (r.matchFn != nil && !r.matchFn(path)) {
			continue
		}
		r.seen++
		if r.seen < r.from || (r.once && r.fired > 0) {
			continue
		}
		r.fired++
		inj.events = append(inj.events, Event{Op: op, Path: path, Err: r.err})
		return r.err
	}
	if inj.lieRoot == "" || (op != syncfile.OpSyncData && op != syncfile.OpSync) {
		return nil
	}
	clean := filepath.Clean(path)
	if clean != inj.lieRoot && !strings.HasPrefix(clean, inj.lieRoot+string(filepath.Separator)) {
		return nil
	}
	isDir := false
	if info, err := os.Stat(clean); err == nil && info.IsDir() {
		isDir = true
	}
	if isDir && inj.dirsHonest {
		inj.recordDirLocked(clean)
		return nil
	}
	phase := inj.syncCalls % (inj.honestWindow + inj.lyingWindow)
	inj.syncCalls++
	if phase < inj.honestWindow {
		inj.honestSyncs.Add(1)
		if isDir {
			inj.recordDirLocked(clean)
		} else {
			inj.recordFileLocked(clean)
		}
		return nil
	}
	inj.lies.Add(1)
	inj.events = append(inj.events, Event{Op: op, Path: path, Lie: true})
	return syncfile.ErrLie
}

func (inj *Injector) recordFileLocked(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	snap := Snapshot{Len: info.Size()}
	if info.Size() <= snapshotContentMax {
		if content, err := os.ReadFile(path); err == nil {
			snap.Content = content
		}
	}
	inj.honest[path] = snap
}

func (inj *Injector) recordDirLocked(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	names := make(map[string]bool, len(entries))
	for _, e := range entries {
		names[e.Name()] = true
	}
	inj.dirs[dir] = names
}

// Torn-tail helpers: the shapes a crashed write leaves behind.

// TruncateTail cuts n bytes off the end of path (a write that never
// completed).
func TruncateTail(path string, n int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.Truncate(path, max(info.Size()-n, 0))
}

// AppendGarbage appends n pseudo-random bytes to path (a sector the
// drive wrote with stale or scrambled content). The bytes may well
// contain a frame magic, which is the point.
func AppendGarbage(path string, n int, seed uint64) error {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	garbage := make([]byte, n)
	for i := range garbage {
		garbage[i] = byte(rng.IntN(256))
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(garbage)
	return err
}

// ZeroTail overwrites the last n bytes of path with zeros (the classic
// ext4/XFS "size updated, data not" tail after a crash).
func ZeroTail(path string, n int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if n > info.Size() {
		n = info.Size()
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(make([]byte, n), info.Size()-n)
	return err
}

// Describe renders the delivered faults for a failure message.
func Describe(events []Event) string {
	var b strings.Builder
	for _, e := range events {
		if e.Lie {
			continue
		}
		fmt.Fprintf(&b, "%s %s -> %v\n", e.Op, e.Path, e.Err)
	}
	return b.String()
}
