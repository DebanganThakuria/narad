package metastore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// mountSmallVolume creates and mounts a fresh APFS disk image of the
// given size and returns its mount point; the image is detached when
// the test ends. It skips the test where hdiutil is unavailable.
func mountSmallVolume(t *testing.T, size string) string {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("needs hdiutil to create a small disk image")
	}
	hdiutil, err := exec.LookPath("hdiutil")
	if err != nil {
		t.Skip("hdiutil not found")
	}
	dir := t.TempDir()
	image := filepath.Join(dir, "volume.dmg")
	mount := filepath.Join(dir, "mnt")
	if out, err := exec.Command(hdiutil, "create", "-size", size, "-fs", "APFS", "-volname", "narad-metastore", "-quiet", image).CombinedOutput(); err != nil {
		t.Skipf("hdiutil create: %v: %s", err, out)
	}
	if out, err := exec.Command(hdiutil, "attach", "-nobrowse", "-noautoopen", "-mountpoint", mount, image).CombinedOutput(); err != nil {
		t.Skipf("hdiutil attach: %v: %s", err, out)
	}
	t.Cleanup(func() {
		for attempt := range 5 {
			args := []string{"detach", mount}
			if attempt > 1 {
				args = append(args, "-force")
			}
			out, err := exec.Command(hdiutil, args...).CombinedOutput()
			if err == nil {
				return
			}
			if attempt == 4 {
				t.Errorf("hdiutil detach %s: %v: %s", mount, err, out)
			}
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	})
	return mount
}

// fillVolume writes a filler file into dir until the volume refuses
// more, and returns its path.
func fillVolume(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "filler")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, chunk := range []int{1 << 20, 64 << 10, 4 << 10, 512} {
		buf := make([]byte, chunk)
		for {
			if _, err := f.Write(buf); err != nil {
				break
			}
		}
	}
	_ = f.Sync()
	return path
}

// A follower whose metastore volume is full cannot write a committed
// schema. It must stop applying, leave Raft and never report caught up
// or ready, rather than count the entry as applied and serve a replica
// without it; once space is freed, a restart applies the entry.
//
// Only the follower's fsm.db is on the small volume (a symlink into it),
// so its Raft log keeps accepting the entry and the failure lands where
// it does in production: in the metadata database's commit.
func TestFullDiskFollowerStopsInsteadOfSkippingAnEntry(t *testing.T) {
	mount := mountSmallVolume(t, "16m")
	shortApplyRetries(t)
	ctx := context.Background()

	base := t.TempDir()
	ids := []string{"disk-1", "disk-2", "disk-3"}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	cfgs := make([]Config, len(ids))
	stores := make([]*Store, len(ids))
	t.Cleanup(func() {
		for _, s := range stores {
			if s != nil {
				_ = s.Close()
			}
		}
	})
	const victim = 2
	for i := range ids {
		var peers []Peer
		for j := range ids {
			if i != j {
				peers = append(peers, Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		cfgs[i] = Config{NodeID: ids[i], DataDir: filepath.Join(base, ids[i]), BindAddr: addrs[i], AdvertiseAddr: addrs[i], Peers: peers}
		if i == victim {
			if err := os.MkdirAll(cfgs[i].DataDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(mount, "fsm.db"), filepath.Join(cfgs[i].DataDir, "fsm.db")); err != nil {
				t.Fatal(err)
			}
		}
		s, err := New(cfgs[i])
		if err != nil {
			t.Fatalf("New(%s): %v", ids[i], err)
		}
		stores[i] = s
	}
	leaderOf := func() *Store {
		var leader *Store
		waitUntil(t, 15*time.Second, "a caught-up leader", func() bool {
			for _, s := range stores {
				if s != nil && s.IsLeader() && s.AppliedCaughtUp() {
					leader = s
					return true
				}
			}
			return false
		})
		return leader
	}
	leader := leaderOf()
	if leader == stores[victim] {
		if err := leader.TransferLeadership(); err != nil {
			t.Fatalf("transfer leadership off the victim: %v", err)
		}
		waitUntil(t, 10*time.Second, "leader moved off the victim", func() bool { return !stores[victim].IsLeader() })
		leader = leaderOf()
	}
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, "victim caught up", func() bool { return stores[victim].AppliedIndex() >= leader.AppliedIndex() })

	filler := fillVolume(t, mount)
	schema := []byte(`{"type":"object","description":"` + strings.Repeat("x", 512<<10) + `"}`)
	if err := leader.PutSchema(ctx, "orders", 1, schema); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	committed := leader.AppliedIndex()

	s := stores[victim]
	deadline := time.Now().Add(10 * time.Second)
	for halted := false; !halted; {
		select {
		case <-s.Halted():
			halted = true
			continue
		case <-time.After(10 * time.Millisecond):
		}
		if s.fsm.applied.Load() >= committed {
			if _, err := s.GetSchema(ctx, "orders", 1); errors.Is(err, ErrNotFound) {
				t.Fatalf("the follower reports applied index %d (the schema committed at %d) but lacks the schema: the failed write was consumed",
					s.fsm.applied.Load(), committed)
			}
			t.Skip("the follower wrote the schema on the filled volume; the volume did not fill, inconclusive")
		}
		if time.Now().After(deadline) {
			t.Fatalf("the follower neither stopped nor applied the schema within 10s (applied %d, committed %d)", s.fsm.applied.Load(), committed)
		}
	}
	if !strings.Contains(s.HaltErr().Error(), "could not write raft entry") {
		t.Fatalf("stop error %q does not say the write failed", s.HaltErr())
	}
	for range 20 {
		if s.AppliedCaughtUp() || s.ClusterReady() == nil {
			t.Fatal("a follower that stopped applying reports caught up or ready")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Free space and restart: the replay applies the entry it stopped on.
	if err := s.Close(); err != nil {
		t.Logf("close the stopped follower: %v", err)
	}
	stores[victim] = nil
	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(cfgs[victim])
	if err != nil {
		t.Fatalf("restart the follower: %v", err)
	}
	stores[victim] = restarted
	waitUntil(t, 20*time.Second, "restarted follower caught up", func() bool {
		return restarted.AppliedIndex() >= committed && restarted.AppliedCaughtUp()
	})
	got, err := restarted.GetSchema(ctx, "orders", 1)
	if err != nil || len(got) != len(schema) {
		t.Fatalf("restarted follower's schema: %d bytes, %v; want the %d bytes the leader committed", len(got), err, len(schema))
	}
	if _, err := os.Stat(filepath.Join(cfgs[victim].DataDir, "fsm.db.stale")); err == nil {
		t.Fatalf("the restart set fsm.db aside in %s; its index should have been trusted", cfgs[victim].DataDir)
	}
}
