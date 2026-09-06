//go:build cluster

package cluster

import (
	"testing"
	"time"
)

// Compaction settings that make snapshot restore routine: a snapshot
// every 64 applied entries (checked every 100-200ms) with 32 entries
// kept behind it. A node that misses more than 32 entries can only
// rejoin by installing the leader's snapshot.
const (
	snapshotThreshold = 64
	trailingLogs      = 32
)

func snapshotEnv() map[string]string {
	return map[string]string{
		"NARAD_CLUSTER_RAFT_SNAPSHOT_THRESHOLD": "64",
		"NARAD_CLUSTER_RAFT_SNAPSHOT_INTERVAL":  "100ms",
		"NARAD_CLUSTER_RAFT_TRAILING_LOGS":      "32",
	}
}

type restoreScenario struct {
	name string
	// killLeader kills the Raft leader instead of a follower.
	killLeader bool
	// duringSnapshot times the kill to the leader writing a snapshot.
	duringSnapshot bool
	// killAfterInstall kills the restored node a second time the moment
	// it logs the snapshot install, before it turns ready; otherwise the
	// second kill lands right after /readyz flips true.
	killAfterInstall bool
}

// TestSnapshotRestoreUnderLoad kills a node under load, churns the
// metadata far past the trailing log on the survivors, restarts it and
// asserts it comes back by snapshot install with the same metadata as
// the leader, owning and serving its partitions, with the driver
// reporting every message acked; then restarts it once more right away.
func TestSnapshotRestoreUnderLoad(t *testing.T) {
	scenarios := []restoreScenario{
		{name: "follower-kill"},
		{name: "leader-kill", killLeader: true},
		{name: "follower-kill-during-leader-snapshot", duringSnapshot: true, killAfterInstall: true},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) { runRestoreScenario(t, sc) })
	}
}

func runRestoreScenario(t *testing.T, sc restoreScenario) {
	c := newCluster(t, clusterOptions{env: snapshotEnv()})
	c.startAll()
	c.waitAllReady(90 * time.Second)
	c.waitAdmin(30 * time.Second)

	// About 60s of paced load; the driver keeps producing and consuming
	// through every kill and restart below and only passes if every
	// accepted message is acked in its claim table.
	d := c.startDriver(driverOptions{topics: 3, partitions: 6, messages: 9000, rate: 150, timeout: 5 * time.Minute})
	c.waitTopics(d, 6, 60*time.Second)
	time.Sleep(3 * time.Second)

	leader := c.waitLeader(20 * time.Second)
	victim := leader
	if !sc.killLeader {
		victim = c.anyRunning(leader)
	}
	survivor := c.anyRunning(victim)
	owned := c.ownedPartitions(survivor, d.topics)
	if owned[c.nodes[victim].id] == 0 {
		t.Fatalf("%s owns none of the driver's partitions: %v", c.nodes[victim].id, owned)
	}
	t.Logf("leader=%s victim=%s (owns %d of the driver's %d partitions)", c.nodes[leader].id, c.nodes[victim].id, owned[c.nodes[victim].id], 3*6)

	if sc.duringSnapshot {
		killDuringLeaderSnapshot(t, c, leader, victim, survivor)
	} else {
		c.kill(victim)
	}

	lastIdx, err := lastLogIndex(c.nodes[victim].dataDir)
	if err != nil {
		t.Fatalf("read %s raft log: %v", c.nodes[victim].id, err)
	}
	t.Logf("%s log ended at index %d", c.nodes[victim].id, lastIdx)

	// Churn on the survivors until both have compacted well past the
	// victim's last index: whichever of them leads can then no longer
	// replicate by log and must install a snapshot.
	writes := 0
	for round := 0; ; round++ {
		writes += c.churn(survivor, 8)
		compacted := true
		for i := range c.nodes {
			if i == victim {
				continue
			}
			snap, ok := newestSnapshot(c.nodes[i].dataDir)
			if !ok || snap.Index < lastIdx+3*trailingLogs {
				compacted = false
			}
		}
		if compacted {
			break
		}
		if round > 30 {
			t.Fatalf("survivors never compacted past index %d after %d writes", lastIdx+3*trailingLogs, writes)
		}
	}
	for i := range c.nodes {
		if i != victim {
			snap, _ := newestSnapshot(c.nodes[i].dataDir)
			t.Logf("%s newest snapshot index %d after %d churn writes", c.nodes[i].id, snap.Index, writes)
		}
	}

	off := c.logOffset(victim)
	c.start(victim)
	installIn := c.waitLog(victim, off, "Installed remote snapshot", 120*time.Second)
	t.Logf("%s installed a remote snapshot %s after restart", c.nodes[victim].id, installIn.Round(time.Millisecond))

	if sc.killAfterInstall {
		// The restore just replaced the FSM; a node that dies now and
		// comes back must not latch ownership on that view before the
		// leader has confirmed it is current.
		ready, body := c.readyNow(victim)
		t.Logf("%s readiness right after install: ready=%v (%s)", c.nodes[victim].id, ready, body)
		c.kill(victim)
		off = c.logOffset(victim)
		c.start(victim)
	}
	c.waitReady(victim, 120*time.Second)
	assertRestoredFromSnapshot(t, c, victim, lastIdx)

	leaderNow := c.waitLeader(20 * time.Second)
	c.waitConverged(leaderNow, 60*time.Second)
	if n := c.memberOwned(victim, c.nodes[victim].id); n == 0 {
		t.Fatalf("%s owns no partitions after restore", c.nodes[victim].id)
	}
	c.assertServesOwnPartitions(victim)

	if !sc.killAfterInstall {
		// Second restart immediately after the restore.
		c.kill(victim)
		c.start(victim)
		c.waitReady(victim, 120*time.Second)
		leaderNow = c.waitLeader(20 * time.Second)
		c.waitConverged(leaderNow, 60*time.Second)
		c.assertServesOwnPartitions(victim)
	}

	d.wait(t)
	c.waitConverged(c.waitLeader(20*time.Second), 60*time.Second)
}

// killDuringLeaderSnapshot churns metadata in the background so the
// leader keeps snapshotting, watches the leader's snapshot directory, and
// kills victim the instant a snapshot is being written (a "<id>.tmp"
// directory exists) or, failing to catch that window, the instant a new
// snapshot lands (the leader is then compacting its log).
func killDuringLeaderSnapshot(t *testing.T, c *cluster, leader, victim, survivor int) {
	t.Helper()
	stop := make(chan struct{})
	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		for {
			select {
			case <-stop:
				return
			default:
				c.churn(survivor, 1)
			}
		}
	}()
	defer func() {
		close(stop)
		<-churnDone
	}()

	leaderDir := c.nodes[leader].dataDir
	before, _ := newestSnapshot(leaderDir)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if snapshotInProgress(leaderDir) {
			c.kill(victim)
			t.Logf("%s killed while %s was writing a snapshot", c.nodes[victim].id, c.nodes[leader].id)
			return
		}
		if now, ok := newestSnapshot(leaderDir); ok && now.Index != before.Index {
			c.kill(victim)
			t.Logf("%s killed as %s finished snapshot %d (log compaction in progress)", c.nodes[victim].id, c.nodes[leader].id, now.Index)
			return
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Fatalf("%s took no snapshot in 90s of churn", c.nodes[leader].id)
}

// assertRestoredFromSnapshot checks the on-disk evidence: the restarted
// node now holds a snapshot past the index its log had reached when it
// was killed, which it could only have received from the leader (its own
// log ended before that point, and the survivors had compacted theirs).
func assertRestoredFromSnapshot(t *testing.T, c *cluster, victim int, lastIdx uint64) {
	t.Helper()
	snap, ok := newestSnapshot(c.nodes[victim].dataDir)
	if !ok {
		t.Fatalf("%s has no snapshot after restart", c.nodes[victim].id)
	}
	if snap.Index <= lastIdx {
		t.Fatalf("%s newest snapshot index %d is not past its pre-kill log index %d", c.nodes[victim].id, snap.Index, lastIdx)
	}
	t.Logf("%s newest snapshot index %d (log had ended at %d)", c.nodes[victim].id, snap.Index, lastIdx)
}
