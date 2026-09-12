//go:build cluster

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSteadyLoadSurvivesACrashWithNoLoss is devstack suite 3 C as a
// local test: the integration driver runs a steady produce/consume/ack
// flow while a node is killed and restarted under it, and its claim
// table has to account for every message id exactly once.
//
// It exists because a devstack run of this shape reported thousands of
// messages undelivered when its drain budget expired, which reads like
// loss and is not. After an outage a partition can go quiet for a whole
// visibility timeout: a consumer that died holding a message keeps the
// committed frontier where it is, and every offset above it is already
// acked-ahead or in flight, so there is genuinely nothing to hand out
// until that lease lapses and the message is redelivered. Delivery then
// arrives in a burst as the frontier collapses over the acked run.
//
// The drain budget here is deliberately longer than the 30s visibility
// timeout so the test measures loss, not that arithmetic. Shrinking the
// visibility timeout removes the quiet windows entirely, which is the
// operator's knob and is documented in docs/operate/index.md.
func TestSteadyLoadSurvivesACrashWithNoLoss(t *testing.T) {
	c := newCluster(t, clusterOptions{})
	c.startAll()
	c.waitAllReady(90 * time.Second)
	c.waitAdmin(60 * time.Second)
	defer c.teardown()

	runID := fmt.Sprintf("crash-drain-%d", time.Now().UnixNano()%1_000_000_000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, driverBin,
		"--mode", "steady",
		"--nodes", strings.Join(c.urls(), ","),
		"--username", "admin", "--password", adminPassword,
		"--run-id", runID,
		"--no-schema",
		"--topics", "3",
		"--partitions", "12",
		"--produce-concurrency", "16",
		"--consume-concurrency", "16",
		"--duration", "30s",
		"--drain-timeout", "60s",
		"--report-every", "5s",
		"--visibility-timeout", "30s",
		// Leases and the acked-ahead set that outlive a crash are the
		// broker's documented contract, not a driver failure.
		"--fatal-dup-after-ack=false",
		"--cleanup=false",
		"--timeout", "4m",
	)
	var out bytes.Buffer
	var outMu sync.Mutex
	cmd.Stdout = &lockedWriter{buf: &out, mu: &outMu}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatalf("start driver: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	// Kill a node once the flow is established, and bring it back after
	// an outage long enough to strand leases and reroute produces.
	time.Sleep(10 * time.Second)
	const victim = 1
	c.kill(victim)
	time.Sleep(4 * time.Second)
	c.start(victim)
	c.waitReady(victim, 90*time.Second)
	t.Logf("node %d crashed and recovered while the driver ran", victim)

	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(4 * time.Minute):
		cancel()
		t.Fatal("the driver did not finish")
	}

	outMu.Lock()
	text := out.String()
	outMu.Unlock()
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "TOTAL") || strings.HasPrefix(line, "POSTMORTEM") ||
			strings.HasPrefix(line, "PASS") || strings.Contains(line, "UNDELIVERED") ||
			strings.Contains(line, "violation") {
			t.Log("  " + line)
		}
	}
	if runErr != nil {
		t.Fatalf("a crash under steady load did not account for every message: %v", runErr)
	}
}
