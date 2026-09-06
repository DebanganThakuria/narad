//go:build cluster

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Ports come from the 34000-34099 range only, ten per cluster: three
// HTTP (which the QUIC RPC plane shares over UDP) and three Raft.
const (
	portRangeBase = 34000
	portBlockSize = 10
	portBlocks    = 10
)

const (
	adminPassword = "cluster-test-admin"
	clusterSecret = "cluster-test-secret"
)

var (
	naradBin  string
	driverBin string
	repoRoot  string
)

func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, "repo root:", err)
		os.Exit(2)
	}
	repoRoot = root
	binDir, err := os.MkdirTemp("", "narad-cluster-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "bin dir:", err)
		os.Exit(2)
	}
	naradBin = filepath.Join(binDir, "narad")
	driverBin = filepath.Join(binDir, "driver")
	buildArgs := []string{"build"}
	if os.Getenv("NARAD_CLUSTER_TEST_NORACE") == "" {
		// The server under test runs with the race detector so a data
		// race provoked by a kill or a restart is a test failure, not a
		// silent corruption.
		buildArgs = append(buildArgs, "-race")
	}
	for _, b := range []struct{ out, pkg string }{{naradBin, "./cmd/narad"}, {driverBin, "./tests/integration"}} {
		cmd := exec.Command("go", append(buildArgs, "-o", b.out, b.pkg)...)
		cmd.Dir = root
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n", b.pkg, err)
			os.RemoveAll(binDir)
			os.Exit(2)
		}
	}
	code := m.Run()
	os.RemoveAll(binDir)
	os.Exit(code)
}

// ---- ports -----------------------------------------------------------------

var nextPortBlock atomic.Int32

// allocPorts hands out the next free block of the reserved range: three
// HTTP ports and three Raft ports. A block is skipped when anything in
// it is still bound (a previous test's process that has not quite gone).
func allocPorts(t *testing.T) (httpPorts, raftPorts [3]int) {
	t.Helper()
	for attempt := 0; attempt < portBlocks*2; attempt++ {
		k := int(nextPortBlock.Add(1)-1) % portBlocks
		base := portRangeBase + portBlockSize*k
		candidates := []int{base, base + 1, base + 2, base + 5, base + 6, base + 7}
		if !portsFree(candidates) {
			continue
		}
		return [3]int{base, base + 1, base + 2}, [3]int{base + 5, base + 6, base + 7}
	}
	t.Fatalf("no free port block in %d-%d", portRangeBase, portRangeBase+portBlockSize*portBlocks-1)
	return
}

func portsFree(ports []int) bool {
	for _, p := range ports {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(p))
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return false
		}
		l.Close()
		u, err := net.ListenPacket("udp", addr)
		if err != nil {
			return false
		}
		u.Close()
	}
	return true
}

// ---- cluster ---------------------------------------------------------------

type node struct {
	idx         int
	id          string
	httpPort    int
	raftPort    int
	dataDir     string
	logPath     string
	env         map[string]string // per-node extra env (TLS files)
	mu          sync.Mutex
	cmd         *exec.Cmd
	exited      chan struct{}
	exitErr     error
	running     bool
	restarts    int
	lastStarted time.Time
}

type cluster struct {
	t     *testing.T
	dir   string
	nodes [3]*node
	peers string
	env   map[string]string // extra env applied to every node
	http  *http.Client
}

type clusterOptions struct {
	env map[string]string
}

// newCluster prepares a 3-node cluster (directories, ports, env) without
// starting anything. Cleanup kills whatever is still running, scans the
// node logs for panics and data races, and preserves the logs of a
// failed test when NARAD_CLUSTER_TEST_ARTIFACTS is set.
func newCluster(t *testing.T, opts clusterOptions) *cluster {
	t.Helper()
	httpPorts, raftPorts := allocPorts(t)
	c := &cluster{
		t:    t,
		dir:  t.TempDir(),
		env:  opts.env,
		http: &http.Client{Timeout: 15 * time.Second},
	}
	var peers []string
	for i := range c.nodes {
		id := fmt.Sprintf("narad-%d", i+1)
		c.nodes[i] = &node{
			idx:      i,
			id:       id,
			httpPort: httpPorts[i],
			raftPort: raftPorts[i],
			dataDir:  filepath.Join(c.dir, id),
			logPath:  filepath.Join(c.dir, id+".log"),
			env:      map[string]string{},
		}
		peers = append(peers, fmt.Sprintf("%s@127.0.0.1:%d", id, raftPorts[i]))
	}
	c.peers = strings.Join(peers, ",")
	t.Cleanup(func() { c.teardown() })
	return c
}

func (c *cluster) url(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.nodes[i].httpPort)
}

func (c *cluster) urls() []string {
	out := make([]string, 0, len(c.nodes))
	for i := range c.nodes {
		out = append(out, c.url(i))
	}
	return out
}

// start launches node i with the cluster's env plus the node's own.
func (c *cluster) start(i int) {
	c.t.Helper()
	n := c.nodes[i]
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.running {
		c.t.Fatalf("%s already running", n.id)
	}
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.t.Fatalf("open log %s: %v", n.logPath, err)
	}
	env := map[string]string{
		"NARAD_HTTP_ADDR":                     fmt.Sprintf("127.0.0.1:%d", n.httpPort),
		"NARAD_CLUSTER_ADDR":                  fmt.Sprintf("127.0.0.1:%d", n.raftPort),
		"NARAD_NODE_ID":                       n.id,
		"NARAD_CLUSTER_PEERS":                 c.peers,
		"NARAD_DATA_DIR":                      n.dataDir,
		"NARAD_SECURITY_ENABLED":              "true",
		"NARAD_CLUSTER_SECRET":                clusterSecret,
		"NARAD_ADMIN_PASSWORD":                adminPassword,
		"NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT": "true",
		"NARAD_LOG_FORMAT":                    "text",
		"NARAD_LOG_LEVEL":                     "info",
		"GORACE":                              "halt_on_error=0",
	}
	for k, v := range c.env {
		env[k] = v
	}
	for k, v := range n.env {
		env[k] = v
	}
	cmd := exec.Command(naradBin, "serve")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir()}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	fmt.Fprintf(logFile, "==== test harness: starting %s (restart %d) at %s\n", n.id, n.restarts, time.Now().Format(time.RFC3339Nano))
	if err := cmd.Start(); err != nil {
		logFile.Close()
		c.t.Fatalf("start %s: %v", n.id, err)
	}
	n.cmd = cmd
	n.running = true
	n.restarts++
	n.lastStarted = time.Now()
	exited := make(chan struct{})
	n.exited = exited
	go func() {
		err := cmd.Wait()
		logFile.Close()
		n.mu.Lock()
		n.exitErr = err
		n.running = false
		n.mu.Unlock()
		close(exited)
	}()
	c.t.Logf("%s started pid=%d http=%d raft=%d", n.id, cmd.Process.Pid, n.httpPort, n.raftPort)
}

func (c *cluster) startAll() {
	for i := range c.nodes {
		c.start(i)
	}
}

func (c *cluster) isRunning(i int) bool {
	n := c.nodes[i]
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

// kill sends SIGKILL and waits for the process to be reaped.
func (c *cluster) kill(i int) {
	c.t.Helper()
	n := c.nodes[i]
	n.mu.Lock()
	cmd, exited, running := n.cmd, n.exited, n.running
	n.mu.Unlock()
	if !running {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGKILL)
	<-exited
	c.t.Logf("%s killed (SIGKILL)", n.id)
}

// stop sends SIGTERM (graceful: leadership transfer, drain) and waits;
// a node that ignores it for 30s is killed.
func (c *cluster) stop(i int) time.Duration {
	c.t.Helper()
	n := c.nodes[i]
	n.mu.Lock()
	cmd, exited, running := n.cmd, n.exited, n.running
	n.mu.Unlock()
	if !running {
		return 0
	}
	start := time.Now()
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		c.t.Errorf("%s did not exit within 30s of SIGTERM; killing", n.id)
		_ = cmd.Process.Signal(syscall.SIGKILL)
		<-exited
	}
	d := time.Since(start)
	c.t.Logf("%s stopped gracefully in %s", n.id, d.Round(time.Millisecond))
	return d
}

// exitError returns how node i last exited (nil for a clean exit).
func (c *cluster) exitError(i int) error {
	n := c.nodes[i]
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.exitErr
}

func (c *cluster) teardown() {
	for i := range c.nodes {
		c.kill(i)
	}
	for i, n := range c.nodes {
		body, err := os.ReadFile(n.logPath)
		if err != nil {
			continue
		}
		for _, bad := range []string{"panic:", "fatal error:", "WARNING: DATA RACE"} {
			if idx := bytes.Index(body, []byte(bad)); idx >= 0 {
				end := min(len(body), idx+2000)
				c.t.Errorf("%s log contains %q:\n%s", c.nodes[i].id, bad, body[idx:end])
			}
		}
	}
	if dir := os.Getenv("NARAD_CLUSTER_TEST_ARTIFACTS"); dir != "" && (c.t.Failed() || os.Getenv("NARAD_CLUSTER_TEST_KEEP_LOGS") != "") {
		dst := filepath.Join(dir, strings.ReplaceAll(c.t.Name(), "/", "_")+"-"+time.Now().Format("150405"))
		_ = os.MkdirAll(dst, 0o755)
		for _, n := range c.nodes {
			if body, err := os.ReadFile(n.logPath); err == nil {
				_ = os.WriteFile(filepath.Join(dst, n.id+".log"), body, 0o644)
			}
		}
		if c.t.Failed() {
			// The data directories too (partition logs, quarantined
			// copies, Raft state): a lost message is found in them.
			cp := exec.Command("cp", "-R", c.dir+"/.", filepath.Join(dst, "data"))
			if err := cp.Run(); err != nil {
				c.t.Logf("copy data dirs: %v", err)
			}
		}
		c.t.Logf("node logs preserved under %s", dst)
	}
}

// ---- readiness and HTTP -------------------------------------------------------

// waitReady blocks until node i answers /readyz with 200 and returns how
// long that took. It fails the test after timeout.
func (c *cluster) waitReady(i int, timeout time.Duration) time.Duration {
	c.t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	var last string
	// reasons is the sequence of distinct not-ready answers with when
	// each was first seen; a slow readiness is explained by it.
	var reasons []string
	for time.Now().Before(deadline) {
		if !c.isRunning(i) {
			c.t.Fatalf("%s exited while waiting for /readyz: %v", c.nodes[i].id, c.exitError(i))
		}
		resp, err := c.http.Get(c.url(i) + "/readyz")
		var now string
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				d := time.Since(start)
				if d > 5*time.Second {
					c.t.Logf("%s ready after %s; not-ready answers on the way: %s", c.nodes[i].id, d.Round(time.Millisecond), strings.Join(reasons, " | "))
				} else {
					c.t.Logf("%s ready after %s", c.nodes[i].id, d.Round(time.Millisecond))
				}
				return d
			}
			now = fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		} else {
			now = err.Error()
		}
		if now != last {
			reasons = append(reasons, fmt.Sprintf("+%s %s", time.Since(start).Round(100*time.Millisecond), now))
			last = now
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatalf("%s not ready within %s (last: %s)", c.nodes[i].id, timeout, last)
	return 0
}

func (c *cluster) waitAllReady(timeout time.Duration) {
	for i := range c.nodes {
		c.waitReady(i, timeout)
	}
}

// readyNow reports whether node i answers /readyz 200 right now.
func (c *cluster) readyNow(i int) (bool, string) {
	resp, err := c.http.Get(c.url(i) + "/readyz")
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode == http.StatusOK, strings.TrimSpace(string(body))
}

// waitAdmin waits until the seeded root admin can authenticate.
func (c *cluster) waitAdmin(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i := range c.nodes {
			if !c.isRunning(i) {
				continue
			}
			if status, _, err := c.api(i, http.MethodGet, "/v1/users", nil); err == nil && status == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatal("root admin never became usable")
}

// api sends one admin-authenticated request to node i.
func (c *cluster) api(i int, method, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		raw, ok := body.([]byte)
		if !ok {
			var err error
			raw, err = json.Marshal(body)
			if err != nil {
				return 0, nil, err
			}
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.url(i)+path, reader)
	if err != nil {
		return 0, nil, err
	}
	req.SetBasicAuth("admin", adminPassword)
	req.Header.Set("X-Narad-Client", "cluster-test")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, out, err
}

// apiWant retries the request against node i until one of the wanted
// statuses comes back or timeout passes; transport errors and 503s (an
// election in progress, a replica not yet caught up) are retried.
func (c *cluster) apiWant(i int, method, path string, body any, timeout time.Duration, want ...int) (int, []byte) {
	c.t.Helper()
	status, out, err := c.apiTry(i, method, path, body, timeout, want...)
	if err != nil {
		c.t.Fatal(err)
	}
	return status, out
}

// apiTry is apiWant without the fatal: safe from helper goroutines.
func (c *cluster) apiTry(i int, method, path string, body any, timeout time.Duration, want ...int) (int, []byte, error) {
	deadline := time.Now().Add(timeout)
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for {
		status, out, err := c.api(i, method, path, body)
		if err == nil {
			for _, w := range want {
				if status == w {
					return status, out, nil
				}
			}
		}
		lastStatus, lastBody, lastErr = status, out, err
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	return lastStatus, lastBody, fmt.Errorf("%s %s on %s: want %v, last status=%d err=%v body=%s", method, path, c.nodes[i].id, want, lastStatus, lastErr, truncate(lastBody, 400))
}

// anyRunning returns the index of a running node other than exclude
// (pass -1 for none).
func (c *cluster) anyRunning(exclude int) int {
	c.t.Helper()
	for i := range c.nodes {
		if i != exclude && c.isRunning(i) {
			return i
		}
	}
	c.t.Fatal("no running node")
	return -1
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// ---- logs -----------------------------------------------------------------------

// logOffset returns the current size of node i's log, for "since now"
// scans.
func (c *cluster) logOffset(i int) int64 {
	info, err := os.Stat(c.nodes[i].logPath)
	if err != nil {
		return 0
	}
	return info.Size()
}

// logSince returns node i's log from offset on.
func (c *cluster) logSince(i int, offset int64) string {
	body, err := os.ReadFile(c.nodes[i].logPath)
	if err != nil || int64(len(body)) <= offset {
		return ""
	}
	return string(body[offset:])
}

// waitLog blocks until node i's log (from offset on) contains substr.
func (c *cluster) waitLog(i int, offset int64, substr string, timeout time.Duration) time.Duration {
	c.t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		if strings.Contains(c.logSince(i, offset), substr) {
			return time.Since(start)
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s log did not contain %q within %s; tail:\n%s", c.nodes[i].id, substr, timeout, tail(c.logSince(i, offset), 3000))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

// countLog counts occurrences of substr in node i's log from offset on.
func (c *cluster) countLog(i int, offset int64, substr string) int {
	return strings.Count(c.logSince(i, offset), substr)
}

// leader finds the current Raft leader from the nodes' logs: the running
// node whose most recent Raft state transition is "entering leader
// state". Raft logs every transition through the process log.
func (c *cluster) leader() (int, bool) {
	best, bestPos := -1, -1
	for i := range c.nodes {
		if !c.isRunning(i) {
			continue
		}
		body := c.logSince(i, 0)
		leaderPos := strings.LastIndex(body, "entering leader state")
		if leaderPos < 0 {
			continue
		}
		followerPos := strings.LastIndex(body, "entering follower state")
		candidatePos := strings.LastIndex(body, "entering candidate state")
		if leaderPos > followerPos && leaderPos > candidatePos {
			// Several nodes can each have led at some point; the one
			// whose leader line is newest in absolute terms wins, and
			// log position is a fair proxy when only one qualifies.
			if best < 0 || leaderPos > bestPos {
				best, bestPos = i, leaderPos
			}
		}
	}
	return best, best >= 0
}

// waitLeader waits until exactly one running node believes it leads.
func (c *cluster) waitLeader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if i, ok := c.leader(); ok {
			return i
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.t.Fatal("no raft leader visible in the node logs")
	return -1
}

// follower returns a running node that is not the leader.
func (c *cluster) follower() int {
	c.t.Helper()
	leader := c.waitLeader(20 * time.Second)
	return c.anyRunning(leader)
}

// ---- driver ------------------------------------------------------------------

type driverRun struct {
	cmd    *exec.Cmd
	out    bytes.Buffer
	outMu  sync.Mutex
	done   chan struct{}
	err    error
	runID  string
	topics []string
	cancel context.CancelFunc
}

type driverOptions struct {
	topics     int
	partitions int
	messages   int
	rate       int
	timeout    time.Duration
}

// startDriver runs the tests/integration chaos driver against every node
// in the background: it produces messages (paced by rate), consumes and
// acks them, and passes only if every message is acked exactly once in
// its claim table.
func (c *cluster) startDriver(opts driverOptions) *driverRun {
	c.t.Helper()
	runID := fmt.Sprintf("cl-%d", time.Now().UnixNano()%1_000_000_000)
	d := &driverRun{done: make(chan struct{}), runID: runID}
	for i := range opts.topics {
		d.topics = append(d.topics, fmt.Sprintf("%s-%02d", runID, i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.cmd = exec.CommandContext(ctx, driverBin,
		"--mode", "chaos",
		"--nodes", strings.Join(c.urls(), ","),
		"--username", "admin", "--password", adminPassword,
		"--run-id", runID,
		"--topics", strconv.Itoa(opts.topics),
		"--partitions", strconv.Itoa(opts.partitions),
		"--messages", strconv.Itoa(opts.messages),
		"--produce-concurrency", "8",
		"--consume-concurrency", "8",
		"--produce-rate", strconv.Itoa(opts.rate),
		"--visibility-timeout", "3s",
		"--timeout", opts.timeout.String(),
		"--cleanup=false",
	)
	w := &lockedWriter{buf: &d.out, mu: &d.outMu}
	d.cmd.Stdout, d.cmd.Stderr = w, w
	if err := d.cmd.Start(); err != nil {
		c.t.Fatalf("start driver: %v", err)
	}
	go func() {
		d.err = d.cmd.Wait()
		close(d.done)
	}()
	c.t.Logf("driver started run_id=%s topics=%d messages=%d rate=%d/s", runID, opts.topics, opts.messages, opts.rate)
	return d
}

type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (d *driverRun) output() string {
	d.outMu.Lock()
	defer d.outMu.Unlock()
	return d.out.String()
}

// finished reports whether the driver has exited.
func (d *driverRun) finished() bool {
	select {
	case <-d.done:
		return true
	default:
		return false
	}
}

// wait blocks for the driver and asserts it passed: every accepted
// message was consumed and acked (duplicates are counted, not failed).
// It returns the driver's summary line.
func (d *driverRun) wait(t *testing.T) string {
	t.Helper()
	<-d.done
	out := d.output()
	summary := ""
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "PASS") {
			summary = line
		}
	}
	if d.err != nil || summary == "" {
		t.Fatalf("driver failed: %v\n%s", d.err, tail(out, 4000))
	}
	t.Logf("driver: %s", summary)
	return summary
}

// waitTopics waits until every driver topic has all its partitions
// assigned and visible on every running node, so a kill that follows
// hits partitions the victim really owns.
func (c *cluster) waitTopics(d *driverRun, partitions int, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for _, topic := range d.topics {
		for i := range c.nodes {
			for {
				status, body, err := c.api(i, http.MethodGet, "/v1/topics/"+topic, nil)
				if err == nil && status == http.StatusOK {
					var got struct {
						Stats []struct {
							Owner string `json:"owner_node"`
						} `json:"partition_stats"`
					}
					if json.Unmarshal(body, &got) == nil && len(got.Stats) == partitions && allOwned(got.Stats) {
						break
					}
				}
				if time.Now().After(deadline) {
					c.t.Fatalf("driver topic %s not fully assigned on %s within %s (status %d, err %v)", topic, c.nodes[i].id, timeout, status, err)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	if d.finished() {
		c.t.Fatalf("driver finished before the scenario started:\n%s", tail(d.output(), 2000))
	}
}

func allOwned(stats []struct {
	Owner string `json:"owner_node"`
},
) bool {
	for _, s := range stats {
		if s.Owner == "" {
			return false
		}
	}
	return true
}

// ownedPartitions returns, per node ID, how many partitions of the given
// topics it owns according to node via's merged topic view.
func (c *cluster) ownedPartitions(via int, topics []string) map[string]int {
	c.t.Helper()
	owned := map[string]int{}
	for _, topic := range topics {
		_, body := c.apiWant(via, http.MethodGet, "/v1/topics/"+topic, nil, 20*time.Second, http.StatusOK)
		var got struct {
			Stats []struct {
				Owner string `json:"owner_node"`
			} `json:"partition_stats"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			c.t.Fatalf("decode topic %s: %v", topic, err)
		}
		for _, s := range got.Stats {
			owned[s.Owner]++
		}
	}
	return owned
}

var errNotYet = errors.New("not yet")
