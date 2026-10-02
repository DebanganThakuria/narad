package metastore

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

// lockedBuffer is an io.Writer a logger and the test can share.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// recordedAddressLines returns the JSON log lines about this node's
// recorded Raft address.
func recordedAddressLines(t *testing.T, b *lockedBuffer) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var lines []map[string]any
	for _, raw := range bytes.Split(b.buf.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal(raw, &line); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		if msg, _ := line["msg"].(string); strings.HasPrefix(msg, "raft configuration records this node at an address other than the one it advertises") {
			lines = append(lines, line)
		}
	}
	return lines
}

// A seed whose Raft first started on a loopback cluster.addr, rebound to
// a routable one to grow it, is still recorded at the loopback address:
// rebinding changes the transport, not the Raft configuration, and the
// nodes that join would dial the loopback address. Startup used to say
// nothing; it now says so at error level, naming both addresses.
func TestStartupLogsAnErrorWhenTheRaftConfigurationKeepsAnEarlierAddress(t *testing.T) {
	dir := t.TempDir()
	first, err := New(Config{NodeID: "seed", DataDir: dir, BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:7943"})
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var out lockedBuffer
	log := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	second, err := New(Config{NodeID: "seed", DataDir: dir, BindAddr: "127.0.0.1:0", AdvertiseAddr: "10.0.0.5:7943", Log: log})
	if err != nil {
		t.Fatalf("restart on a routable address: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	lines := recordedAddressLines(t, &out)
	if len(lines) != 1 {
		t.Fatalf("got %d lines about the recorded raft address, want 1: %v", len(lines), lines)
	}
	line := lines[0]
	if line["level"] != "ERROR" || line["recorded_addr"] != "127.0.0.1:7943" || line["advertise_addr"] != "10.0.0.5:7943" {
		t.Fatalf("line = %v, want an error naming recorded_addr 127.0.0.1:7943 and advertise_addr 10.0.0.5:7943", line)
	}
}

// Only a mismatch other nodes can trip over is an error: a node alone in
// the configuration that now advertises loopback takes no peers, and a
// node not yet in the configuration (a joiner awaiting admission) has no
// recorded address at all.
func TestRecordedRaftAddressIsAnErrorOnlyWhereAnotherNodeMayDialIt(t *testing.T) {
	self := func(addr string) raft.Server { return raft.Server{ID: "a", Address: raft.ServerAddress(addr)} }
	other := func(id, addr string) raft.Server {
		return raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(addr)}
	}
	for _, tc := range []struct {
		name      string
		servers   []raft.Server
		advertise string
		wantLevel string // "" for no line
	}{
		{name: "recorded where it advertises", servers: []raft.Server{self("10.0.0.5:7943"), other("b", "10.0.0.6:7943")}, advertise: "10.0.0.5:7943"},
		{name: "not in the configuration yet", servers: []raft.Server{other("b", "10.0.0.6:7943")}, advertise: "10.0.0.5:7943"},
		{name: "empty configuration", advertise: "10.0.0.5:7943"},
		{name: "alone, first started on loopback, now routable", servers: []raft.Server{self("127.0.0.1:7943")}, advertise: "10.0.0.5:7943", wantLevel: "ERROR"},
		{name: "joined, first started on loopback, now routable", servers: []raft.Server{self("127.0.0.1:7943"), other("b", "10.0.0.6:7943")}, advertise: "10.0.0.5:7943", wantLevel: "ERROR"},
		{name: "joined, now rebound to loopback", servers: []raft.Server{self("10.0.0.5:7943"), other("b", "10.0.0.6:7943")}, advertise: "127.0.0.1:7943", wantLevel: "ERROR"},
		{name: "alone, now routable at another address", servers: []raft.Server{self("10.0.0.5:7943")}, advertise: "10.0.0.9:7943", wantLevel: "ERROR"},
		{name: "alone, now rebound to loopback", servers: []raft.Server{self("10.0.0.5:7943")}, advertise: "127.0.0.1:7943", wantLevel: "INFO"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out lockedBuffer
			log := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
			reportRecordedAddress(log, raft.Configuration{Servers: tc.servers}, "a", tc.advertise)
			lines := recordedAddressLines(t, &out)
			if tc.wantLevel == "" {
				if len(lines) != 0 {
					t.Fatalf("lines = %v, want none", lines)
				}
				return
			}
			if len(lines) != 1 || lines[0]["level"] != tc.wantLevel {
				t.Fatalf("lines = %v, want one at %s", lines, tc.wantLevel)
			}
			if lines[0]["advertise_addr"] != tc.advertise || lines[0]["recorded_addr"] != string(tc.servers[0].Address) {
				t.Fatalf("line = %v, want both addresses named", lines[0])
			}
		})
	}
}
