package metastore

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// The compaction knobs reach Raft, and a zero value keeps Raft's default
// for that field rather than turning compaction off.
func TestRaftCompactionConfigReachesRaft(t *testing.T) {
	s, err := New(Config{
		NodeID:            "test-0",
		DataDir:           t.TempDir(),
		BindAddr:          "127.0.0.1:0",
		AdvertiseAddr:     "127.0.0.1:0",
		SnapshotThreshold: 64,
		SnapshotInterval:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	got := s.r.ReloadableConfig()
	want := raft.DefaultConfig()
	if got.SnapshotThreshold != 64 || got.SnapshotInterval != 50*time.Millisecond {
		t.Fatalf("snapshot threshold/interval = %d/%s, want 64/50ms", got.SnapshotThreshold, got.SnapshotInterval)
	}
	if got.TrailingLogs != want.TrailingLogs {
		t.Fatalf("trailing logs = %d, want raft default %d when unset", got.TrailingLogs, want.TrailingLogs)
	}
}
