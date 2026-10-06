package metastore

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// While one member does not report a release that applies them, every
// proposer of an entry type newer than 3.0.x proposes nothing and says
// which member holds it back, so a 3.0.x replica never meets one.
func TestOlderFSMNeverSeesANewEntryType(t *testing.T) {
	ctx := context.Background()
	logs := &lockedBuffer{}
	addr := freeAddr(t)
	s, err := New(Config{NodeID: "ot-0", DataDir: t.TempDir(), BindAddr: addr, AdvertiseAddr: addr, Log: slog.New(slog.NewTextHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	waitUntil(t, 10*time.Second, "the store to lead", func() bool {
		_, err := s.ListAssignments("__probe__")
		return s.IsLeader() && err == nil
	})
	registerCurrent(t, s, "a")
	if err := s.RegisterMember(ctx, Member{ID: "old", Addr: "old:7942", Status: MemberAlive, LastHeartbeat: 1}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orders", "orders-copy"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, ID: "00000000000000" + name[:2], Partitions: 3, RetentionMs: 3_600_000}); err != nil {
			t.Fatal(err)
		}
	}

	before := s.r.LastIndex()
	proposers := map[string]func() error{
		"CreateTopicWith": func() error {
			return s.CreateTopicWith(ctx, topic.Topic{Name: "fresh", Partitions: 3}, CreateTopicSpec{Schema: []byte(`{}`)})
		},
		"UpdateTopicIf": func() error {
			return s.UpdateTopicIf(ctx, topic.Topic{Name: "orders", Partitions: 6}, "00000000000000or")
		},
		"DeleteTopicIf": func() error { return s.DeleteTopicIf(ctx, "orders", "00000000000000or") },
		"PutSchemaIf":   func() error { return s.PutSchemaIf(ctx, "orders", 1, []byte(`{}`), "00000000000000or") },
		"AttachChildIf": func() error {
			return s.AttachChildIf(ctx, "orders", "orders-copy", 0, "00000000000000or", "00000000000000or")
		},
		"DetachChildIf": func() error {
			return s.DetachChildIf(ctx, "orders", "orders-copy", "00000000000000or", "00000000000000or")
		},
		"AssignPartitionIfAbsent": func() error { return s.AssignPartitionIfAbsent(ctx, "orders", 0, "a", "00000000000000or") },
		"PruneAssignment":         func() error { return s.PruneAssignment(ctx, "orders", 7) },
		"MarkMemberDeadIf":        func() error { return s.MarkMemberDeadIf(ctx, "a", 1) },
	}
	for name, propose := range proposers {
		for range 2 {
			err := propose()
			if !errors.Is(err, ErrEntryTypeNotYetUsable) || !strings.Contains(err.Error(), `"old"`) {
				t.Fatalf("%s with a 3.0.x member = %v, want ErrEntryTypeNotYetUsable naming the member", name, err)
			}
		}
	}
	if after := s.r.LastIndex(); after != before {
		t.Fatalf("raft log moved from %d to %d: a proposer that may not use its entry type proposed something", before, after)
	}
	if n := strings.Count(logs.String(), "not using a new raft entry type yet"); n != len(proposers) {
		t.Fatalf("%d held-back lines for %d entry types each proposed twice within a minute, want one per type:\n%s", n, len(proposers), logs.String())
	}
	if strings.Contains(logs.String(), "every member applies raft entry type") {
		t.Fatalf("a first-use line was logged while no new type was used:\n%s", logs.String())
	}
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
