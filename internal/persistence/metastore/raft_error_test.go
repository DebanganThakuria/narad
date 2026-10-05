package metastore

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Raft's leadership/availability failures must classify to
// ErrUnavailable (→ 503, retryable), while genuine errors pass through
// untouched (→ 500). Chaos testing showed these were surfacing as 500,
// telling clients "bug, don't retry" during ordinary elections.
func TestClassifyRaftError(t *testing.T) {
	unavailable := []error{
		raft.ErrNotLeader,
		raft.ErrLeadershipLost,
		raft.ErrLeadershipTransferInProgress,
		raft.ErrRaftShutdown,
		raft.ErrEnqueueTimeout,
		fmt.Errorf("wrapped: %w", raft.ErrNotLeader),
	}
	for _, in := range unavailable {
		if got := classifyRaftError(in); !errors.Is(got, errs.ErrUnavailable) {
			t.Fatalf("classifyRaftError(%v) not ErrUnavailable: %v", in, got)
		}
	}

	// Leadership lost after the append, and a shutdown that can end a
	// pending apply, leave the entry's fate open: a later leader may still
	// commit it. Every other refusal comes before the append.
	undecided := map[error]bool{
		raft.ErrLeadershipLost: true,
		raft.ErrRaftShutdown:   true,
	}
	for _, in := range unavailable {
		got := classifyRaftError(in)
		if errors.Is(got, errs.ErrOutcomeUnknown) != undecided[in] {
			t.Fatalf("classifyRaftError(%v) = %v; outcome unknown = %v, want %v", in, got, errors.Is(got, errs.ErrOutcomeUnknown), undecided[in])
		}
		if !strings.Contains(got.Error(), in.Error()) {
			t.Fatalf("classifyRaftError(%v) = %q dropped the cause", in, got)
		}
	}

	// A real business/logic error must NOT be masked as unavailable.
	other := errors.New("bbolt: disk full")
	if got := classifyRaftError(other); errors.Is(got, errs.ErrUnavailable) {
		t.Fatalf("classifyRaftError masked a real error as unavailable: %v", got)
	}
	if got := classifyRaftError(other); !errors.Is(got, other) {
		t.Fatalf("classifyRaftError dropped the original error: %v", got)
	}
}
