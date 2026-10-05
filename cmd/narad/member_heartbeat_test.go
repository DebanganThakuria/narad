package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// olderLeaderRegistrar answers member registrations the way a v3.0.1
// leader does: its strict decoder refuses a frame carrying the build or
// entry types with 400 and the trailing-payload error, and records the
// v3.0.1 frame. answer, when set, replaces the answer to that frame.
type olderLeaderRegistrar struct {
	mu     sync.Mutex
	seen   []nodewire.MemberRequest
	answer *nodewire.Response
}

func (r *olderLeaderRegistrar) RegisterMember(_ context.Context, _ string, req nodewire.MemberRequest) (nodewire.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, req)
	if req.Build != "" || req.EntryTypes != 0 {
		return nodewire.Response{Status: http.StatusBadRequest, Body: []byte(`{"error":"invalid member request: ` + nodewire.TrailingPayloadError + `"}` + "\n")}, nil
	}
	if r.answer != nil {
		return *r.answer, nil
	}
	return nodewire.Response{Status: http.StatusNoContent}, nil
}

// A v3.0.1 leader refuses a heartbeat carrying the build and entry
// types. The member sends it again at once in the v3.0.1 frame, so
// during a rolling upgrade it never misses a heartbeat window and is
// never marked dead for it.
func TestHeartbeatToAnOlderLeaderFallsBackToTheLegacyFrame(t *testing.T) {
	ctx := context.Background()
	member := metastore.Member{
		ID: "narad-1", Addr: "10.0.0.11:7942", ClusterAddr: "10.0.0.11:7943", Status: metastore.MemberAlive, LastHeartbeat: 1700000000,
		Build: "narad v3.1.0", EntryTypes: metastore.MaxEntryType,
	}

	reg := &olderLeaderRegistrar{}
	if err := forwardMember(ctx, reg, "10.0.0.10:7942", member); err != nil {
		t.Fatalf("heartbeat to a v3.0.1 leader: %v", err)
	}
	if len(reg.seen) != 2 {
		t.Fatalf("frames sent = %d (%+v), want the new frame and then the v3.0.1 frame", len(reg.seen), reg.seen)
	}
	if first := reg.seen[0]; first.Build != member.Build || first.EntryTypes != member.EntryTypes {
		t.Fatalf("first frame = %+v, want it to carry the build and entry types", first)
	}
	legacy := reg.seen[0]
	legacy.Build, legacy.EntryTypes = "", 0
	if reg.seen[1] != legacy {
		t.Fatalf("resent frame = %+v, want %+v", reg.seen[1], legacy)
	}

	// The legacy answer still counts: a removed member stays removed.
	reg = &olderLeaderRegistrar{answer: &nodewire.Response{Status: http.StatusGone}}
	if err := forwardMember(ctx, reg, "10.0.0.10:7942", member); !errors.Is(err, errMemberRemoved) {
		t.Fatalf("410 to the v3.0.1 frame: err = %v, want errMemberRemoved", err)
	}

	// Any other refusal is not resent.
	other := &fixedRegistrar{res: nodewire.Response{Status: http.StatusBadRequest, Body: []byte(`{"error":"member addr is required"}`)}}
	if err := forwardMember(ctx, other, "10.0.0.10:7942", member); err == nil {
		t.Fatal("a 400 that is not a trailing-field refusal was treated as success")
	}
	if other.calls != 1 {
		t.Fatalf("frames sent on a plain 400 = %d, want 1", other.calls)
	}
}

// fixedRegistrar answers every registration with res.
type fixedRegistrar struct {
	res   nodewire.Response
	calls int
}

func (r *fixedRegistrar) RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error) {
	r.calls++
	return r.res, nil
}

// This node's heartbeat reports its build and the newest Raft entry type
// it applies, and the leader records both, which is how it learns which
// entry types every member knows.
func TestHeartbeatRecordsBuildAndEntryTypes(t *testing.T) {
	store, err := metastore.New(metastore.Config{NodeID: "narad-0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	waitForLeadership(t, store)

	member := localMember("narad-0", "127.0.0.1:7942", "127.0.0.1:7943")
	if err := registerMember(context.Background(), store, member, nil); err != nil {
		t.Fatalf("registerMember on the leader: %v", err)
	}
	got, err := store.GetMember("narad-0")
	if err != nil {
		t.Fatalf("GetMember: %v", err)
	}
	if got.Build != versionString() || got.EntryTypes != metastore.MaxEntryType || got.Status != metastore.MemberAlive {
		t.Fatalf("recorded member = %+v, want build %q, entry types %d, alive", got, versionString(), metastore.MaxEntryType)
	}
}
