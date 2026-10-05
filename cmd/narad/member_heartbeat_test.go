package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
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

// A member whose heartbeats keep failing is on its way to being marked
// dead by the leader. That used to be logged at debug only, so a
// production log showed nothing until the node was already dead. Three
// failures in a row spanning two intervals warn, at most once a minute
// while the streak lasts, and the streak is exported.
func TestHeartbeatFailuresWarnAfterThreeConsecutive(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	logs := &recordedLog{}
	now := time.Unix(1_700_000_000, 0)
	h := newHeartbeatHealth(slog.New(logs), m, "narad-1", 5*time.Second, func() time.Time { return now })
	fail := errors.New("register member: leader member address unavailable")

	h.observe(fail)
	now = now.Add(5 * time.Second)
	h.observe(fail)
	if n := logs.count(slog.LevelWarn, "member heartbeat failing"); n != 0 {
		t.Fatalf("warnings after 2 failures = %d, want 0", n)
	}
	if got := gaugeValue(t, reg, "narad_member_heartbeat_failures"); got != 2 {
		t.Fatalf("narad_member_heartbeat_failures after 2 failures = %v, want 2", got)
	}
	now = now.Add(5 * time.Second)
	h.observe(fail)
	if n := logs.count(slog.LevelWarn, "member heartbeat failing"); n != 1 {
		t.Fatalf("warnings after 3 failures over 2 intervals = %d, want 1", n)
	}
	if n := logs.count(slog.LevelDebug, "member heartbeat failed"); n != 3 {
		t.Fatalf("debug lines = %d, want one per failure", n)
	}

	// Repeats are held to one a minute while the streak lasts.
	for range 11 {
		now = now.Add(5 * time.Second)
		h.observe(fail)
	}
	if n := logs.count(slog.LevelWarn, "member heartbeat failing"); n != 1 {
		t.Fatalf("warnings within a minute of the first = %d, want 1", n)
	}
	now = now.Add(5 * time.Second)
	h.observe(fail)
	if n := logs.count(slog.LevelWarn, "member heartbeat failing"); n != 2 {
		t.Fatalf("warnings a minute after the first = %d, want 2", n)
	}
	if got := gaugeValue(t, reg, "narad_member_heartbeat_last_success_timestamp_seconds"); got != 0 {
		t.Fatalf("last success before any = %v, want 0", got)
	}

	now = now.Add(5 * time.Second)
	h.observe(nil)
	if n := logs.count(slog.LevelInfo, "member heartbeat recovered"); n != 1 {
		t.Fatalf("recovery lines = %d, want 1", n)
	}
	if got := gaugeValue(t, reg, "narad_member_heartbeat_failures"); got != 0 {
		t.Fatalf("narad_member_heartbeat_failures after a success = %v, want 0", got)
	}
	if got := gaugeValue(t, reg, "narad_member_heartbeat_last_success_timestamp_seconds"); got != float64(now.Unix()) {
		t.Fatalf("last success = %v, want %v", got, now.Unix())
	}
}

// Until a leader exists a booting node retries every 250 ms, and a few
// failed retries are a normal boot, not a warning. The streak must also
// have lasted two heartbeat intervals.
func TestHeartbeatBootRetriesStayQuiet(t *testing.T) {
	logs := &recordedLog{}
	now := time.Unix(1_700_000_000, 0)
	h := newHeartbeatHealth(slog.New(logs), nil, "narad-0", 5*time.Second, func() time.Time { return now })
	for range 20 { // 5 s of boot retries
		h.observe(errors.New("register member: leader member address unavailable"))
		now = now.Add(memberRegisterRetryInterval)
	}
	h.observe(nil)
	if n := logs.count(slog.LevelWarn, "member heartbeat failing"); n != 0 {
		t.Fatalf("warnings during 5s of boot retries = %d, want 0", n)
	}
	if n := logs.count(slog.LevelInfo, "member heartbeat recovered"); n != 0 {
		t.Fatalf("recovery lines after a streak that never warned = %d, want 0", n)
	}
}

// A decommissioned member can never register again: that is said once,
// at error level, not warned about every minute.
func TestHeartbeatRemovedMemberIsLoggedOnceAtError(t *testing.T) {
	logs := &recordedLog{}
	now := time.Unix(1_700_000_000, 0)
	h := newHeartbeatHealth(slog.New(logs), nil, "narad-2", 5*time.Second, func() time.Time { return now })
	for range 30 {
		h.observe(errMemberRemoved)
		now = now.Add(5 * time.Second)
	}
	if n := logs.count(slog.LevelError, "member heartbeat refused"); n != 1 {
		t.Fatalf("error lines for a removed member = %d, want 1", n)
	}
	if n := logs.count(slog.LevelWarn, "member heartbeat failing"); n != 0 {
		t.Fatalf("warnings for a removed member = %d, want 0", n)
	}
}

// The heartbeater itself feeds the warning: a node that can reach no
// leader (a join-only node nobody admitted) warns within a few
// intervals, and a heartbeat cut short by shutdown is not counted.
func TestHeartbeaterWarnsWhenRegistrationsKeepFailing(t *testing.T) {
	store, err := metastore.New(metastore.Config{NodeID: "narad-3", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", JoinOnly: true})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	logs := &recordedLog{}
	member := metastore.Member{ID: "narad-3", Addr: "127.0.0.1:7949", Status: metastore.MemberAlive}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runMemberHeartbeater(ctx, store, member, 50*time.Millisecond, nil, m, slog.New(logs))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for logs.count(slog.LevelWarn, "member heartbeat failing") == 0 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("no warning after 5s of failing heartbeats (debug lines: %d)", logs.count(slog.LevelDebug, "member heartbeat failed"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if got := gaugeValue(t, reg, "narad_member_heartbeat_failures"); got < 3 {
		t.Fatalf("narad_member_heartbeat_failures = %v, want at least 3", got)
	}
}

// Both heartbeat series are part of the process registry the binary
// serves.
func TestHeartbeatGaugesAreRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.New(reg)
	gaugeValue(t, reg, "narad_member_heartbeat_failures")
	gaugeValue(t, reg, "narad_member_heartbeat_last_success_timestamp_seconds")
}

// gaugeValue returns the value of an unlabelled gauge, failing the test
// when it is not exported.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("%s is not exported", name)
	return 0
}
