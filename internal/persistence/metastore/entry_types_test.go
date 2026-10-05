package metastore

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// A Raft entry type newer than the 3.0.x set may be proposed only once
// every member reports a release that applies it; until then the leader
// keeps proposing today's entries. The reason names the member holding
// the type back, so the caller can log it.
func TestEveryMemberKnowsAnEntryTypeOnlyWhenAllReportIt(t *testing.T) {
	next := legacyMaxEntryType + 1
	cases := []struct {
		name       string
		entryType  uint32
		reports    []entryTypeReport
		want       bool
		reasonHas  []string
		reasonLack []string
	}{
		{
			name:      "a type every release since 3.0.0 applies is always usable",
			entryType: legacyMaxEntryType,
			reports: []entryTypeReport{
				{id: "a", entryTypes: next, recorded: true, self: true},
				{id: "ghost"},
				{id: "old", entryTypes: 1, recorded: true},
			},
			want: true,
		},
		{
			name:      "every member reports a release that applies it",
			entryType: next,
			reports: []entryTypeReport{
				{id: "a", build: "narad v3.2.0", entryTypes: next, recorded: true, self: true},
				{id: "b", build: "narad v3.3.0", entryTypes: next + 1, recorded: true},
			},
			want: true,
		},
		{
			name:      "a member reporting fewer types holds it back",
			entryType: next,
			reports: []entryTypeReport{
				{id: "a", build: "narad v3.2.0", entryTypes: next, recorded: true, self: true},
				{id: "b", build: "narad v3.0.1", entryTypes: legacyMaxEntryType, recorded: true},
			},
			want:      false,
			reasonHas: []string{`"b"`, "narad v3.0.1", "up to 22", "entry type 23"},
		},
		{
			name:      "a member that reported no build is named as such",
			entryType: next,
			reports: []entryTypeReport{
				{id: "b", entryTypes: legacyMaxEntryType, recorded: true},
			},
			want:      false,
			reasonHas: []string{`"b"`, "unknown build"},
		},
		{
			name:      "a raft server without a member record holds it back",
			entryType: next,
			reports: []entryTypeReport{
				{id: "a", entryTypes: next, recorded: true, self: true},
				{id: "ghost"},
			},
			want:      false,
			reasonHas: []string{`"ghost"`, "no member record"},
		},
		{
			name:      "the first member holding it back is named",
			entryType: next,
			reports: []entryTypeReport{
				{id: "a", entryTypes: next, recorded: true, self: true},
				{id: "b", build: "narad v3.0.0", entryTypes: legacyMaxEntryType, recorded: true},
				{id: "c", build: "narad v3.0.1", entryTypes: legacyMaxEntryType, recorded: true},
			},
			want:       false,
			reasonHas:  []string{`"b"`, "narad v3.0.0"},
			reasonLack: []string{`"c"`},
		},
		{
			name:      "this node holds back a type its own release does not apply",
			entryType: next,
			reports: []entryTypeReport{
				{id: "a", build: "narad v3.1.0", entryTypes: legacyMaxEntryType, recorded: true, self: true},
			},
			want:      false,
			reasonHas: []string{"this node", `"a"`, "narad v3.1.0"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := everyMemberKnows(tc.entryType, tc.reports)
			if got != tc.want {
				t.Fatalf("everyMemberKnows(%d) = %v (%q), want %v", tc.entryType, got, reason, tc.want)
			}
			if tc.want && reason != "" {
				t.Fatalf("usable type came with reason %q", reason)
			}
			for _, s := range tc.reasonHas {
				if !strings.Contains(reason, s) {
					t.Fatalf("reason %q does not mention %s", reason, s)
				}
			}
			for _, s := range tc.reasonLack {
				if strings.Contains(reason, s) {
					t.Fatalf("reason %q mentions %s", reason, s)
				}
			}
		})
	}
}

// Every server in the Raft configuration and every member record, dead
// ones included, reports: a record without entry types reads as the
// 3.0.x set, a server with no record reads as nothing, and this node
// reads as the newest type its own release applies whatever its record
// says.
func TestEveryServerAndMemberRecordReportsItsEntryTypes(t *testing.T) {
	ctx := context.Background()
	addr := freeAddr(t)
	s, err := New(Config{NodeID: "et-0", DataDir: t.TempDir(), BindAddr: addr, AdvertiseAddr: addr, Build: "narad test-build"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	waitUntil(t, 10*time.Second, "the store to lead", func() bool {
		if !s.IsLeader() {
			return false
		}
		_, err := s.ListAssignments("__probe__")
		return err == nil
	})

	register := func(m Member) {
		t.Helper()
		m.Status, m.Addr, m.LastHeartbeat = MemberAlive, m.ID+":7942", time.Now().Unix()
		if err := s.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", m.ID, err)
		}
	}
	register(Member{ID: "et-0"}) // this node's record, as a 3.0.x heartbeat left it
	register(Member{ID: "et-new", Build: "narad v3.2.0", EntryTypes: legacyMaxEntryType + 2})
	register(Member{ID: "et-old"})
	register(Member{ID: "et-dead", Build: "narad v3.0.1"})
	if err := s.MarkMemberDead(ctx, "et-dead"); err != nil {
		t.Fatalf("MarkMemberDead: %v", err)
	}
	if adm, err := s.AdmitJoiner("ghost", freeAddr(t)); err != nil || adm.Status != JoinStaged {
		t.Fatalf("AdmitJoiner(ghost) = %+v, %v; want staged", adm, err)
	}

	reports, err := s.entryTypeReports()
	if err != nil {
		t.Fatalf("entryTypeReports: %v", err)
	}
	want := []entryTypeReport{
		{id: "et-0", build: "narad test-build", entryTypes: MaxEntryType, recorded: true, self: true},
		{id: "et-dead", build: "narad v3.0.1", entryTypes: legacyMaxEntryType, recorded: true},
		{id: "et-new", build: "narad v3.2.0", entryTypes: legacyMaxEntryType + 2, recorded: true},
		{id: "et-old", entryTypes: legacyMaxEntryType, recorded: true},
		{id: "ghost"},
	}
	if !slices.Equal(reports, want) {
		t.Fatalf("reports =\n%+v\nwant\n%+v", reports, want)
	}

	if ok, reason := s.EveryMemberKnows(legacyMaxEntryType); !ok || reason != "" {
		t.Fatalf("EveryMemberKnows(%d) = %v, %q; want true: every release applies it", legacyMaxEntryType, ok, reason)
	}
	if ok, reason := s.EveryMemberKnows(MaxEntryType + 1); ok || !strings.Contains(reason, `"et-0"`) {
		t.Fatalf("EveryMemberKnows(%d) = %v, %q; want false naming et-0", MaxEntryType+1, ok, reason)
	}

	for _, tc := range []struct {
		exclude string
		want    uint32
	}{
		{"", 0},                       // ghost reports nothing
		{"ghost", legacyMaxEntryType}, // et-old and et-dead read as the 3.0.x set
		{"et-old", 0},
	} {
		got, err := s.MinMemberEntryTypes(tc.exclude)
		if err != nil || got != tc.want {
			t.Fatalf("MinMemberEntryTypes(%q) = %d, %v; want %d", tc.exclude, got, err, tc.want)
		}
	}
	for id, want := range map[string]bool{"et-0": true, "ghost": true, "et-old": false, "nobody": false} {
		if got, err := s.RaftServer(id); err != nil || got != want {
			t.Fatalf("RaftServer(%q) = %v, %v; want %v", id, got, err, want)
		}
	}
}

// A heartbeat replaces the whole member record, so one from a node
// rolled back to a release that reports nothing clears what it reported
// before, and the member reads as the 3.0.x set again. The reported
// fields are not routing fields: changing them invalidates no route
// cache.
func TestLegacyHeartbeatClearsTheReportedVersion(t *testing.T) {
	ctx := context.Background()
	s, _ := singleVoter(t, "lh-0")
	m := Member{ID: "lh-1", Addr: "lh-1:7942", ClusterAddr: "lh-1:7943", Status: MemberAlive, LastHeartbeat: 1000}
	upgraded := m
	upgraded.Build, upgraded.EntryTypes = "narad v3.2.0", legacyMaxEntryType+1
	if err := s.RegisterMember(ctx, upgraded); err != nil {
		t.Fatalf("RegisterMember(upgraded): %v", err)
	}
	got, err := s.GetMember("lh-1")
	if err != nil || got.Build != "narad v3.2.0" || got.EntryTypes != legacyMaxEntryType+1 {
		t.Fatalf("after an upgraded heartbeat: %+v, %v; want build and entry types recorded", got, err)
	}
	routing := s.RoutingMembersVersion()

	legacy := m
	legacy.LastHeartbeat = 1005
	if err := s.RegisterMember(ctx, legacy); err != nil {
		t.Fatalf("RegisterMember(legacy): %v", err)
	}
	got, err = s.GetMember("lh-1")
	if err != nil || got.Build != "" || got.EntryTypes != 0 || got.LastHeartbeat != 1005 {
		t.Fatalf("after a legacy heartbeat: %+v, %v; want build and entry types cleared", got, err)
	}
	if v := s.RoutingMembersVersion(); v != routing {
		t.Fatalf("routing members version moved %d -> %d on a build change alone", routing, v)
	}
	lowest, err := s.MinMemberEntryTypes("")
	if err != nil || lowest != legacyMaxEntryType {
		t.Fatalf("MinMemberEntryTypes = %d, %v; want the 3.0.x set %d", lowest, err, legacyMaxEntryType)
	}
}
