package metastore

// Raft entry types during a rolling upgrade. A release that adds a Raft
// entry type must not propose it while any member runs a release that
// does not apply it: a 3.0.x replica would skip the entry silently, and
// 3.1.0 and later stop applying (fsm_failstop.go). So each member reports
// its build and the newest entry type it applies in the heartbeat it
// already sends (Member.Build, Member.EntryTypes), and the leader asks
// EveryMemberKnows before it proposes a new type, proposing today's
// entries until every member knows it. A joiner that applies fewer types
// than every current member is refused at the door (the join handler,
// MinMemberEntryTypes), so a cluster that may already use a type never
// admits a node that would skip it.
//
// The check reads the local replica, so it is the leader's view: a
// member that rolls back counts with its old report until its next
// heartbeat is applied, about five seconds. Rolling back after a new
// type was used is unsupported.

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/hashicorp/raft"
)

// entryTypeReport is one member's account of the Raft entry types it
// applies.
type entryTypeReport struct {
	id    string
	build string
	// entryTypes is the newest type the member applies: as reported, the
	// 3.0.x set for a record that reports nothing, 0 for a Raft server
	// with no member record.
	entryTypes uint32
	// recorded is false for a Raft server with no member record (a
	// staged joiner that has not registered yet, or a server whose record
	// was forgotten): its release is unknown.
	recorded bool
	// self marks this node, which runs this release whatever its record
	// says.
	self bool
}

// reportedEntryTypes is the newest entry type m applies. A record
// without the field comes from a release that predates it (a 3.0.x
// heartbeat, or a node rolled back to one) and reads as the set every
// 3.0.x release applies.
func reportedEntryTypes(m Member) uint32 {
	if m.EntryTypes == 0 {
		return legacyMaxEntryType
	}
	return m.EntryTypes
}

// entryTypeReports returns a report for every server in the latest Raft
// configuration (voters and non-voters) and every member record (alive,
// dead or draining), sorted by ID. This node reports MaxEntryType and its
// own build.
func (s *Store) entryTypeReports() ([]entryTypeReport, error) {
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, classifyRaftError(err)
	}
	members, err := s.ListMembers()
	if err != nil {
		return nil, err
	}
	byID := make(map[string]entryTypeReport, len(members)+1)
	for _, srv := range future.Configuration().Servers {
		byID[string(srv.ID)] = entryTypeReport{id: string(srv.ID)}
	}
	for _, m := range members {
		byID[m.ID] = entryTypeReport{id: m.ID, build: m.Build, entryTypes: reportedEntryTypes(m), recorded: true}
	}
	self := string(s.id)
	byID[self] = entryTypeReport{id: self, build: s.fsm.build, entryTypes: MaxEntryType, recorded: true, self: true}
	return slices.SortedFunc(maps.Values(byID), func(a, b entryTypeReport) int { return cmp.Compare(a.id, b.id) }), nil
}

// EveryMemberKnows reports whether entryType may be proposed: whether
// every member of the cluster applies it. It reads the local replica, so
// call it on the leader, right before proposing. A type every release
// since 3.0.0 applies is always usable. A newer one is usable only when
// every server in the latest Raft configuration (voters and non-voters)
// and every member record, dead ones included, reports a release that
// applies it: a server with no record holds it back until it registers,
// and a dead member until it is removed. When the answer is false the
// caller proposes today's entries instead, and the reason names the
// first member holding the type back and the build it reported, for the
// caller to log.
func (s *Store) EveryMemberKnows(entryType uint32) (bool, string) {
	if entryType <= legacyMaxEntryType {
		return true, ""
	}
	reports, err := s.entryTypeReports()
	if err != nil {
		return false, "the cluster's members could not be read: " + err.Error()
	}
	return everyMemberKnows(entryType, reports)
}

// everyMemberKnows is EveryMemberKnows over a set of reports.
func everyMemberKnows(entryType uint32, reports []entryTypeReport) (bool, string) {
	if entryType <= legacyMaxEntryType {
		return true, ""
	}
	for _, r := range reports {
		if r.entryTypes < entryType {
			return false, r.holdsBack(entryType)
		}
	}
	return true, ""
}

// holdsBack says why r keeps entryType from being proposed.
func (r entryTypeReport) holdsBack(entryType uint32) string {
	switch {
	case !r.recorded:
		return fmt.Sprintf("raft server %q has no member record, so its release is unknown; Raft entry type %d waits until it registers or is removed", r.id, entryType)
	case r.self:
		return fmt.Sprintf("this node %q (%s) applies Raft entry types up to %d, not entry type %d", r.id, buildName(r.build), r.entryTypes, entryType)
	default:
		return fmt.Sprintf("member %q (%s) applies Raft entry types up to %d; Raft entry type %d waits until it is upgraded or removed", r.id, buildName(r.build), r.entryTypes, entryType)
	}
}

// MinMemberEntryTypes returns the fewest Raft entry types any member
// applies, over the same servers and records as EveryMemberKnows except
// excludeID (a joiner's own record must not judge it). A Raft server
// with no member record counts as 0. The join handler refuses a joiner
// that applies fewer types than this: every member applies more, so the
// cluster may already use an entry the joiner would skip.
func (s *Store) MinMemberEntryTypes(excludeID string) (uint32, error) {
	reports, err := s.entryTypeReports()
	if err != nil {
		return 0, err
	}
	lowest := MaxEntryType
	for _, r := range reports {
		if r.id != excludeID {
			lowest = min(lowest, r.entryTypes)
		}
	}
	return lowest, nil
}

// RaftServer reports whether id is a server, voter or non-voter, in the
// latest Raft configuration this node knows.
func (s *Store) RaftServer(id string) (bool, error) {
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return false, classifyRaftError(err)
	}
	sid := raft.ServerID(id)
	return slices.ContainsFunc(future.Configuration().Servers, func(srv raft.Server) bool { return srv.ID == sid }), nil
}
