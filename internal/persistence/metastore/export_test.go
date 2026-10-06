package metastore

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

// LoggedEntryTypesForTest returns the entry type of every command in
// s's Raft log, oldest first.
func LoggedEntryTypesForTest(t testing.TB, s *Store) []uint32 {
	t.Helper()
	first, err := s.logs.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := s.logs.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	var out []uint32
	for i := max(first, 1); i <= last; i++ {
		var l raft.Log
		if err := s.logs.GetLog(i, &l); err != nil {
			t.Fatalf("raft log %d: %v", i, err)
		}
		if l.Type != raft.LogCommand {
			continue
		}
		var env entryEnvelope
		if err := json.Unmarshal(l.Data, &env); err != nil {
			t.Fatalf("raft log %d: %v", i, err)
		}
		out = append(out, uint32(env.Op))
	}
	return out
}

// LegacyMaxEntryTypeForTest is the newest entry type every 3.0.x
// release applies.
const LegacyMaxEntryTypeForTest = legacyMaxEntryType
