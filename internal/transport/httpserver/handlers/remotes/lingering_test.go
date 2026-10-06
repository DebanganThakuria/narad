package remotes

import (
	"reflect"
	"testing"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/remote"
)

// A member that did not answer is reported even when no answering
// member still holds a deleted remote: the compromise playbook reads
// the listing to confirm every node let go, and a silent node is the
// one it must not miss.
func TestListNamesASilentMemberWhenNoNodeHoldsADeletedRemote(t *testing.T) {
	statuses := []remote.MemberStatus{
		{Node: "narad-0", Report: &remote.NodeStatusReport{Node: "narad-0"}},
		{Node: "narad-2", Class: "unreachable"},
	}
	held, silent := lingering(nil, statuses)
	if len(held) != 0 {
		t.Fatalf("lingering = %+v, want none (no answering node holds a deleted remote)", held)
	}
	if !reflect.DeepEqual(silent, []string{"narad-2"}) {
		t.Fatalf("not answering = %v, want [narad-2]", silent)
	}
	held, silent = lingering([]domremote.Record{{Name: "b"}}, append(statuses, remote.MemberStatus{
		Node: "narad-1", Report: &remote.NodeStatusReport{Node: "narad-1", Remotes: []remote.NodeRemoteStatus{{Remote: "c"}}},
	}))
	if len(held) != 1 || held[0].Remote != "c" || !reflect.DeepEqual(held[0].NotAnswering, []string{"narad-2"}) || !reflect.DeepEqual(silent, []string{"narad-2"}) {
		t.Fatalf("lingering = %+v, not answering = %v", held, silent)
	}
}
