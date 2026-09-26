package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func (f *fakeControllerStore) wp10bSetMember(id string, status metastore.MemberStatus) {
	for i := range f.members {
		if f.members[i].ID == id {
			f.members[i].Status = status
			f.membersVersion++
			return
		}
	}
	f.members = append(f.members, metastore.Member{ID: id, Status: status})
	f.membersVersion++
}

// The out-of-cycle pass waits for more members after one turns alive,
// so the first to register does not receive every unassigned partition,
// and skips the wait once every voter is alive.
func TestWP10BMemberPassDueDebouncesUntilVotersAlive(t *testing.T) {
	store := newFakeControllerStore()
	store.voters = []string{"narad-0", "narad-1", "narad-2"}
	c := &Controller{store: store, cfg: Config{MemberSettleDelay: time.Second}.withDefaults()}
	w := c.newMemberWatch()
	t0 := time.Now()
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	if c.memberPassDue(w, at(0)) {
		t.Fatal("pass due with no member change")
	}

	store.wp10bSetMember("narad-0", metastore.MemberAlive)
	if c.memberPassDue(w, at(0)) {
		t.Fatal("pass due on the first arrival; it would get every unassigned partition")
	}
	if c.memberPassDue(w, at(500*time.Millisecond)) {
		t.Fatal("pass due before the settle delay with voters still missing")
	}

	// A second arrival restarts the settle delay.
	store.wp10bSetMember("narad-1", metastore.MemberAlive)
	if c.memberPassDue(w, at(900*time.Millisecond)) {
		t.Fatal("pass due right after a second arrival")
	}
	if c.memberPassDue(w, at(1500*time.Millisecond)) {
		t.Fatal("pass due 600ms after the latest arrival, want the full settle delay")
	}
	if !c.memberPassDue(w, at(1900*time.Millisecond)) {
		t.Fatal("pass not due once the settle delay passed with no new arrival")
	}
	if c.memberPassDue(w, at(2000*time.Millisecond)) {
		t.Fatal("pass due again with nothing new since the last one")
	}

	// The last voter arriving makes the pass due at once.
	store.wp10bSetMember("narad-2", metastore.MemberAlive)
	if !c.memberPassDue(w, at(2100*time.Millisecond)) {
		t.Fatal("pass not due immediately once every voter is alive")
	}

	// A member dying owes no pass: its partitions stay with it.
	store.wp10bSetMember("narad-1", metastore.MemberDead)
	if c.memberPassDue(w, at(5*time.Second)) {
		t.Fatal("pass due after a member died")
	}

	// Coming back (a full-cluster restart past DeadTimeout) does.
	store.wp10bSetMember("narad-1", metastore.MemberAlive)
	if !c.memberPassDue(w, at(5100*time.Millisecond)) {
		t.Fatal("pass not due after a dead member came back with every voter alive")
	}
}

// A failed member read must not lose the arrival: the version is not
// advanced, so the next look re-reads and still sees the new member.
func TestWP10BMemberPassDueRetriesAfterListMembersError(t *testing.T) {
	store := newFakeControllerStore()
	store.voters = []string{"narad-0"}
	c := &Controller{store: store, cfg: Config{MemberSettleDelay: time.Second}.withDefaults()}
	w := c.newMemberWatch()
	now := time.Now()

	store.wp10bSetMember("narad-0", metastore.MemberAlive)
	store.listMembersErr = errors.New("transient read failure")
	if c.memberPassDue(w, now) {
		t.Fatal("pass due on a failed member read")
	}
	store.listMembersErr = nil
	if !c.memberPassDue(w, now.Add(memberWatchInterval)) {
		t.Fatal("arrival lost after a failed member read")
	}
}

// Members already alive when leadership starts are covered by the
// leader's first pass and must not trigger another.
func TestWP10BMemberWatchBaselineIsCurrentMembers(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1")
	c := &Controller{store: store, cfg: Config{}.withDefaults()}
	w := c.newMemberWatch()
	store.membersVersion++ // e.g. an address change, no new alive member
	if c.memberPassDue(w, time.Now().Add(time.Hour)) {
		t.Fatal("pass due for members that were alive at the baseline")
	}
}
