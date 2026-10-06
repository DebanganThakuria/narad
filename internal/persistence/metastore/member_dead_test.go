package metastore

// A dead mark carries the heartbeat it was decided from, and is refused
// when a fresher heartbeat committed before it.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// A leader whose replica has not applied a heartbeat that committed
// ahead of its dead mark decides from the old heartbeat; the live member
// must not be marked dead.
func TestDeadMarkFromAStaleReplicaIsRefused(t *testing.T) {
	ctx := context.Background()
	s, _ := singleVoter(t, "dm-0")
	old := time.Now().Add(-2 * time.Minute).Unix()
	m := Member{ID: "m", Addr: "m:7942", Status: MemberAlive, LastHeartbeat: old, Build: "narad test", EntryTypes: MaxEntryType}
	if err := s.RegisterMember(ctx, m); err != nil {
		t.Fatal(err)
	}

	release := stallFSM(t, s)
	committed := s.r.LastIndex()
	beat := make(chan error, 1)
	fresh := m
	fresh.LastHeartbeat = time.Now().Unix()
	// A heartbeat re-registers the member (cluster.RPCServer stamps it).
	go func() { beat <- s.RegisterMember(ctx, fresh) }()
	waitLogPast(t, s, committed)
	mark := make(chan error, 1)
	go func() { mark <- s.MarkMemberDead(ctx, "m") }()
	waitLogPast(t, s, committed+1)
	release()

	if err := <-beat; err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	markErr := <-mark
	got, err := s.GetMember("m")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != MemberAlive {
		t.Fatalf("member status after a dead mark decided from a stale heartbeat = %s (mark answered %v), want alive", got.Status, markErr)
	}
}

func TestDeadMarkIfIsRefusedForANewerHeartbeat(t *testing.T) {
	l := newEntryLog(t)
	l.must(opMemberJoin, Member{ID: "m", Addr: "m:7942", Status: MemberAlive, LastHeartbeat: 100})
	routing := l.f.versions.routingMembersVersion()

	if err := l.apply(opMarkMemberDeadIf, markMemberDeadIfPayload{ID: "m", Observed: 99}); !errors.Is(err, ErrMemberHeartbeatNewer) {
		t.Fatalf("dead mark decided from heartbeat 99 when 100 is on record = %v, want ErrMemberHeartbeatNewer", err)
	}
	if got := l.member("m"); got.Status != MemberAlive || l.f.versions.routingMembersVersion() != routing {
		t.Fatalf("member after a refused dead mark = %+v (routing version moved: %v), want alive and unchanged", got, l.f.versions.routingMembersVersion() != routing)
	}
	l.must(opMarkMemberDeadIf, markMemberDeadIfPayload{ID: "m", Observed: 100})
	if got := l.member("m"); got.Status != MemberDead || l.f.versions.routingMembersVersion() == routing {
		t.Fatalf("member after a dead mark from its last heartbeat = %+v, want dead and the routing version moved", got)
	}
	if err := l.apply(opMarkMemberDeadIf, markMemberDeadIfPayload{ID: "nobody", Observed: 100}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dead mark of an unknown member = %v, want ErrNotFound", err)
	}
}

func (l *entryLog) member(id string) Member {
	l.t.Helper()
	var m Member
	if err := l.f.view(func(tx *bolt.Tx) error {
		return json.Unmarshal(tx.Bucket(bucketMembers).Get([]byte(id)), &m)
	}); err != nil {
		l.t.Fatal(err)
	}
	return m
}
