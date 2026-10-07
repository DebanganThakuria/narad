package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The leader re-authorizes every remotes registry write against its own
// user records, as it does for a remote child write: a caller it does
// not know, one that is not an admin, and a write with no caller are
// refused with 403 and a refused audit line, before anything is
// proposed.
func TestRemoteRegistryWritesReauthorizeTheCallerOnTheLeader(t *testing.T) {
	ctx := context.Background()
	store := rigStore(t, "n0")
	if err := store.RegisterMember(ctx, metastore.Member{ID: "n0", Addr: "127.0.0.1:1", Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	rigSeedUser(t, store, "alice", rigRandomString(18), user.Grant{Action: user.ActionAdmin}, false)
	rigSeedUser(t, store, "bob", rigRandomString(18), user.Grant{Action: user.ActionProduce, Patterns: []string{"*"}}, false)
	logs := &syncLog{}
	reg := NewRemoteRegistry(RemoteRegistryDeps{Store: store, Log: slog.New(slog.NewJSONHandler(logs, nil)), SelfID: "n0"})

	body, _ := json.Marshal(metastore.DeleteRemoteOp{Name: "b"})
	for _, tc := range []struct {
		actor  string
		status int
		want   string
	}{
		{"", http.StatusForbidden, "caller required"},
		{"mallory", http.StatusForbidden, "caller unknown to the leader"},
		{"bob", http.StatusForbidden, "admin privileges required"},
		// An admin passes the check and reaches the store: no such remote.
		{"alice", http.StatusNotFound, "remote not found"},
	} {
		for _, sub := range []string{nodewire.RemoteSubDelete, nodewire.RemoteSubCreate, nodewire.RemoteSubUpdate, nodewire.RemoteSubReencrypt} {
			if tc.status != http.StatusForbidden && sub != nodewire.RemoteSubDelete {
				continue
			}
			res := reg.ServeWrite(ctx, nodewire.RemoteWriteRequest{SubOp: sub, Actor: tc.actor, RequestID: "req-" + tc.actor, Body: body})
			if res.Status != tc.status || !strings.Contains(string(res.Body), tc.want) {
				t.Errorf("%s as %q: %d %s, want %d naming %q", sub, tc.actor, res.Status, res.Body, tc.status, tc.want)
			}
		}
	}
	denied := 0
	for _, sub := range []string{nodewire.RemoteSubDelete, nodewire.RemoteSubCreate, nodewire.RemoteSubUpdate, nodewire.RemoteSubReencrypt} {
		for _, l := range logs.auditLines(sub) {
			if l["outcome"] == "refused" && l["class"] == "denied" {
				denied++
			}
		}
	}
	if denied != 12 {
		t.Fatalf("refused audit lines with class denied = %d, want 12 (three callers, four writes)", denied)
	}
}
