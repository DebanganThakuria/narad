package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A delete of a remote that does not exist answers 404 without a Raft
// entry: a stray `remote rm` or an "ensure absent" reconcile must not
// write the remote entry types, which a rollback to v3.1.0 cannot read.
func TestDeleteOfAMissingRemoteWritesNoEntry(t *testing.T) {
	ctx := context.Background()
	store := rigStore(t, "n0")
	if err := store.RegisterMember(ctx, metastore.Member{ID: "n0", Addr: "127.0.0.1:1", Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	rigSeedUser(t, store, "alice", rigRandomString(18), user.Grant{Action: user.ActionAdmin}, false)
	reg := NewRemoteRegistry(RemoteRegistryDeps{Store: store, Log: slog.New(slog.NewJSONHandler(&syncLog{}, nil)), SelfID: "n0"})
	before, err := store.NewestAppliedEntryType()
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(metastore.DeleteRemoteOp{Name: "b"})
	res := reg.ServeWrite(ctx, nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubDelete, Actor: "alice", RequestID: "req-rm", Body: body})
	if res.Status != http.StatusNotFound {
		t.Fatalf("delete of a missing remote: %d %s, want 404", res.Status, res.Body)
	}
	after, err := store.NewestAppliedEntryType()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("newest applied entry type %d -> %d: the refused delete was proposed", before, after)
	}
}
