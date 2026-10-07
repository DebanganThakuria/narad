package metastore

import (
	"encoding/json"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestRemotesVersionMovesOnlyOnItsOwnBumpAndOnRestore(t *testing.T) {
	var v metadataDomainVersions
	start := v.remotesVersion()
	v.bumpTopic("orders")
	v.bumpAssignment("orders")
	v.bumpSchema("orders")
	v.bumpUsers()
	v.bumpRoutingMembers()
	v.retireTopic("orders")
	if got := v.remotesVersion(); got != start {
		t.Fatalf("remotes version moved to %d on another domain's bump", got)
	}
	v.bumpRemotes()
	afterBump := v.remotesVersion()
	if afterBump <= start {
		t.Fatalf("bumpRemotes did not move the version: %d", afterBump)
	}
	if v.latest() != afterBump {
		t.Fatalf("latest() = %d, want the remotes bump %d", v.latest(), afterBump)
	}
	v.bumpAll()
	if got := v.remotesVersion(); got <= afterBump {
		t.Fatalf("bumpAll did not move the remotes version: %d", got)
	}
}

func openTestFSM(t *testing.T) *fsmState {
	t.Helper()
	f, err := newFSM(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	return f
}

func putRaw(t *testing.T, f *fsmState, bucket []byte, key string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.update(func(tx *bolt.Tx) error { return tx.Bucket(bucket).Put([]byte(key), raw) }); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteHelpersInTheCallersTransaction(t *testing.T) {
	f := openTestFSM(t)
	putRaw(t, f, bucketRemotes, "b", domremote.Record{Name: "b"})
	putRaw(t, f, bucketRemotes, domremote.KeysKey, domremote.Keys{Salt: []byte{1}})
	putRaw(t, f, bucketTopics, "orders-to-b", topic.Topic{Name: "orders-to-b", Parent: "orders", Role: topic.RoleChild, Remote: &topic.RemoteLink{Name: "b", Topic: "orders"}})
	putRaw(t, f, bucketTopics, "audit-to-b", topic.Topic{Name: "audit-to-b", Parent: "audit", Role: topic.RoleChild, Remote: &topic.RemoteLink{Name: "b", Topic: "audit"}})
	putRaw(t, f, bucketTopics, "orders-to-c", topic.Topic{Name: "orders-to-c", Parent: "orders", Role: topic.RoleChild, Remote: &topic.RemoteLink{Name: "c", Topic: "orders"}})
	putRaw(t, f, bucketTopics, "orders", topic.Topic{Name: "orders", Role: topic.RoleParent})

	err := f.view(func(tx *bolt.Tx) error {
		for name, want := range map[string]bool{"b": true, "c": false, domremote.KeysKey: false, "": false} {
			if got, err := remoteExists(tx, name); err != nil || got != want {
				t.Fatalf("remoteExists(%q) = %v, %v, want %v", name, got, err, want)
			}
		}
		links, err := remoteChildrenNaming(tx, "b")
		if err != nil {
			return err
		}
		if len(links) != 2 || links[0] != "audit/audit-to-b" || links[1] != "orders/orders-to-b" {
			t.Fatalf("remoteChildrenNaming(b) = %q", links)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The phase-0 apply stubs of the remote child ops refuse without
// touching state.
func TestRemoteChildApplyStubsRefuse(t *testing.T) {
	f := openTestFSM(t)
	for _, apply := range []func([]byte) error{f.applyAttachRemoteChild, f.applySetRemoteChildState} {
		if err := apply([]byte(`{}`)); err == nil {
			t.Fatal("a phase-0 stub applied")
		}
	}
}
