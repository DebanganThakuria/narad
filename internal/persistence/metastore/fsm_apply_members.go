package metastore

// Member removal. Decommission ends with RemoveServer, which takes the
// node out of the Raft configuration but says nothing about the member
// record: without this op the pod keeps re-registering through the
// leader (listed alive and draining forever, walked on every route-table
// rebuild and delete broadcast) and, once the pod is deleted, its record
// lingers as dead forever. opRemoveMember deletes the record and leaves a
// tombstone that applyMemberJoin honours, so no later heartbeat can
// resurrect the member. opReadmitMember clears the tombstone; the leader
// applies it when it admits a node of that ID that starts from an EMPTY
// data directory (a deliberate re-add), never for the old incarnation.

import (
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

// memberTombstone is the value stored under bucketRemovedMembers.
type memberTombstone struct {
	RemovedAt int64 `json:"removed_at"`
}

func memberTombstoned(tx *bolt.Tx, id string) bool {
	b := tx.Bucket(bucketRemovedMembers)
	return b != nil && b.Get([]byte(id)) != nil
}

// applyRemoveMember deletes the member record and writes its tombstone.
// Idempotent: removing an already-removed (or never-registered) member
// refreshes the tombstone and succeeds, so a removal that raced a
// leadership change simply completes on a later tick. The routing
// members version advances when a record actually disappears.
func (f *fsmState) applyRemoveMember(data []byte) error {
	var p memberRemovalPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	routingChanged := false
	err := f.update(func(tx *bolt.Tx) error {
		members := tx.Bucket(bucketMembers)
		if members.Get([]byte(p.ID)) != nil {
			if err := members.Delete([]byte(p.ID)); err != nil {
				return err
			}
			routingChanged = true
		}
		v, err := json.Marshal(memberTombstone{RemovedAt: p.At})
		if err != nil {
			return err
		}
		return tx.Bucket(bucketRemovedMembers).Put([]byte(p.ID), v)
	})
	if err == nil && routingChanged {
		f.versions.bumpRoutingMembers()
	}
	return err
}

// applyReadmitMember clears a tombstone so a node of that ID may
// register again. Idempotent on a missing tombstone.
func (f *fsmState) applyReadmitMember(data []byte) error {
	var p memberRemovalPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return f.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRemovedMembers).Delete([]byte(p.ID))
	})
}
