package metastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestFSMSnapshotRestoreRoundTrip(t *testing.T) {
	source, err := newFSM(filepath.Join(t.TempDir(), "meta.db"), nil)
	if err != nil {
		t.Fatalf("newFSM(source): %v", err)
	}
	defer source.db.Close()

	data, err := json.Marshal(topic.Topic{Name: "orders", Partitions: 2})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := source.applyCreateTopic(data); err != nil {
		t.Fatalf("applyCreateTopic: %v", err)
	}

	snap, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	snapData := snap.(*fsmSnapshot).data

	target, err := newFSM(filepath.Join(t.TempDir(), "meta.db"), nil)
	if err != nil {
		t.Fatalf("newFSM(target): %v", err)
	}
	defer func() { target.db.Close() }()

	versionBefore := target.metadataVersion()
	if err := target.Restore(io.NopCloser(bytes.NewReader(snapData))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := target.metadataVersion(); got <= versionBefore {
		t.Fatalf("metadataVersion after restore = %d, want > %d", got, versionBefore)
	}

	// The restored db must be open and contain the snapshotted topic.
	err = target.view("get_topic", func(tx *bolt.Tx) error {
		if tx.Bucket(bucketTopics).Get([]byte("orders")) == nil {
			return errors.New("topic missing after restore")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view after restore: %v", err)
	}
}
