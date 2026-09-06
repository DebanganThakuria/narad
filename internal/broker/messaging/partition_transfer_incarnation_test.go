package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// The transfer listing reads the directory without opening a log, so it
// checks the incarnation itself: a directory of a deleted incarnation is
// refused rather than shipped to a new owner, and a matching one reports
// its incarnation for the destination to verify.
func TestPartitionTransferInfoRefusesStaleIncarnation(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "2222222222222222", Partitions: 1}
	dataDir := t.TempDir()
	e := newTestEngineWithDir(t, dataDir, ms, nil, nil)
	ctx := context.Background()

	info, err := e.PartitionTransferInfo(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("PartitionTransferInfo: %v", err)
	}
	if info.IncarnationID != "2222222222222222" {
		t.Fatalf("IncarnationID = %q, want the record's", info.IncarnationID)
	}

	// The directory now belongs to a deleted incarnation (this node
	// missed its purge): the listing must refuse.
	if err := e.logs.CloseTopic("orders"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "orders"), "1111111111111111"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	if _, err := e.PartitionTransferInfo(ctx, "orders", 0); !errors.Is(err, runtime.ErrStaleTopicIncarnation) {
		t.Fatalf("PartitionTransferInfo on a stale directory error = %v, want ErrStaleTopicIncarnation", err)
	}
}
