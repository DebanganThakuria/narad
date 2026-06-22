package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/replication"
)

func TestProduce_HTTPAcceptPathDoesNotUseSynchronousReplicator(t *testing.T) {
	env := newTestEnv(t, withReplicatorFactory(func(*metastore.Store, *http.Client) replication.Replicator {
		return rejectingReplicator{}
	}))
	mustCreateTopic(t, env, createTopicReq{Name: "wal-accept", Partitions: 3, ReplicationFactor: 2})

	result := mustProduce(t, env, "wal-accept", "stable", map[string]string{"hello": "wal"})

	resp := getJSON(t, fmt.Sprintf("%s/v1/topics/wal-accept/consume?partition=%d&offset=%d", env.Server.URL, result.Partition, result.Offset))
	expectStatus(t, resp, http.StatusOK)
	msg := readJSON[topic.Message](t, resp)
	if msg.Partition != result.Partition || msg.Offset != result.Offset {
		t.Fatalf("visible message = partition %d offset %d, want partition %d offset %d",
			msg.Partition, msg.Offset, result.Partition, result.Offset)
	}
}

type rejectingReplicator struct{}

func (rejectingReplicator) Replicate(context.Context, string, int, int64, []byte) error {
	return errors.New("synchronous replicator should not be called by HTTP produce")
}
