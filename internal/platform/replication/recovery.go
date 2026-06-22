package replication

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

type StoreRecovery struct {
	selfID string
	store  *metastore.Store
	logs   *runtime.Logs
	quic   *quicClientPool
}

func NewStoreRecovery(selfID string, store *metastore.Store, logs *runtime.Logs, client *http.Client) StoreRecovery {
	timeout := defaultStreamTimeout
	if client != nil && client.Timeout > 0 {
		timeout = client.Timeout
	}
	return NewStoreRecoveryWithTimeout(selfID, store, logs, timeout)
}

func NewStoreRecoveryWithTimeout(selfID string, store *metastore.Store, logs *runtime.Logs, timeout time.Duration) StoreRecovery {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	return StoreRecovery{selfID: selfID, store: store, logs: logs, quic: newQUICClientPool(timeout)}
}

func (r StoreRecovery) RepairOwnedPartitions(ctx context.Context) error {
	topics, _, err := r.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for _, t := range topics {
		assignments, err := r.store.ListAssignments(t.Name)
		if err != nil {
			return fmt.Errorf("list assignments for %s: %w", t.Name, err)
		}
		for _, assignment := range assignments {
			if assignment.OwnerID != r.selfID || assignment.FollowerID == "" {
				continue
			}
			if err := r.repairPartition(ctx, assignment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r StoreRecovery) repairPartition(ctx context.Context, assignment metastore.Assignment) error {
	follower, err := r.store.GetMember(assignment.FollowerID)
	if err != nil {
		return fmt.Errorf("lookup follower %s: %w", assignment.FollowerID, err)
	}
	if follower.Status == metastore.MemberDead {
		return nil
	}

	log, err := r.logs.Get(assignment.Topic, assignment.Partition)
	if err != nil {
		return fmt.Errorf("open owner log: %w", err)
	}
	committedTail, err := log.PersistedHighWatermark()
	if err != nil {
		return fmt.Errorf("read owner persisted high watermark: %w", err)
	}
	if err := r.repairOwnerFromFollower(ctx, follower.Addr, assignment.Topic, assignment.Partition, log, committedTail); err != nil {
		return err
	}
	if err := r.repairFollowerFromOwner(ctx, follower.Addr, assignment.Topic, assignment.Partition, log, committedTail); err != nil {
		return err
	}
	if committedTail > log.HighWatermark() {
		if err := log.AdvanceHighWatermark(committedTail); err != nil {
			return fmt.Errorf("restore owner high watermark: %w", err)
		}
	}
	return nil
}

func (r StoreRecovery) repairOwnerFromFollower(ctx context.Context, followerAddr, topicName string, partition int, log interface {
	NextOffset() int64
	Append([]byte) (int64, error)
}, committedTail int64,
) error {
	for offset := log.NextOffset(); offset < committedTail; offset++ {
		payload, found, err := r.fetchReplicaRecord(ctx, followerAddr, topicName, partition, offset, true)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("repair owner missing committed offset %d", offset)
		}
		appended, err := log.Append(payload)
		if err != nil {
			return fmt.Errorf("append recovered record: %w", err)
		}
		if appended != offset {
			return fmt.Errorf("repair offset mismatch: got %d want %d", appended, offset)
		}
	}
	return nil
}

func (r StoreRecovery) repairFollowerFromOwner(ctx context.Context, followerAddr, topicName string, partition int, log interface {
	Read(int64) ([]byte, error)
}, committedTail int64,
) error {
	start, err := r.findFollowerNextOffset(ctx, followerAddr, topicName, partition, committedTail)
	if err != nil {
		return err
	}
	for offset := start; offset < committedTail; offset++ {
		payload, err := log.Read(offset)
		if err != nil {
			return fmt.Errorf("read owner record %d: %w", offset, err)
		}
		if err := r.pushReplicaRecord(ctx, followerAddr, topicName, partition, offset, payload); err != nil {
			return err
		}
	}
	return nil
}

func (r StoreRecovery) findFollowerNextOffset(ctx context.Context, addr, topicName string, partition int, upperBound int64) (int64, error) {
	for offset := int64(0); ; offset++ {
		_, found, err := r.fetchReplicaRecord(ctx, addr, topicName, partition, offset, false)
		if err != nil {
			return 0, err
		}
		if !found {
			return offset, nil
		}
	}
}

func (r StoreRecovery) fetchReplicaRecord(ctx context.Context, addr, topicName string, partition int, offset int64, committedOnly bool) ([]byte, bool, error) {
	return r.quic.readReplica(ctx, addr, topicName, partition, offset, committedOnly)
}

func (r StoreRecovery) pushReplicaRecord(ctx context.Context, addr, topicName string, partition int, offset int64, payload []byte) error {
	results, err := r.quic.appendMulti(ctx, addr, []replicationwire.StreamAppendGroup{{
		Topic:      topicName,
		Partition:  partition,
		BaseOffset: offset,
		Payloads:   [][]byte{payload},
	}})
	if err != nil {
		return fmt.Errorf("send replicate request: %w", err)
	}
	if len(results) != 1 {
		return fmt.Errorf("replicate request returned %d results, want 1", len(results))
	}
	result := results[0]
	if result.Message != "" {
		return fmt.Errorf("replicate request failed: %s", result.Message)
	}
	if result.NextOffset != offset+1 {
		return &OffsetMismatchError{RequestedOffset: offset, ReplicaNextOffset: result.NextOffset}
	}
	return nil
}
