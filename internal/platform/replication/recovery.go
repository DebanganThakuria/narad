package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

type replicaReadResponse struct {
	Payload []byte `json:"payload"`
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

func findReplicaNextOffset(ctx context.Context, client *http.Client, addr, topicName string, partition int, upperBound int64) (int64, error) {
	if upperBound <= 0 {
		return 0, nil
	}
	low, high := int64(0), upperBound
	for low < high {
		mid := low + (high-low)/2
		_, found, err := fetchReplicaRecord(ctx, client, addr, topicName, partition, mid, false)
		if err != nil {
			return 0, err
		}
		if found {
			low = mid + 1
			continue
		}
		high = mid
	}
	return low, nil
}

func fetchReplicaRecord(ctx context.Context, client *http.Client, addr, topicName string, partition int, offset int64, committedOnly bool) ([]byte, bool, error) {
	if client == nil {
		client = http.DefaultClient
	}
	parsed, err := url.Parse(replicateEndpoint(addr))
	if err != nil {
		return nil, false, fmt.Errorf("parse replica read url: %w", err)
	}
	q := parsed.Query()
	q.Set("topic", topicName)
	q.Set("partition", fmt.Sprintf("%d", partition))
	q.Set("offset", fmt.Sprintf("%d", offset))
	if committedOnly {
		q.Set("committed", "true")
	}
	parsed.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, false, fmt.Errorf("build replica read request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("send replica read request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("replica read failed with status %d: %s", resp.StatusCode, string(body))
	}
	var out replicaReadResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false, fmt.Errorf("decode replica read response: %w", err)
	}
	if !json.Valid(out.Payload) {
		return nil, false, fmt.Errorf("replica payload is not valid json")
	}
	return out.Payload, true, nil
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
