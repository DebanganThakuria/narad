package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type StoreRecovery struct {
	selfID string
	store  *metastore.Store
	logs   *runtime.Logs
	client *http.Client
}

type replicaReadResponse struct {
	Payload []byte `json:"payload"`
}

type replicaWriteRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Payload   []byte `json:"payload"`
	LeaderID  string `json:"leader_id"`
}

func NewStoreRecovery(selfID string, store *metastore.Store, logs *runtime.Logs, client *http.Client) StoreRecovery {
	if client == nil {
		client = http.DefaultClient
	}
	return StoreRecovery{selfID: selfID, store: store, logs: logs, client: client}
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
	start, err := r.findFollowerNextOffset(ctx, followerAddr, topicName, partition)
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

func (r StoreRecovery) findFollowerNextOffset(ctx context.Context, addr, topicName string, partition int) (int64, error) {
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
	resp, err := r.client.Do(req)
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
	body, err := json.Marshal(replicaWriteRequest{
		Topic:     topicName,
		Partition: partition,
		Offset:    offset,
		Payload:   payload,
		LeaderID:  r.selfID,
	})
	if err != nil {
		return fmt.Errorf("marshal replicate request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, replicateEndpoint(addr), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build replicate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("send replicate request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("replicate request failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
