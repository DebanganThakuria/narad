package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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
	for offset := log.NextOffset(); ; offset++ {
		payload, found, err := r.fetchReplicaRecord(ctx, follower.Addr, assignment.Topic, assignment.Partition, offset)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		appended, err := log.Append(payload)
		if err != nil {
			return fmt.Errorf("append recovered record: %w", err)
		}
		if appended != offset {
			return fmt.Errorf("repair offset mismatch: got %d want %d", appended, offset)
		}
	}
}

func (r StoreRecovery) fetchReplicaRecord(ctx context.Context, addr, topicName string, partition int, offset int64) ([]byte, bool, error) {
	endpoint := "http://" + strings.TrimPrefix(addr, "http://") + "/internal/v1/replicate"
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, false, fmt.Errorf("parse replica read url: %w", err)
	}
	q := parsed.Query()
	q.Set("topic", topicName)
	q.Set("partition", fmt.Sprintf("%d", partition))
	q.Set("offset", fmt.Sprintf("%d", offset))
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
