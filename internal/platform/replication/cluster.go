package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type memberLister interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	GetMember(podID string) (metastore.Member, error)
}

type Cluster struct {
	selfID string
	store  memberLister
	client *http.Client
}

type replicateRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Payload   []byte `json:"payload"`
	LeaderID  string `json:"leader_id"`
}

func NewCluster(selfID string, store memberLister, client *http.Client) Cluster {
	if client == nil {
		client = http.DefaultClient
	}
	return Cluster{selfID: selfID, store: store, client: client}
}

func (c Cluster) Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error {
	assignment, err := c.store.GetAssignment(topic, partition)
	if err != nil {
		return fmt.Errorf("lookup assignment: %w", err)
	}
	if assignment.FollowerID == "" || assignment.FollowerID == c.selfID {
		return nil
	}

	follower, err := c.store.GetMember(assignment.FollowerID)
	if err != nil {
		return fmt.Errorf("lookup follower: %w", err)
	}
	if follower.Status == metastore.MemberDead {
		return fmt.Errorf("follower %s is dead", follower.ID)
	}

	body, err := json.Marshal(replicateRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Payload:   payload,
		LeaderID:  c.selfID,
	})
	if err != nil {
		return fmt.Errorf("marshal replicate request: %w", err)
	}

	// TODO Why not https?
	url := "http://" + strings.TrimPrefix(follower.Addr, "http://") + "/internal/v1/replicate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build replicate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("send replicate request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("replicate request failed with status %d", resp.StatusCode)
	}
	return nil
}

var _ Replicator = Cluster{}
