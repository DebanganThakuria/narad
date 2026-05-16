package metastore

import (
	"context"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// Metastore is the broker's view of durable metadata.
type Metastore interface {
	CreateTopic(ctx context.Context, t topic.Topic) error
	UpdateTopic(ctx context.Context, t topic.Topic) error
	DeleteTopic(ctx context.Context, name string) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	ListTopics(ctx context.Context, opts ListOptions) ([]topic.Topic, string, error)

	PutSchema(ctx context.Context, topic string, version int, schema []byte) error
	GetSchema(ctx context.Context, topic string, version int) ([]byte, error)

	GetConsumerOffset(ctx context.Context, topic string, partition int) (int64, error)
	SetConsumerOffset(ctx context.Context, topic string, partition int, offset int64) error

	Close() error
}

// ListOptions controls pagination for ListTopics. PageToken is the
// first key of the next page — pass it back verbatim on the next call.
// Limit == 0 returns all topics in one shot.
type ListOptions struct {
	Limit     int
	PageToken string
}

// MemberStatus is the liveness state of a cluster pod.
type MemberStatus string

const (
	MemberAlive MemberStatus = "alive"
	MemberDead  MemberStatus = "dead"
)

// Member represents a narad pod registered in the cluster.
type Member struct {
	ID            string       `json:"id"`
	Addr          string       `json:"addr"` // host:port for intra-cluster RPCs
	Status        MemberStatus `json:"status"`
	LastHeartbeat int64        `json:"last_heartbeat"` // Unix seconds
}

// Assignment maps a single partition of a topic to its owner pod.
type Assignment struct {
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	OwnerID    string `json:"owner_id"`
	FollowerID string `json:"follower_id"`
}
