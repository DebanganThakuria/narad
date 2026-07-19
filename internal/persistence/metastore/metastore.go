package metastore

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrNotFound and ErrAlreadyExists are aliases of the canonical
// sentinels in internal/errs.
var (
	ErrNotFound      = errs.ErrNotFound
	ErrAlreadyExists = errs.ErrAlreadyExists
)

// ErrRootProtected is returned when a write would delete the root admin
// or otherwise violate a root-account invariant enforced in the FSM.
var ErrRootProtected = errors.New("metastore: root account is protected")

// Metastore is the broker's view of durable metadata.
type Metastore interface {
	CreateTopic(ctx context.Context, t topic.Topic) error
	UpdateTopic(ctx context.Context, t topic.Topic) error
	DeleteTopic(ctx context.Context, name string) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	ListTopics(ctx context.Context, opts ListOptions) ([]topic.Topic, string, error)

	AttachChild(ctx context.Context, parent, child string, delayMs int64) error
	DetachChild(ctx context.Context, parent, child string) error

	PutSchema(ctx context.Context, topic string, version int, schema []byte) error
	GetSchema(ctx context.Context, topic string, version int) ([]byte, error)

	LeaderAddr() string
	GetMember(podID string) (Member, error)

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

// The two liveness states a member can be in. There is no intermediate
// state: a member is routable until it is marked dead.
const (
	MemberAlive MemberStatus = "alive"
	MemberDead  MemberStatus = "dead"
)

// Member represents a narad pod registered in the cluster.
type Member struct {
	ID            string       `json:"id"`
	Addr          string       `json:"addr"` // host:port for HTTP peer RPCs
	ClusterAddr   string       `json:"cluster_addr,omitempty"`
	Status        MemberStatus `json:"status"`
	LastHeartbeat int64        `json:"last_heartbeat"` // Unix seconds
}

// Assignment maps a single partition of a topic to its owner pod.
// Narad has no follower replication, so each partition has exactly one
// owner.
type Assignment struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	OwnerID   string `json:"owner_id"`
	// TargetID names the node this partition is being moved to during a
	// rebalance/decommission. Empty when the assignment is stable. The
	// leader (controller) sets it as desired state; the destination node
	// copies the partition and proposes the atomic flip when caught up.
	TargetID string `json:"target_id,omitempty"`
}
