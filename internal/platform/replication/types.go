package replication

import (
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type memberLister interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	GetMember(podID string) (metastore.Member, error)
}

type stageObserver interface {
	ObserveHotPathStage(component, operation, stage, outcome string, duration time.Duration)
}

type OffsetMismatchError struct {
	RequestedOffset   int64
	ReplicaNextOffset int64
}

func (e *OffsetMismatchError) Error() string {
	return fmt.Sprintf("replicate offset mismatch: replica_next_offset=%d requested_offset=%d", e.ReplicaNextOffset, e.RequestedOffset)
}

func observeOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
