package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestRPCServerWaitTopicDeletedLocally(t *testing.T) {
	store := newTestStore(t)
	s := &RPCServer{store: store}

	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Topic still present -> must NOT report deleted (returns false after
	// the timeout). This is the guard that stops a purge from running
	// while the local replica still shows the topic live.
	if s.waitTopicDeletedLocally("orders", 200*time.Millisecond) {
		t.Fatal("waitTopicDeletedLocally(existing) = true, want false")
	}

	if err := store.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	// Once the local replica reflects the delete, returns true promptly.
	if !s.waitTopicDeletedLocally("orders", 2*time.Second) {
		t.Fatal("waitTopicDeletedLocally(deleted) = false, want true")
	}
}
