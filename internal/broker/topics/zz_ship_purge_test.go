package topics

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// forgettingReleaser records the waiter releases and the cache forgets
// the manager asks of the messaging engine.
type forgettingReleaser struct {
	released  []string
	forgotten []string
}

func (r *forgettingReleaser) ReleaseTopicWaiters(name string) { r.released = append(r.released, name) }
func (r *forgettingReleaser) ForgetTopic(name string)         { r.forgotten = append(r.forgotten, name) }

// A purge that arrives late, for an incarnation the name has already
// been recreated past, leaves the live successor's directory, open logs
// and in-memory state alone (the retired hook does not run), but still
// drops what this node only caches about the name: its schemas and the
// engine's cached metadata, which the successor reloads on next use.
func TestStalePurgeKeepsSuccessorButDropsNameCaches(t *testing.T) {
	const oldID, liveID = "1111111111111111", "2222222222222222"
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, ID: liveID, Partitions: 1}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })
	rel := &forgettingReleaser{}
	manager.SetWaiterReleaser(rel)

	if _, err := manager.logs.Get(testTopicName, 0); err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	topicDir := storage.TopicDir(manager.dataDir, testTopicName)

	if err := manager.PurgeTopic(context.Background(), testTopicName, oldID); err != nil {
		t.Fatalf("PurgeTopic(stale id): %v", err)
	}

	if id, marked, err := storage.ReadTopicIncarnation(topicDir); err != nil || !marked || id != liveID {
		t.Fatalf("successor marker after a stale purge = (%q, %v, %v), want %q kept", id, marked, err, liveID)
	}
	if _, open := manager.logs.Peek(testTopicName, 0); !open {
		t.Fatal("a stale purge closed the successor's open log")
	}
	if len(rel.released) != 1 || rel.released[0] != testTopicName {
		t.Fatalf("waiter releases = %v, want only the one before the purge (the retired hook must not run for the successor)", rel.released)
	}
	if reg.lastDroppedTopic != testTopicName {
		t.Fatalf("schemas dropped for %q, want %q: the name's caches are dropped regardless", reg.lastDroppedTopic, testTopicName)
	}
	if len(rel.forgotten) != 1 || rel.forgotten[0] != testTopicName {
		t.Fatalf("engine cache forgets = %v, want one for %q", rel.forgotten, testTopicName)
	}
}
