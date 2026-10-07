package runtime

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// topicDirT is storage.TopicDir for a topic name the test knows is valid.
func topicDirT(tb testing.TB, dataDir, topicName string) string {
	tb.Helper()
	dir, err := storage.TopicDir(dataDir, topicName)
	if err != nil {
		tb.Fatal(err)
	}
	return dir
}

// staleTopicDirT is storage.StaleTopicDir for a name the test knows is
// valid.
func staleTopicDirT(tb testing.TB, dataDir, topicName, id string) string {
	tb.Helper()
	dir, err := storage.StaleTopicDir(dataDir, topicName, id)
	if err != nil {
		tb.Fatal(err)
	}
	return dir
}

// topicPartitionDirT is storage.TopicPartitionDir for a topic name the
// test knows is valid.
func topicPartitionDirT(tb testing.TB, dataDir, topicName string, partition int) string {
	tb.Helper()
	dir, err := storage.TopicPartitionDir(dataDir, topicName, partition)
	if err != nil {
		tb.Fatal(err)
	}
	return dir
}
