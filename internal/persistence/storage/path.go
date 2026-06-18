package storage

import (
	"fmt"
	"path/filepath"
)

func TopicPartitionDir(dataDir, topicName string, partition int) string {
	return filepath.Join(dataDir, "topics", topicName, fmt.Sprintf("p%05d", partition))
}
