package storage

import (
	"fmt"
	"path/filepath"
)

// TopicPartitionDir returns the directory holding one partition's log:
// <dataDir>/topics/<topic>/p<NNNNN>. The zero-padded partition number
// keeps lexicographic and numeric ordering identical.
func TopicPartitionDir(dataDir, topicName string, partition int) string {
	return filepath.Join(dataDir, "topics", topicName, fmt.Sprintf("p%05d", partition))
}
