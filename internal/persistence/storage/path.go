package storage

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// TopicPartitionDir returns the directory holding one partition's log:
// <dataDir>/topics/<topic>/p<NNNNN>. The zero-padded partition number
// keeps lexicographic and numeric ordering identical.
func TopicPartitionDir(dataDir, topicName string, partition int) string {
	return filepath.Join(dataDir, "topics", topicName, fmt.Sprintf("p%05d", partition))
}

// ParsePartitionDirName reverses TopicPartitionDir's p<NNNNN> naming. It
// lives beside the formatter so the two cannot drift apart.
func ParsePartitionDirName(name string) (int, bool) {
	rest, ok := strings.CutPrefix(name, "p")
	if !ok || rest == "" {
		return 0, false
	}
	idx, err := strconv.Atoi(rest)
	if err != nil || idx < 0 {
		return 0, false
	}
	return idx, true
}

// ParseSegmentFileName reports the base offset a segment file name
// encodes, for callers that list a partition directory without opening
// its log.
func ParseSegmentFileName(name string) (int64, bool) {
	return parseSegmentFileName(name)
}
