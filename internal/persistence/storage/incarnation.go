package storage

// The topic incarnation marker. Topic directories are keyed by NAME
// (topics/<name>/pNNNNN), but a name is reused: a topic can be deleted
// and recreated under the same name, and a node that missed the purge
// (it was down, or the purge lost a race with the recreate) is left
// with the OLD incarnation's segments, high-watermark and consumer
// offset sitting exactly where the NEW topic's partition logs open.
// Served as-is, the recreated topic resurrects the deleted one's data.
//
// The marker breaks the tie: `topics/<name>/incarnation` holds the ID
// of the topic incarnation the directory belongs to (topic.Topic.ID).
// It is stamped the first time a node opens a partition log for the
// topic, or installs a moved partition, and every open compares it
// with the metastore record before serving the directory. A mismatch
// means the directory is a deleted incarnation's leftover: it is set
// aside as `topics/<name>.stale-<oldid>` (quarantined, never served,
// reclaimed by the orphan sweep once the leader confirms the ID is
// gone) and a fresh directory is opened for the current ID.
//
// Upgrade: a directory with no marker predates markers and is adopted
// by the first open (stamped with the current ID). Older binaries never
// read the marker and ignore the file.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// IncarnationMarkerFileName is the marker file inside a topic
// directory, next to the partition directories.
const IncarnationMarkerFileName = "incarnation"

// StaleTopicDirSuffix separates a quarantined topic directory's name
// from the incarnation ID it belonged to: topics/<name>.stale-<id>.
const StaleTopicDirSuffix = ".stale-"

// TopicDir returns the directory holding one topic's partitions:
// <dataDir>/topics/<topic>.
func TopicDir(dataDir, topicName string) string {
	return filepath.Join(dataDir, "topics", topicName)
}

// StaleTopicDir returns the quarantine path for topicName's directory
// when it belonged to the incarnation id: <dataDir>/topics/<topic>.stale-<id>.
func StaleTopicDir(dataDir, topicName, id string) string {
	return filepath.Join(dataDir, "topics", topicName+StaleTopicDirSuffix+id)
}

// ParseStaleTopicDirName splits a quarantined directory's base name
// into the topic name and the incarnation ID. ok=false when the name
// does not carry the suffix. The suffix alone is not proof (a topic may
// legitimately be named "x.stale-y"); callers confirm by reading the
// directory's marker, which a quarantined directory always carries and
// which equals the parsed ID.
func ParseStaleTopicDirName(base string) (topicName, id string, ok bool) {
	i := strings.LastIndex(base, StaleTopicDirSuffix)
	if i <= 0 {
		return "", "", false
	}
	topicName, id = base[:i], base[i+len(StaleTopicDirSuffix):]
	if id == "" {
		return "", "", false
	}
	return topicName, id, true
}

// ReadTopicIncarnation reads the marker in topicDir. ok=false (nil
// error) when the directory has no marker or the marker is empty (a
// crash between create and write leaves an empty file, which reads as
// "unmarked" so the next open adopts the directory again).
func ReadTopicIncarnation(topicDir string) (id string, ok bool, err error) {
	buf, err := os.ReadFile(filepath.Join(topicDir, IncarnationMarkerFileName))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("storage: read incarnation marker: %w", err)
	}
	id = strings.TrimSpace(string(buf))
	if id == "" {
		return "", false, nil
	}
	return id, true, nil
}

// WriteTopicIncarnation stamps topicDir with id, creating the directory
// if needed. The write is atomic (temp file, fsync, rename) so a crash
// never leaves a torn marker; the directory is fsynced so the rename is
// durable before the caller writes any partition data under it.
func WriteTopicIncarnation(topicDir, id string) error {
	if id == "" {
		return errors.New("storage: incarnation id required")
	}
	if err := os.MkdirAll(topicDir, dataDirMode); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(topicDir, IncarnationMarkerFileName+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.WriteString(id); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(topicDir, IncarnationMarkerFileName)); err != nil {
		return err
	}
	return syncDir(topicDir)
}

// QuarantineTopicDir renames topicName's directory to its stale path
// for the incarnation id and returns that path. If a quarantine for the
// same ID already exists (a directory quarantined twice can only happen
// if something recreated the original in between), the older one is
// kept and the new one gets a numeric suffix so nothing is overwritten.
func QuarantineTopicDir(dataDir, topicName, id string) (string, error) {
	dir := TopicDir(dataDir, topicName)
	target := StaleTopicDir(dataDir, topicName, id)
	for n := 1; ; n++ {
		if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return "", err
		}
		target = fmt.Sprintf("%s.%d", StaleTopicDir(dataDir, topicName, id), n)
	}
	if err := os.Rename(dir, target); err != nil {
		return "", err
	}
	return target, syncDir(filepath.Dir(dir))
}
