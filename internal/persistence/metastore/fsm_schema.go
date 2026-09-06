package metastore

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// latestSchemaVersion returns the highest persisted schema version for
// the topic, or 0 when it has none. Keys are "<topic>:<version>" and
// sort lexically, so the maximum is taken explicitly rather than read
// off the last key.
func latestSchemaVersion(tx *bolt.Tx, topicName string) (int, error) {
	latest := 0
	prefix := []byte(topicName + ":")
	c := tx.Bucket(bucketSchemas).Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		version, err := strconv.Atoi(strings.TrimPrefix(string(k), string(prefix)))
		if err != nil {
			return 0, fmt.Errorf("metastore: malformed schema key %q: %w", k, err)
		}
		if version > latest {
			latest = version
		}
	}
	return latest, nil
}

// checkNextSchemaVersion enforces append-only schema history: version
// must be exactly latest+1. A version at or below the latest is one a
// stale proposer is trying to overwrite (ErrAlreadyExists, so the
// proposer re-reads the history and retries); a version beyond
// latest+1 would leave a hole that PersistedHistory's scan would stop
// at (ErrInvalidArgument).
func checkNextSchemaVersion(tx *bolt.Tx, topicName string, version int) error {
	latest, err := latestSchemaVersion(tx, topicName)
	if err != nil {
		return err
	}
	switch {
	case version <= latest:
		return fmt.Errorf("%w: schema version %d for %q (latest is %d); schema history is append-only",
			errs.ErrAlreadyExists, version, topicName, latest)
	case version != latest+1:
		return fmt.Errorf("%w: schema version %d for %q skips versions (latest is %d)",
			errs.ErrInvalidArgument, version, topicName, latest)
	}
	return nil
}
