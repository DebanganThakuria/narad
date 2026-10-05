package topics

// Topic name limits (audit L9 and M6).
//
// Length. A topic name is a directory name, and storage derives other
// file names from it by adding a prefix or suffix: the quarantine
// directory "<name>.stale-<16 hex>" (storage.StaleTopicDir, plus the
// ".<n>" storage.QuarantineTopicDir adds on a collision) and the fan-out
// cursor file "fanout-<child>.offset" plus the random suffix of its
// atomic-write temp file. Names up to 255 bytes were accepted, so a
// derived name could exceed the filesystem's NAME_MAX (255 bytes on
// ext4, XFS and APFS): the first cursor write for a long-named child
// failed with "file name too long" and fan-out to it never started, and
// a long-named topic's directory could never be quarantined. New names
// are capped at MaxNewTopicNameBytes, which leaves room for every
// derived name (create_test.go runs the real storage helpers at the
// cap). Existing longer names keep working for every operation, except
// that one over maxFanoutChildNameBytes (230) cannot become a fan-out
// child.
//
// Letter case. On a case-insensitive filesystem (APFS, the macOS
// default; NTFS; Docker Desktop bind mounts) "Orders" and "orders" are
// one directory, and opening the second quarantined the first's data.
// A new name that equals an existing topic's except for letter case is
// refused, and the per-name lock folds case so two such creates
// serialize. Existing pairs keep working.

import (
	"context"
	"fmt"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// MaxNewTopicNameBytes is the longest name a new topic may have. Topics
// created before the cap with longer names (up to 255 bytes) keep
// working.
const MaxNewTopicNameBytes = 200

// nameMaxBytes is NAME_MAX, the longest single path component the
// supported filesystems accept.
const nameMaxBytes = 255

// The shapes of the file names storage derives from a topic name.
const (
	// staleDirExtraBytes: ".stale-" plus a 16-hex incarnation ID.
	staleDirExtraBytes = len(".stale-") + 16
	// staleDirCollisionBytes: the ".<n>" storage.QuarantineTopicDir
	// appends when a quarantine for the same ID already exists (n is an
	// int, at most 10 digits in practice).
	staleDirCollisionBytes = 1 + 10
	// fanoutCursorExtraBytes: "fanout-" and ".offset" around the child
	// name, plus the "." and up to 10 random digits os.CreateTemp adds
	// for the atomic write.
	fanoutCursorExtraBytes = len("fanout-") + len(".offset") + 1 + 10
)

// maxFanoutChildNameBytes is the longest name a fan-out child may have:
// its cursor file name, temp suffix included, then just fits NAME_MAX.
const maxFanoutChildNameBytes = nameMaxBytes - fanoutCursorExtraBytes

// validateNewTopicName is validateTopicName plus the length cap that
// applies to a name being created.
func validateNewTopicName(name string) error {
	if err := validateTopicName(name); err != nil {
		return err
	}
	if len(name) > MaxNewTopicNameBytes {
		return fmt.Errorf("%w: topic name is %d bytes; new topic names are limited to %d bytes",
			ErrInvalid, len(name), MaxNewTopicNameBytes)
	}
	if n := len(name) + max(staleDirExtraBytes+staleDirCollisionBytes, fanoutCursorExtraBytes); n > nameMaxBytes {
		return fmt.Errorf("%w: topic name %q derives a %d-byte file name, over the %d-byte limit",
			ErrInvalid, name, n, nameMaxBytes)
	}
	return nil
}

// validateFanoutChildName refuses to link a child whose fan-out cursor
// file name would not fit NAME_MAX: the link would be accepted but the
// fan-out to it could never start. Only topics named before the length
// cap can fail it.
func validateFanoutChildName(child string) error {
	if len(child) > maxFanoutChildNameBytes {
		n := len(child) + fanoutCursorExtraBytes
		return fmt.Errorf("%w: child topic name is %d bytes; its fan-out cursor file name (%d bytes) would exceed the %d-byte file name limit, so fan-out to it could never start; use a child named in at most %d bytes",
			ErrInvalid, len(child), n, nameMaxBytes, MaxNewTopicNameBytes)
	}
	return nil
}

// topicNameFolder is the metastore capability behind the case-fold
// name check (implemented by *metastore.Store).
type topicNameFolder interface {
	TopicNameFoldConflict(name string) (existing string, found bool, err error)
}

// foldConflictPageSize is the ListTopics page size of the fallback scan.
const foldConflictPageSize = 1000

// checkNameFold refuses a new name that equals an existing topic's
// except for letter case. The caller holds the name's lock (which folds
// case) and has run the leader barrier. A metastore without the
// capability is scanned through ListTopics.
func (m *Manager) checkNameFold(ctx context.Context, name string) error {
	existing, found, err := m.findNameFold(ctx, name)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return fmt.Errorf("%w: %q differs only in letter case from the existing topic %q; on a case-insensitive filesystem both would share one directory, so choose another name",
		ErrAlreadyExists, name, existing)
}

func (m *Manager) findNameFold(ctx context.Context, name string) (string, bool, error) {
	if folder, ok := m.metastore.(topicNameFolder); ok {
		return folder.TopicNameFoldConflict(name)
	}
	opts := metastore.ListOptions{Limit: foldConflictPageSize}
	for {
		page, next, err := m.metastore.ListTopics(ctx, opts)
		if err != nil {
			return "", false, err
		}
		for _, t := range page {
			if t.Name != name && strings.EqualFold(t.Name, name) {
				return t.Name, true, nil
			}
		}
		if next == "" {
			return "", false, nil
		}
		opts.PageToken = next
	}
}
