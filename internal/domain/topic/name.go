package topic

import (
	"errors"
	"fmt"
	"regexp"
)

// namePattern is the set of allowed topic names. Restricting to a
// single path-safe segment is load-bearing: the name becomes a directory
// under dataDir/topics, so "/" would nest (and the startup orphan sweep
// would delete the nested dirs), ".." would resolve the topic dir to the
// data dir itself, and "." to the topics root, either of which would
// make DeleteTopic/PurgeTopic os.RemoveAll far more than one topic. The
// same rule makes a name one safe path segment on another cluster's
// API, which is what a remote child's target topic relies on.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

// ValidateName rejects names that are unsafe as on-disk directory
// names or as a URL path segment. "." and ".." match the allowed
// character set but are path traversals, so they are rejected
// explicitly.
func ValidateName(name string) error {
	if name == "" {
		return errors.New("name required")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("topic name must not be %q", name)
	}
	if !namePattern.MatchString(name) {
		return fmt.Errorf("topic name must match %s", namePattern)
	}
	return nil
}
