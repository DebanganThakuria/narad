package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A topic name forms a path only when it is exactly one element under
// the topics directory. Topic creation already refuses anything else;
// this is the storage layer's own check, so a name that reached it by
// any other path cannot read, rename or remove outside <dataDir>/topics.
func TestTopicDirRefusesANameThatIsNotOnePathElement(t *testing.T) {
	dataDir := t.TempDir()
	for _, name := range []string{"", ".", "..", "../outside", "a/b", `a\b`, "/etc", "orders/../../x"} {
		if dir, err := TopicDir(dataDir, name); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("TopicDir(%q) = %q, %v; want ErrUnsafeTopicName", name, dir, err)
		}
		if dir, err := StaleTopicDir(dataDir, name, "0123456789abcdef"); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("StaleTopicDir(%q) = %q, %v; want ErrUnsafeTopicName", name, dir, err)
		}
		if _, _, err := ReadTopicIncarnationOf(dataDir, name); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("ReadTopicIncarnationOf(%q) = %v; want ErrUnsafeTopicName", name, err)
		}
	}
	if dir, err := StaleTopicDir(dataDir, "orders", "../../x"); !errors.Is(err, ErrUnsafeTopicName) {
		t.Errorf("StaleTopicDir with an id holding a separator = %q, %v; want ErrUnsafeTopicName", dir, err)
	}

	// Every name topic creation accepts still works, dots included.
	for _, name := range []string{"orders", "a..b", "x.stale-y", "v1.2_payments-captured"} {
		dir, err := TopicDir(dataDir, name)
		if err != nil || dir != filepath.Join(dataDir, "topics", name) {
			t.Errorf("TopicDir(%q) = %q, %v; want %q", name, dir, err, filepath.Join(dataDir, "topics", name))
		}
	}
}

// Quarantining under an unsafe name moves nothing: the directory the
// name would escape to is left exactly where it is.
func TestQuarantineTopicDirMovesNothingForAnUnsafeName(t *testing.T) {
	dataDir := t.TempDir()
	outside := filepath.Join(dataDir, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := QuarantineTopicDir(dataDir, "../outside", "0123456789abcdef"); !errors.Is(err, ErrUnsafeTopicName) {
		t.Fatalf("QuarantineTopicDir(../outside) = %v; want ErrUnsafeTopicName", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("the directory outside the topics directory was moved: %v", err)
	}
}

// A partition directory is built from the same topic name, so it gets
// the same check: a name that is not one path element forms no path at
// all, and every name topic creation accepts maps to the directory it
// always did.
func TestTopicPartitionDirRefusesANameThatIsNotOnePathElement(t *testing.T) {
	dataDir := t.TempDir()
	for _, name := range []string{"", ".", "..", "../x", "a/b", `a\b`, "/etc", "orders/../../x"} {
		if dir, err := TopicPartitionDir(dataDir, name, 0); !errors.Is(err, ErrUnsafeTopicName) {
			t.Errorf("TopicPartitionDir(%q) = %q, %v; want ErrUnsafeTopicName", name, dir, err)
		}
	}

	for _, name := range []string{"orders", "a..b", "x.stale-y", "v1.2_payments-captured", "..a", "a.."} {
		want := filepath.Join(dataDir, "topics", name, "p00007")
		dir, err := TopicPartitionDir(dataDir, name, 7)
		if err != nil || dir != want {
			t.Errorf("TopicPartitionDir(%q, 7) = %q, %v; want %q", name, dir, err, want)
		}
	}
}

// topicPartitionDirT is TopicPartitionDir for a topic name the test
// knows is valid.
func topicPartitionDirT(tb testing.TB, dataDir, topicName string, partition int) string {
	tb.Helper()
	dir, err := TopicPartitionDir(dataDir, topicName, partition)
	if err != nil {
		tb.Fatal(err)
	}
	return dir
}
