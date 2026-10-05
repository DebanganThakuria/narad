package topics

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// A topic's directory is named after it, and on a case-insensitive
// filesystem (APFS, the macOS default; NTFS; Docker Desktop bind
// mounts) "Orders" and "orders" are one directory: opening the second
// quarantined the first's data. A create whose name differs from an
// existing topic's only in letter case is refused with 409, whatever
// filesystem the test runs on. Existing names are untouched.
func TestCreateRefusesNameThatDiffersOnlyInCase(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3}
	m := newTestManager(t, ms, nil)

	_, err := m.CreateTopic(context.Background(), CreateOpts{Name: "Orders"})
	if !errors.Is(err, errs.ErrTopicAlreadyExists) {
		t.Fatalf("create Orders next to orders = %v, want ErrTopicAlreadyExists", err)
	}
	if !strings.Contains(err.Error(), `"orders"`) {
		t.Fatalf("error %q does not name the existing topic", err)
	}
	if _, ok := ms.topics["Orders"]; ok {
		t.Fatal("Orders was created")
	}
	if _, err := m.CreateTopic(context.Background(), CreateOpts{Name: "orders-eu"}); err != nil {
		t.Fatalf("an unrelated name was refused: %v", err)
	}
}

// slowCreateMetastore makes the window between a create's name checks
// and its write wide, and is safe for concurrent use.
type slowCreateMetastore struct {
	*fakeMetastore
	mu sync.Mutex
}

func (f *slowCreateMetastore) CreateTopic(ctx context.Context, t topic.Topic) error {
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fakeMetastore.CreateTopic(ctx, t)
}

func (f *slowCreateMetastore) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fakeMetastore.ListTopics(ctx, opts)
}

func (f *slowCreateMetastore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fakeMetastore.GetTopic(ctx, name)
}

// Two creates whose names differ only in case take the same name lock,
// so the second sees the first and is refused: exactly one lands.
func TestCreatesDifferingOnlyInCaseSerialize(t *testing.T) {
	ms := &slowCreateMetastore{fakeMetastore: newFakeMetastore()}
	m := newTestManagerForMetastore(t, ms, nil, nil, "")

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i, name := range []string{"Orders", "orders"} {
		wg.Go(func() {
			_, results[i] = m.CreateTopic(context.Background(), CreateOpts{Name: name})
		})
	}
	wg.Wait()
	created := 0
	for _, err := range results {
		switch {
		case err == nil:
			created++
		case !errors.Is(err, errs.ErrTopicAlreadyExists):
			t.Fatalf("create = %v, want success or ErrTopicAlreadyExists", err)
		}
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if created != 1 || len(ms.topics) != 1 {
		t.Fatalf("%d creates succeeded, topics = %v; want exactly one", created, ms.topics)
	}
}

// New names are capped at 200 bytes so every name derived from them
// fits the filesystem's 255-byte limit (master accepted 255, and the
// quarantine directory and fan-out cursor of such a topic could never
// be created). Topics created before the cap keep working.
func TestNewTopicNamesAreCappedAt200Bytes(t *testing.T) {
	ms := newFakeMetastore()
	m := newTestManager(t, ms, nil)
	ctx := context.Background()

	if _, err := m.CreateTopic(ctx, CreateOpts{Name: strings.Repeat("a", 201)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("create with a 201-byte name = %v, want ErrInvalid", err)
	}
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: strings.Repeat("b", 255)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("create with a 255-byte name = %v, want ErrInvalid", err)
	}
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: strings.Repeat("c", 200)}); err != nil {
		t.Fatalf("create with a 200-byte name: %v", err)
	}

	old := strings.Repeat("d", 255)
	ms.topics[old] = topic.Topic{Name: old, ID: "0000000000000001", Partitions: 3, RetentionMs: 3_600_000}
	if _, err := m.UpdateTopicRetention(ctx, old, 7_200_000); err != nil {
		t.Fatalf("alter of a topic named before the cap: %v", err)
	}
	if err := m.DeleteTopic(ctx, old); err != nil {
		t.Fatalf("delete of a topic named before the cap: %v", err)
	}
}

// The cap leaves room for every file name storage derives from a topic
// name: the real quarantine rename, twice so the collision suffix is
// added, and the first fan-out cursor write (an atomic temp file) for a
// child with a name at the cap.
func TestDerivedFileNamesFitAtTheNameCap(t *testing.T) {
	dataDir := t.TempDir()
	name := strings.Repeat("n", MaxNewTopicNameBytes)
	if err := validateNewTopicName(name); err != nil {
		t.Fatalf("a name at the cap is refused: %v", err)
	}
	const id = "0123456789abcdef"
	for range 2 {
		if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, name), id); err != nil {
			t.Fatalf("write the incarnation marker: %v", err)
		}
		stale, err := storage.QuarantineTopicDir(dataDir, name, id)
		if err != nil {
			t.Fatalf("quarantine a %d-byte topic name: %v", len(name), err)
		}
		if _, err := os.Stat(stale); err != nil {
			t.Fatalf("quarantined directory: %v", err)
		}
	}

	partitionDir := storage.TopicPartitionDir(dataDir, "parent", 0)
	if err := storage.WriteFanoutCursorCreating(partitionDir, name, storage.FanoutCursor{Epoch: "e1", NextOffset: 0}); err != nil {
		t.Fatalf("first fan-out cursor write for a %d-byte child name: %v", len(name), err)
	}
	if _, ok, err := storage.ReadFanoutCursor(partitionDir, name); err != nil || !ok {
		t.Fatalf("read the cursor back: ok=%v err=%v", ok, err)
	}
}

// A topic named before the cap whose fan-out cursor file name would not
// fit cannot become a fan-out child: the attach would be accepted and
// fan-out to it could never start. It can still be a parent.
func TestLongNamedTopicCannotBecomeAFanoutChild(t *testing.T) {
	ms := newFakeMetastore()
	long := strings.Repeat("x", 240)
	ms.topics[long] = topic.Topic{Name: long, ID: "0000000000000001", Partitions: 3, RetentionMs: 3_600_000}
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000002", Partitions: 3, RetentionMs: 3_600_000}
	m := newTestManager(t, ms, nil)
	ctx := context.Background()

	if err := m.AttachChild(ctx, "orders", long, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("attach of a 240-byte child = %v, want ErrInvalid", err)
	}
	if c := ms.topics[long]; c.Parent != "" {
		t.Fatalf("the refused attach linked the child: %+v", c)
	}
	if err := m.AttachChild(ctx, long, "orders", 0); err != nil {
		t.Fatalf("a long-named topic as a parent: %v", err)
	}
}

// The documented threshold: a name of up to 230 bytes can be a fan-out
// child (its cursor file, temp suffix included, then fits the 255-byte
// file name limit, as a real write shows), and a 231-byte one cannot.
// Names of 201 to 230 bytes, which only an earlier release could
// create, still attach.
func TestFanoutChildNamesUpTo230BytesAttach(t *testing.T) {
	longest := strings.Repeat("c", 230)
	if err := validateFanoutChildName(longest); err != nil {
		t.Fatalf("a 230-byte child name is refused: %v", err)
	}
	if err := validateFanoutChildName(longest + "c"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a 231-byte child name: err = %v, want ErrInvalid", err)
	}
	partitionDir := storage.TopicPartitionDir(t.TempDir(), "parent", 0)
	if err := storage.WriteFanoutCursorCreating(partitionDir, longest, storage.FanoutCursor{Epoch: "e1", NextOffset: 0}); err != nil {
		t.Fatalf("fan-out cursor write for a 230-byte child name: %v", err)
	}

	ms := newFakeMetastore()
	legacy := strings.Repeat("l", 210)
	ms.topics[legacy] = topic.Topic{Name: legacy, ID: "0000000000000001", Partitions: 3, RetentionMs: 3_600_000}
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000002", Partitions: 3, RetentionMs: 3_600_000}
	m := newTestManager(t, ms, nil)
	if err := m.AttachChild(context.Background(), "orders", legacy, 0); err != nil {
		t.Fatalf("attach of a 210-byte child named by an earlier release: %v", err)
	}
}

// A create or a partition increase while every live member is being
// decommissioned is refused with a 503 that says why and what to do,
// before anything is committed: the new partitions could have no owner,
// and they are never placed on a draining member instead. Once a member
// that is not draining is alive, both go through.
func TestCreateAndPartitionIncreaseRefusedWhileEveryMemberDrains(t *testing.T) {
	m, store := newStoreManager(t)
	m.assigner = store
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "narad-0", Addr: "narad-0:7943", Status: metastore.MemberAlive, LastHeartbeat: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: "orders", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMemberDraining(ctx, "narad-0", true); err != nil {
		t.Fatal(err)
	}

	refused := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, errs.ErrUnavailable) || !strings.Contains(err.Error(), "abort a decommission or add a node") {
			t.Fatalf("%s with every live member draining: err = %v, want a 503 that says to abort a decommission or add a node", what, err)
		}
	}
	_, err := m.CreateTopic(ctx, CreateOpts{Name: "late", Partitions: 3})
	refused("create", err)
	if _, err := store.GetTopic(ctx, "late"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("refused create left the topic behind: %v", err)
	}
	_, err = m.IncreaseTopicPartitions(ctx, "orders", 6)
	refused("partition increase", err)
	if got, err := store.GetTopic(ctx, "orders"); err != nil || got.Partitions != 3 {
		t.Fatalf("refused increase: orders has %d partitions (err %v), want 3", got.Partitions, err)
	}
	if got, _ := store.ListAssignments("orders"); len(got) != 3 {
		t.Fatalf("orders has %d assignments, want its 3", len(got))
	}

	if err := store.SetMemberDraining(ctx, "narad-0", false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic(ctx, CreateOpts{Name: "late", Partitions: 3}); err != nil {
		t.Fatalf("create once a member is placeable: %v", err)
	}
	if _, err := m.IncreaseTopicPartitions(ctx, "orders", 6); err != nil {
		t.Fatalf("partition increase once a member is placeable: %v", err)
	}
	if got, _ := store.ListAssignments("orders"); len(got) != 6 {
		t.Fatalf("orders has %d assignments after the increase, want 6", len(got))
	}
}
