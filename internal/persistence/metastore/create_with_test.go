package metastore_test

// A topic create is one Raft entry once every member applies the entry
// type that carries it: the record, its first schema version and its
// fan-out parent link commit together. These tests drive the real topic
// Manager over real Raft stores whose members report this release.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// registerCurrentMembers registers alive members that report this
// release's Raft entry types, so the leader may propose every type it
// knows.
func registerCurrentMembers(t *testing.T, s *metastore.Store, ids ...string) {
	t.Helper()
	for _, id := range ids {
		m := metastore.Member{
			ID: id, Addr: id + ":7942", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix(),
			Build: "narad test", EntryTypes: metastore.MaxEntryType,
		}
		if err := s.RegisterMember(context.Background(), m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
}

// topicManager builds a real topic Manager over ms.
func topicManager(t *testing.T, ms metastore.Metastore, assigner topics.PartitionAssigner) *topics.Manager {
	t.Helper()
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{}, ms, nil)
	return topics.NewManager(dataDir, ms, assigner, schema.NewJSONSchema(),
		consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: 2, MaxAckedAhead: 2}, nil
		}, nil),
		logs,
		topics.Config{
			DefaultPartitions:                3,
			MaxPartitions:                    64,
			DefaultRetentionMs:               3_600_000,
			DefaultVisibilityTimeoutMs:       30_000,
			DefaultMaxInFlightPerPartition:   10,
			DefaultMaxAckedAheadPerPartition: 10,
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")
}

// reserveAddr returns a free loopback address: Raft peers must know each
// other's addresses up front, so ":0" cannot be used.
func reserveAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// raftCluster starts n bootstrapped Raft voters, waits for a leader that
// has committed a write in its term, and registers a member record that
// reports this release for every voter.
func raftCluster(t *testing.T, n int) []*metastore.Store {
	t.Helper()
	base := t.TempDir()
	ids := make([]string, n)
	addrs := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("rc-%d", i+1)
		addrs[i] = reserveAddr(t)
	}
	stores := make([]*metastore.Store, n)
	for i := range ids {
		var peers []metastore.Peer
		for j := range ids {
			if j != i {
				peers = append(peers, metastore.Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		s, err := metastore.New(metastore.Config{
			NodeID: ids[i], DataDir: filepath.Join(base, ids[i]),
			BindAddr: addrs[i], AdvertiseAddr: addrs[i], Peers: peers,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", ids[i], err)
		}
		stores[i] = s
		t.Cleanup(func() { _ = s.Close() })
	}
	leader := waitForLeaderStore(t, stores)
	registerCurrentMembers(t, leader, ids...)
	return stores
}

// leaderOtherThan waits for a store other than not to lead.
func leaderOtherThan(t *testing.T, stores []*metastore.Store, not *metastore.Store) *metastore.Store {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			if s != not && s.IsLeader() {
				return s
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no other store took the lead")
	return nil
}

// stepDownAfterCommit is the leader's store, handing its leadership to
// another voter right after the target topic's create has committed its
// first entry: after the record in a create of several entries, after
// the only entry in a single-entry create (whose partition placement
// comes next).
type stepDownAfterCommit struct {
	*metastore.Store
	t      *testing.T
	target string
	done   bool
}

func (s *stepDownAfterCommit) stepDown(name string) {
	if name != s.target || s.done {
		return
	}
	s.done = true
	if err := s.Store.TransferLeadership(); err != nil {
		s.t.Logf("TransferLeadership: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Store.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *stepDownAfterCommit) CreateTopic(ctx context.Context, tp topic.Topic) error {
	if err := s.Store.CreateTopic(ctx, tp); err != nil {
		return err
	}
	s.stepDown(tp.Name)
	return nil
}

func (s *stepDownAfterCommit) AssignNewPartitions(ctx context.Context, name string, from, to int) error {
	s.stepDown(name)
	return s.Store.AssignNewPartitions(ctx, name, from, to)
}

// A leader change right after a create commits must not leave the topic
// without the schema it was created with: the client's retry on the new
// leader answers 409, and that is the truth, because every replica
// holds the topic with schema version 1.
func TestCreateWithSchemaSurvivesLeaderChange(t *testing.T) {
	ctx := context.Background()
	stores := raftCluster(t, 3)
	leader := leaderOtherThan(t, stores, nil)
	wrapped := &stepDownAfterCommit{Store: leader, t: t, target: "orders"}
	m := topicManager(t, wrapped, wrapped)
	sch := []byte(`{"type":"object","required":["id"]}`)

	_, firstErr := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Schema: sch})
	t.Logf("create on the leader that stepped down: err=%v", firstErr)

	newLeader := leaderOtherThan(t, stores, leader)
	if err := newLeader.Barrier(); err != nil {
		t.Fatalf("new leader barrier: %v", err)
	}
	retry := topicManager(t, newLeader, newLeader)
	if _, err := retry.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Schema: sch}); !errors.Is(err, topics.ErrAlreadyExists) {
		t.Fatalf("retry on the new leader = %v, want topic already exists", err)
	}

	for _, s := range stores {
		deadline := time.Now().Add(5 * time.Second)
		for {
			_, getErr := s.GetTopic(ctx, "orders")
			v, _, _ := s.LatestSchema(ctx, "orders")
			if getErr == nil && v == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("a replica holds topic orders (err %v) with schema version %d, want version 1: the create left a topic without its schema", getErr, v)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// colocatedPartitions counts the child partitions placed on the node
// that owns the parent's partition of the same index.
func colocatedPartitions(t *testing.T, s *metastore.Store, parent, child string) (colocated, total int) {
	t.Helper()
	owners := assignmentsByPartition(t, s, parent)
	for p, owner := range assignmentsByPartition(t, s, child) {
		total++
		if owners[p] == owner {
			colocated++
		}
	}
	return colocated, total
}

// placementPass places every unassigned partition of every topic, the
// way the controller's sweep does between two steps of a create.
func placementPass(ctx context.Context, t *testing.T, s *metastore.Store) {
	t.Helper()
	all, _, err := s.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		t.Errorf("ListTopics: %v", err)
		return
	}
	for _, tp := range all {
		if err := s.AssignNewPartitions(ctx, tp.Name, 0, tp.Partitions); err != nil {
			t.Errorf("place %s: %v", tp.Name, err)
		}
	}
}

// A placement pass while a create-as-child is in flight must not put
// the child's partitions on the parent's nodes. The attach-offset
// resolver is the hook: in production it asks every owner of the
// parent's partitions for its tail, so it runs between the child's
// record and its link in a create of several entries, and before the
// single entry of a single-entry create.
func TestCreateAsChildIsNeverColocatedBySweep(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	registerCurrentMembers(t, s, "a", "b", "c")
	m := topicManager(t, s, s)
	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 6}); err != nil {
		t.Fatal(err)
	}
	s.SetAttachOffsetResolver(func(ctx context.Context, _ string) ([]int64, error) {
		placementPass(ctx, t, s)
		return make([]int64, 6), nil
	})
	got, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders-replica", Parent: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsChild() || got.Parent != "orders" {
		t.Fatalf("created %+v, want a child of orders", got)
	}
	if n, total := colocatedPartitions(t, s, "orders", "orders-replica"); total != 6 || n != 0 {
		t.Fatalf("%d of %d child partitions share a node with the parent's partition of the same index, want 0 of 6", n, total)
	}
}

// remoteWrites makes one write of each remote entry type. Until every
// member applies them (usable false) each is refused with
// ErrEntryTypeNotYetUsable and proposes nothing: there is no older entry
// to fall back to.
func remoteWrites(t *testing.T, s *metastore.Store, suffix string, usable bool) {
	t.Helper()
	ctx := context.Background()
	check := func(what string, err error) {
		t.Helper()
		switch {
		case usable && err != nil:
			t.Fatalf("%s: %v", what, err)
		case !usable && !errors.Is(err, metastore.ErrEntryTypeNotYetUsable):
			t.Fatalf("%s while a member holds the remote entry types back = %v, want ErrEntryTypeNotYetUsable", what, err)
		}
	}
	name, parent, stub := "b"+strings.ReplaceAll(suffix, "-", ""), "src"+suffix, "src-to-b"+suffix
	if err := s.CreateTopic(ctx, topic.Topic{Name: parent, ID: "id-" + parent, Partitions: 1, RetentionMs: topic.MinRemoteSourceRetentionMs}); err != nil {
		t.Fatal(err)
	}
	check("put remote", s.PutRemote(ctx, metastore.PutRemoteOp{Record: domremote.Record{
		Name: name, ID: "id-" + name, URL: "https://" + name + ".example", Username: "repl",
		Credential: domremote.Envelope{V: domremote.EnvelopeVersion, KV: "0123456789abcdef", CT: bytes.Repeat([]byte{1}, 40)},
	}, Salt: bytes.Repeat([]byte{7}, 32), SealedAtMs: 1}))
	inFlight := 4
	check("update remote", s.UpdateRemote(ctx, metastore.UpdateRemoteOp{Name: name, Fields: metastore.RemoteFields{Limits: &domremote.LimitsPatch{MaxInFlight: &inFlight}}, ReadRevision: 1}))
	check("attach remote child", s.AttachRemoteChild(ctx, metastore.AttachRemoteChildOp{
		Parent: parent, ParentID: "id-" + parent, Stub: stub, Remote: topic.RemoteLink{Name: name, Topic: "orders"},
	}))
	var epoch string
	if got, err := s.GetTopic(ctx, stub); err == nil {
		epoch = got.AttachEpoch
	}
	check("pause remote child", s.SetRemoteChildState(ctx, metastore.RemoteChildStateOp{
		Parent: parent, Stub: stub, Epoch: epoch, Pause: &metastore.RemotePauseState{Paused: true},
	}))
	if usable {
		if err := s.DeleteTopic(ctx, stub); err != nil {
			t.Fatal(err)
		}
	}
	check("delete remote", s.DeleteRemote(ctx, metastore.DeleteRemoteOp{Name: name}))
}

// The leader proposes the new entry types only once every member
// reports a release that applies them: until then every topic write and
// placement goes through the entries every release applies, and the
// first use of each new type is logged once.
func TestNewEntryTypesWaitForEveryMember(t *testing.T) {
	ctx := context.Background()
	logs := &lockedBuffer{}
	s := newTestStoreLogging(t, slog.New(slog.NewTextHandler(logs, nil)))
	registerCurrentMembers(t, s, "a", "b")
	old := metastore.Member{ID: "old", Addr: "old:7942", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}
	if err := s.RegisterMember(ctx, old); err != nil {
		t.Fatal(err)
	}
	m := topicManager(t, s, s)
	writes := func(suffix string) {
		t.Helper()
		parent, child := "orders"+suffix, "orders-copy"+suffix
		if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: parent, Schema: []byte(`{"type":"object","required":["id"]}`)}); err != nil {
			t.Fatal(err)
		}
		if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: child, Parent: parent}); err != nil {
			t.Fatal(err)
		}
		if _, err := m.UpdateTopicRetention(ctx, parent, 7_200_000); err != nil {
			t.Fatal(err)
		}
		if _, err := m.IncreaseTopicPartitions(ctx, parent, 6); err != nil {
			t.Fatal(err)
		}
		if _, err := m.UpdateTopicSchema(ctx, parent, []byte(`{"type":"object"}`), 0); err != nil {
			t.Fatal(err)
		}
		if err := m.DetachChild(ctx, parent, child); err != nil {
			t.Fatal(err)
		}
		if err := m.AttachChild(ctx, parent, child, 0); err != nil {
			t.Fatal(err)
		}
		if err := m.DeleteTopic(ctx, child); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkMemberDead(ctx, "b"); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateUser(ctx, user.User{Username: "bob" + suffix}); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteUser(ctx, "bob"+suffix); err != nil {
			t.Fatal(err)
		}
		remoteWrites(t, s, suffix, suffix != "")
	}

	writes("")
	for _, et := range metastore.LoggedEntryTypesForTest(t, s) {
		if et > metastore.LegacyMaxEntryTypeForTest {
			t.Fatalf("the raft log holds entry type %d while member old reports none; a 3.0.x replica would skip it", et)
		}
	}

	old.Build, old.EntryTypes = "narad test", metastore.MaxEntryType
	if err := s.RegisterMember(ctx, old); err != nil {
		t.Fatal(err)
	}
	writes("-2")
	writes("-3")
	used := map[uint32]bool{}
	for _, et := range metastore.LoggedEntryTypesForTest(t, s) {
		used[et] = true
	}
	for et := metastore.LegacyMaxEntryTypeForTest + 1; et <= metastore.MaxEntryType; et++ {
		if et == metastore.LegacyMaxEntryTypeForTest+8 {
			continue // the orphan-row prune: nothing to prune here
		}
		if !used[et] {
			t.Errorf("entry type %d was not used once every member reports it", et)
		}
	}
	for _, line := range []string{
		"every member applies raft entry type 23; using create topic with schema and link",
		"every member applies raft entry type 24; using compare-and-set topic update",
	} {
		if n := strings.Count(logs.String(), line); n != 1 {
			t.Errorf("log line %q appears %d times, want once", line, n)
		}
	}
}
