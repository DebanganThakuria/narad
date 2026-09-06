package topics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

const testTopicName = "orders"

type fakeMetastore struct {
	topics               map[string]topic.Topic
	schemas              map[string]map[int][]byte
	createTopicErr       error
	updateTopicErr       error
	updateTopicHook      func()
	deleteTopicErr       error
	getTopicErr          error
	putSchemaErr         error
	putSchemaConflicts   int
	putSchemaCalls       int
	attachChildErr       error
	detachChildErr       error
	lastCreatedTopic     topic.Topic
	lastUpdatedTopic     topic.Topic
	lastDeletedTopicName string
	lastSchemaTopic      string
	lastSchemaVersion    int
	lastSchemaBytes      []byte
}

func newFakeMetastore() *fakeMetastore {
	return &fakeMetastore{
		topics:  map[string]topic.Topic{},
		schemas: map[string]map[int][]byte{},
	}
}

func (f *fakeMetastore) CreateTopic(_ context.Context, t topic.Topic) error {
	if f.createTopicErr != nil {
		return f.createTopicErr
	}
	f.lastCreatedTopic = t
	f.topics[t.Name] = t
	return nil
}

func (f *fakeMetastore) UpdateTopic(_ context.Context, t topic.Topic) error {
	if f.updateTopicHook != nil {
		f.updateTopicHook()
	}
	if f.updateTopicErr != nil {
		return f.updateTopicErr
	}
	f.lastUpdatedTopic = t
	f.topics[t.Name] = t
	return nil
}

func (f *fakeMetastore) DeleteTopic(_ context.Context, name string) error {
	if f.deleteTopicErr != nil {
		return f.deleteTopicErr
	}
	f.lastDeletedTopicName = name
	delete(f.topics, name)
	delete(f.schemas, name)
	return nil
}

func (f *fakeMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	if f.getTopicErr != nil {
		return topic.Topic{}, f.getTopicErr
	}
	t, ok := f.topics[name]
	if !ok {
		return topic.Topic{}, errs.ErrNotFound
	}
	return t, nil
}

func (f *fakeMetastore) ListTopics(_ context.Context, _ metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}

// AttachChild/DetachChild mimic the FSM's link mutations closely enough
// for Manager-level tests; invariant enforcement is covered by the
// metastore's own tests.
func (f *fakeMetastore) AttachChild(_ context.Context, parent, child string, delayMs int64) error {
	if f.attachChildErr != nil {
		return f.attachChildErr
	}
	p, ok := f.topics[parent]
	if !ok {
		return errs.ErrNotFound
	}
	c, ok := f.topics[child]
	if !ok {
		return errs.ErrNotFound
	}
	p.Role, p.Children = topic.RoleParent, append(p.Children, child)
	c.Role, c.Parent, c.FanoutDelayMs = topic.RoleChild, parent, delayMs
	f.topics[parent], f.topics[child] = p, c
	return nil
}

func (f *fakeMetastore) DetachChild(_ context.Context, parent, child string) error {
	if f.detachChildErr != nil {
		return f.detachChildErr
	}
	p, okP := f.topics[parent]
	c, okC := f.topics[child]
	if !okP || !okC || c.Parent != parent {
		return errs.ErrNotFound
	}
	p.Children = slices.DeleteFunc(p.Children, func(n string) bool { return n == child })
	if len(p.Children) == 0 {
		p.Children, p.Role = nil, topic.RoleStandalone
	}
	c.Role, c.Parent, c.FanoutDelayMs = topic.RoleStandalone, "", 0
	f.topics[parent], f.topics[child] = p, c
	return nil
}

// PutSchema mirrors the FSM's append-only rule: only latest+1 is
// accepted, so the fake refuses the same overwrites the real metastore
// does. putSchemaConflicts simulates a put that lost a race (the FSM
// answering ErrAlreadyExists) that many times before accepting.
func (f *fakeMetastore) PutSchema(_ context.Context, topicName string, version int, raw []byte) error {
	if f.putSchemaErr != nil {
		return f.putSchemaErr
	}
	f.putSchemaCalls++
	if f.putSchemaConflicts > 0 {
		f.putSchemaConflicts--
		return fmt.Errorf("%w: simulated concurrent put", errs.ErrAlreadyExists)
	}
	latest := 0
	for v := range f.schemas[topicName] {
		if v > latest {
			latest = v
		}
	}
	if version <= latest {
		return fmt.Errorf("%w: schema version %d for %q (latest is %d)", errs.ErrAlreadyExists, version, topicName, latest)
	}
	if version != latest+1 {
		return fmt.Errorf("%w: schema version %d for %q skips versions (latest is %d)", errs.ErrInvalidArgument, version, topicName, latest)
	}
	if f.schemas[topicName] == nil {
		f.schemas[topicName] = map[int][]byte{}
	}
	f.lastSchemaTopic = topicName
	f.lastSchemaVersion = version
	f.lastSchemaBytes = append([]byte(nil), raw...)
	f.schemas[topicName][version] = append([]byte(nil), raw...)
	return nil
}

func (f *fakeMetastore) GetSchema(_ context.Context, topicName string, version int) ([]byte, error) {
	if versions, ok := f.schemas[topicName]; ok {
		if raw, ok := versions[version]; ok {
			return append([]byte(nil), raw...), nil
		}
	}
	return nil, errs.ErrNotFound
}

func (f *fakeMetastore) LeaderAddr() string { return "" }

func (f *fakeMetastore) GetMember(string) (metastore.Member, error) {
	return metastore.Member{}, errs.ErrNotFound
}

func (f *fakeMetastore) Close() error { return nil }

type fakePartitionAssigner struct {
	lastTopic         string
	lastFromPartition int
	lastToPartition   int
	calls             int
	err               error
}

func (f *fakePartitionAssigner) AssignNewPartitions(_ context.Context, topicName string, fromPartition, toPartition int) error {
	f.lastTopic = topicName
	f.lastFromPartition = fromPartition
	f.lastToPartition = toPartition
	f.calls++
	return f.err
}

type fakeSchemaRegistry struct {
	validateDefinitionErr error
	compatErr             error
	loadErr               error
	lastValidatedTopic    string
	lastValidatedSchema   []byte
	compatCalls           int
	lastCompatPrevious    []byte
	lastCompatNext        []byte
	lastLoadedTopic       string
	lastLoadedVersion     int
	lastLoadedSchema      []byte
	lastReplacedTopic     string
	lastReplacedHistory   []schema.Version
	lastDroppedTopic      string
}

func (f *fakeSchemaRegistry) ValidateDefinition(_ context.Context, topic string, raw []byte) error {
	f.lastValidatedTopic = topic
	f.lastValidatedSchema = append([]byte(nil), raw...)
	return f.validateDefinitionErr
}

func (f *fakeSchemaRegistry) CheckCompatible(_ context.Context, _ string, previous, next []byte) error {
	f.compatCalls++
	f.lastCompatPrevious = append([]byte(nil), previous...)
	f.lastCompatNext = append([]byte(nil), next...)
	return f.compatErr
}

func (f *fakeSchemaRegistry) Load(_ context.Context, topic string, version int, raw []byte) error {
	if f.loadErr != nil {
		return f.loadErr
	}
	f.lastLoadedTopic = topic
	f.lastLoadedVersion = version
	f.lastLoadedSchema = append([]byte(nil), raw...)
	return nil
}

func (f *fakeSchemaRegistry) ReplaceTopic(_ context.Context, topic string, history []schema.Version) error {
	f.lastReplacedTopic = topic
	f.lastReplacedHistory = history
	return nil
}

func (f *fakeSchemaRegistry) DropTopic(_ context.Context, topic string) error {
	f.lastDroppedTopic = topic
	return nil
}

func (f *fakeSchemaRegistry) Validate(_ context.Context, _ string, _ []byte) error {
	return nil
}

func newTestManager(t *testing.T, ms *fakeMetastore, reg schema.Registry) *Manager {
	return newTestManagerWithAssigner(t, ms, nil, reg)
}

func newTestManagerWithAssigner(t *testing.T, ms *fakeMetastore, assigner PartitionAssigner, reg schema.Registry) *Manager {
	t.Helper()
	if ms == nil {
		ms = newFakeMetastore()
	}
	return newTestManagerForMetastore(t, ms, assigner, reg, "")
}

// newTestManagerForMetastore accepts any metastore implementation (so
// tests can wrap fakeMetastore with assignment support) plus a selfID.
func newTestManagerForMetastore(t *testing.T, ms metastore.Metastore, assigner PartitionAssigner, reg schema.Registry, selfID string) *Manager {
	t.Helper()
	if reg == nil {
		reg = &fakeSchemaRegistry{}
	}
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{}, ms, nil)
	return NewManager(
		dataDir,
		ms,
		assigner,
		reg,
		consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: 2, MaxAckedAhead: 2}, nil
		}, nil),
		logs,
		Config{
			DefaultPartitions:                3,
			MaxPartitions:                    12,
			DefaultRetentionMs:               3600000,
			DefaultVisibilityTimeoutMs:       30000,
			DefaultMaxInFlightPerPartition:   10,
			DefaultMaxAckedAheadPerPartition: 11,
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		selfID,
	)
}

func TestCreateTopic_AppliesDefaultsAndCreatesDirectory(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, nil)

	created, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if created.Name != testTopicName {
		t.Fatalf("CreateTopic() name = %q, want %q", created.Name, testTopicName)
	}
	if created.Partitions != 3 {
		t.Fatalf("CreateTopic() defaults = %+v, want partitions=3", created)
	}
	if created.RetentionMs != 3600000 || created.VisibilityTimeoutMs != 30000 {
		t.Fatalf("CreateTopic() retention defaults = %+v", created)
	}
	if created.MaxInFlightPerPartition != 10 || created.MaxAckedAheadPerPartition != 11 {
		t.Fatalf("CreateTopic() cap defaults = %+v", created)
	}
	if created.CreatedAt == 0 {
		t.Fatal("CreateTopic() CreatedAt = 0, want unix timestamp")
	}
	if ms.lastCreatedTopic.Name != testTopicName {
		t.Fatalf("metastore CreateTopic() topic = %q, want %q", ms.lastCreatedTopic.Name, testTopicName)
	}

	// Create must not touch disk: the leader may own no partition of the
	// topic, and an empty directory only inflates the orphan sweep and
	// the disk-size metrics. Partition logs create their own dirs lazily.
	topicDir := filepath.Join(manager.dataDir, "topics", testTopicName)
	if _, err := os.Stat(topicDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("topic dir stat error = %v, want not exists (create must not create the topic directory)", err)
	}
}

func TestCreateTopic_AssignsPartitionsSynchronously(t *testing.T) {
	ms := newFakeMetastore()
	assigner := &fakePartitionAssigner{}
	manager := newTestManagerWithAssigner(t, ms, assigner, nil)

	created, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if created.Name != testTopicName {
		t.Fatalf("CreateTopic() name = %q, want %q", created.Name, testTopicName)
	}
	if assigner.calls != 1 {
		t.Fatalf("AssignNewPartitions() calls = %d, want 1", assigner.calls)
	}
	if assigner.lastTopic != testTopicName || assigner.lastFromPartition != 0 || assigner.lastToPartition != 3 {
		t.Fatalf("AssignNewPartitions() = topic=%q from=%d to=%d", assigner.lastTopic, assigner.lastFromPartition, assigner.lastToPartition)
	}
}

func TestCreateTopic_WithSchemaPersistsAndLoadsInitialVersion(t *testing.T) {
	ms := newFakeMetastore()
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	rawSchema := []byte(`{"type":"object"}`)

	created, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName, Schema: rawSchema})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if created.Name != testTopicName {
		t.Fatalf("CreateTopic() name = %q, want %q", created.Name, testTopicName)
	}
	if reg.lastValidatedTopic != testTopicName || string(reg.lastValidatedSchema) != string(rawSchema) {
		t.Fatalf("ValidateDefinition() = topic %q schema %q", reg.lastValidatedTopic, string(reg.lastValidatedSchema))
	}
	if ms.lastSchemaTopic != testTopicName || ms.lastSchemaVersion != 1 || string(ms.lastSchemaBytes) != string(rawSchema) {
		t.Fatalf("PutSchema() = topic %q version %d schema %q", ms.lastSchemaTopic, ms.lastSchemaVersion, string(ms.lastSchemaBytes))
	}
	if reg.lastLoadedTopic != testTopicName || reg.lastLoadedVersion != 1 || string(reg.lastLoadedSchema) != string(rawSchema) {
		t.Fatalf("Load() = topic %q version %d schema %q", reg.lastLoadedTopic, reg.lastLoadedVersion, string(reg.lastLoadedSchema))
	}
}

func TestCreateTopic_RejectsInvalidSchemaBeforeCreate(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, &fakeSchemaRegistry{validateDefinitionErr: schema.ErrIncompatible})

	_, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName, Schema: []byte(`{"type":"object"}`)})
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("CreateTopic() error = %v, want %v", err, ErrInvalid)
	}
	if ms.lastCreatedTopic.Name != "" {
		t.Fatalf("CreateTopic() created topic %q before rejecting schema", ms.lastCreatedTopic.Name)
	}
}

func TestCreateTopic_RollsBackWhenInitialSchemaSetupFails(t *testing.T) {
	cases := []struct {
		name string
		ms   *fakeMetastore
		reg  *fakeSchemaRegistry
	}{
		{
			name: "persist schema fails",
			ms:   &fakeMetastore{topics: map[string]topic.Topic{}, schemas: map[string]map[int][]byte{}, putSchemaErr: errors.New("put schema failed")},
			reg:  &fakeSchemaRegistry{},
		},
		{
			name: "load schema fails",
			ms:   newFakeMetastore(),
			reg:  &fakeSchemaRegistry{loadErr: errors.New("load schema failed")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestManager(t, tc.ms, tc.reg)

			_, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName, Schema: []byte(`{"type":"object"}`)})
			if err == nil {
				t.Fatal("CreateTopic() error = nil, want schema setup error")
			}
			if _, ok := tc.ms.topics[testTopicName]; ok {
				t.Fatalf("CreateTopic() left topic metadata for %q after schema failure", testTopicName)
			}
			if _, ok := tc.ms.schemas[testTopicName]; ok {
				t.Fatalf("CreateTopic() left schema metadata for %q after schema failure", testTopicName)
			}
			if tc.ms.lastDeletedTopicName != testTopicName {
				t.Fatalf("rollback deleted topic = %q, want %q", tc.ms.lastDeletedTopicName, testTopicName)
			}
			topicDir := filepath.Join(manager.dataDir, "topics", testTopicName)
			if _, statErr := os.Stat(topicDir); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("topic dir stat error = %v, want not exists", statErr)
			}
		})
	}
}

func TestCreateTopic_RejectsInvalidInputs(t *testing.T) {
	manager := newTestManager(t, nil, nil)
	cases := []struct {
		name string
		opts CreateOpts
		want string
	}{
		{name: "missing name", opts: CreateOpts{}, want: "name required"},
		{name: "path separator", opts: CreateOpts{Name: "orders/nested"}, want: "topic name must match"},
		{name: "dot dot", opts: CreateOpts{Name: ".."}, want: `topic name must not be ".."`},
		{name: "dot", opts: CreateOpts{Name: "."}, want: `topic name must not be "."`},
		{name: "space", opts: CreateOpts{Name: "or ders"}, want: "topic name must match"},
		{name: "negative partitions", opts: CreateOpts{Name: testTopicName, Partitions: -1}, want: "partitions must be >= 3"},
		{name: "partitions over max", opts: CreateOpts{Name: testTopicName, Partitions: 33}, want: "exceeds topic.max_partitions"},
		{name: "negative retention", opts: CreateOpts{Name: testTopicName, RetentionMs: -1}, want: "retention_ms must be >= 0"},
		{name: "negative visibility timeout", opts: CreateOpts{Name: testTopicName, VisibilityTimeoutMs: -1}, want: "visibility_timeout_ms must be >= 0"},
		{name: "negative in flight cap", opts: CreateOpts{Name: testTopicName, MaxInFlightPerPartition: -1}, want: "max_in_flight_per_partition must be >= 0"},
		{name: "negative acked ahead cap", opts: CreateOpts{Name: testTopicName, MaxAckedAheadPerPartition: -1}, want: "max_acked_ahead_per_partition must be >= 0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := manager.CreateTopic(context.Background(), tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CreateTopic() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestCreateTopic_MapsAlreadyExists(t *testing.T) {
	ms := newFakeMetastore()
	ms.createTopicErr = errs.ErrAlreadyExists
	manager := newTestManager(t, ms, nil)

	_, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateTopic() error = %v, want %v", err, ErrAlreadyExists)
	}
}

func TestCreateTopic_AssignmentFailureDoesNotFailCreate(t *testing.T) {
	ms := newFakeMetastore()
	assigner := &fakePartitionAssigner{err: errors.New("assign failed")}
	manager := newTestManagerWithAssigner(t, ms, assigner, nil)

	created, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if created.Name != testTopicName {
		t.Fatalf("CreateTopic() name = %q, want %q", created.Name, testTopicName)
	}
	if assigner.calls != 1 {
		t.Fatalf("AssignNewPartitions() calls = %d, want 1", assigner.calls)
	}
}

func TestGetTopic_MapsNotFound(t *testing.T) {
	manager := newTestManager(t, newFakeMetastore(), nil)

	_, err := manager.GetTopic(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTopic() error = %v, want %v", err, ErrNotFound)
	}
}

func TestIncreaseTopicPartitions_UpdatesTopic(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManager(t, ms, nil)

	updated, err := manager.IncreaseTopicPartitions(context.Background(), testTopicName, 5)
	if err != nil {
		t.Fatalf("IncreaseTopicPartitions() error = %v", err)
	}
	if updated.Partitions != 5 {
		t.Fatalf("IncreaseTopicPartitions() partitions = %d, want 5", updated.Partitions)
	}
	if ms.lastUpdatedTopic.Partitions != 5 {
		t.Fatalf("metastore UpdateTopic() partitions = %d, want 5", ms.lastUpdatedTopic.Partitions)
	}
}

func TestIncreaseTopicPartitions_AssignsOnlyNewRange(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	assigner := &fakePartitionAssigner{}
	manager := newTestManagerWithAssigner(t, ms, assigner, nil)

	updated, err := manager.IncreaseTopicPartitions(context.Background(), testTopicName, 5)
	if err != nil {
		t.Fatalf("IncreaseTopicPartitions() error = %v", err)
	}
	if updated.Partitions != 5 {
		t.Fatalf("IncreaseTopicPartitions() partitions = %d, want 5", updated.Partitions)
	}
	if assigner.calls != 1 {
		t.Fatalf("AssignNewPartitions() calls = %d, want 1", assigner.calls)
	}
	if assigner.lastTopic != testTopicName || assigner.lastFromPartition != 3 || assigner.lastToPartition != 5 {
		t.Fatalf("AssignNewPartitions() = topic=%q from=%d to=%d", assigner.lastTopic, assigner.lastFromPartition, assigner.lastToPartition)
	}
}

func TestIncreaseTopicPartitions_AssignmentFailureDoesNotFailUpdate(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	assigner := &fakePartitionAssigner{err: errors.New("assign failed")}
	manager := newTestManagerWithAssigner(t, ms, assigner, nil)

	updated, err := manager.IncreaseTopicPartitions(context.Background(), testTopicName, 5)
	if err != nil {
		t.Fatalf("IncreaseTopicPartitions() error = %v", err)
	}
	if updated.Partitions != 5 {
		t.Fatalf("IncreaseTopicPartitions() partitions = %d, want 5", updated.Partitions)
	}
	if assigner.calls != 1 {
		t.Fatalf("AssignNewPartitions() calls = %d, want 1", assigner.calls)
	}
}

func TestIncreaseTopicPartitions_RejectsNonIncreasingCounts(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManager(t, ms, nil)

	_, err := manager.IncreaseTopicPartitions(context.Background(), testTopicName, 3)
	if err == nil || !strings.Contains(err.Error(), "must be greater than current") {
		t.Fatalf("IncreaseTopicPartitions() error = %v, want non-increasing error", err)
	}
}

func TestUpdateTopicRetention_UsesDefaultWhenZero(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, RetentionMs: 1000}
	manager := newTestManager(t, ms, nil)

	updated, err := manager.UpdateTopicRetention(context.Background(), testTopicName, 0)
	if err != nil {
		t.Fatalf("UpdateTopicRetention() error = %v", err)
	}
	if updated.RetentionMs != 3600000 {
		t.Fatalf("UpdateTopicRetention() retention = %d, want 3600000", updated.RetentionMs)
	}
}

func TestUpdateTopicCaps_UsesDefaultsAndPersists(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, MaxInFlightPerPartition: 1, MaxAckedAheadPerPartition: 1}
	manager := newTestManager(t, ms, nil)

	updated, err := manager.UpdateTopicCaps(context.Background(), testTopicName, 0, 0)
	if err != nil {
		t.Fatalf("UpdateTopicCaps() error = %v", err)
	}
	if updated.MaxInFlightPerPartition != 10 || updated.MaxAckedAheadPerPartition != 11 {
		t.Fatalf("UpdateTopicCaps() caps = %+v, want defaults", updated)
	}
}

// The version is computed from the persisted history, never from the
// local registry: with six versions in the metastore the update is
// checked against v6 and stored as v7, then loaded locally as v7.
func TestUpdateTopicSchema_VersionsFromPersistedHistory(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	for v := 1; v <= 6; v++ {
		if err := ms.PutSchema(context.Background(), testTopicName, v, []byte(fmt.Sprintf(`{"title":"v%d","type":"object"}`, v))); err != nil {
			t.Fatal(err)
		}
	}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	rawSchema := []byte(`{"title":"v7","type":"object"}`)

	updated, err := manager.UpdateTopicSchema(context.Background(), testTopicName, rawSchema, 0)
	if err != nil {
		t.Fatalf("UpdateTopicSchema() error = %v", err)
	}
	if updated.Name != testTopicName {
		t.Fatalf("UpdateTopicSchema() topic = %q, want %q", updated.Name, testTopicName)
	}
	if reg.compatCalls != 1 || string(reg.lastCompatPrevious) != `{"title":"v6","type":"object"}` || string(reg.lastCompatNext) != string(rawSchema) {
		t.Fatalf("CheckCompatible() calls = %d previous %q next %q, want 1 call against the persisted v6",
			reg.compatCalls, reg.lastCompatPrevious, reg.lastCompatNext)
	}
	if ms.lastSchemaVersion != 7 {
		t.Fatalf("PutSchema() version = %d, want 7", ms.lastSchemaVersion)
	}
	if string(ms.lastSchemaBytes) != string(rawSchema) {
		t.Fatalf("PutSchema() raw schema = %q, want %q", string(ms.lastSchemaBytes), string(rawSchema))
	}
	if reg.lastLoadedTopic != testTopicName || reg.lastLoadedVersion != 7 || string(reg.lastLoadedSchema) != string(rawSchema) {
		t.Fatalf("Load() = topic %q version %d schema %q, want %q v7", reg.lastLoadedTopic, reg.lastLoadedVersion, reg.lastLoadedSchema, testTopicName)
	}
}

// A first schema on a topic that never had one skips the compatibility
// check and is stored as v1.
func TestUpdateTopicSchema_FirstVersionSkipsCompatibilityCheck(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	reg := &fakeSchemaRegistry{compatErr: schema.ErrIncompatible}
	manager := newTestManager(t, ms, reg)

	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"type":"object"}`), 0); err != nil {
		t.Fatalf("UpdateTopicSchema() error = %v", err)
	}
	if reg.compatCalls != 0 {
		t.Fatalf("CheckCompatible() calls = %d, want 0 for the first version", reg.compatCalls)
	}
	if ms.lastSchemaVersion != 1 {
		t.Fatalf("PutSchema() version = %d, want 1", ms.lastSchemaVersion)
	}
}

// An incompatible schema is refused before anything is proposed, and
// the previous version stays exactly as it was.
func TestUpdateTopicSchema_IncompatibleIsRefusedBeforePersist(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}}}`)
	if err := ms.PutSchema(context.Background(), testTopicName, 1, v1); err != nil {
		t.Fatal(err)
	}
	reg := &fakeSchemaRegistry{compatErr: schema.ErrIncompatible}
	manager := newTestManager(t, ms, reg)
	putsBefore := ms.putSchemaCalls

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"type":"object"}`), 0)
	if !errors.Is(err, ErrInvalid) || !errors.Is(err, errs.ErrSchemaIncompatible) {
		t.Fatalf("UpdateTopicSchema() error = %v, want %v wrapping %v", err, ErrInvalid, errs.ErrSchemaIncompatible)
	}
	if ms.putSchemaCalls != putsBefore {
		t.Fatalf("PutSchema() was called %d times for a refused update, want 0", ms.putSchemaCalls-putsBefore)
	}
	if got := string(ms.schemas[testTopicName][1]); got != string(v1) {
		t.Fatalf("schema v1 = %s after refused update, want unchanged", got)
	}
	if reg.lastLoadedVersion != 0 {
		t.Fatalf("Load() called with version %d for a refused update", reg.lastLoadedVersion)
	}
}

// When the metastore refuses the proposed version (another put landed
// first), the history is re-read and the update is re-checked against
// the new latest before being proposed again as the next version.
func TestUpdateTopicSchema_RetriesOnVersionConflict(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	if err := ms.PutSchema(context.Background(), testTopicName, 1, []byte(`{"title":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	ms.putSchemaConflicts = 1
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)

	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v2"}`), 0); err != nil {
		t.Fatalf("UpdateTopicSchema() error = %v", err)
	}
	if reg.compatCalls != 2 {
		t.Fatalf("CheckCompatible() calls = %d, want 2 (once per attempt)", reg.compatCalls)
	}
	if ms.lastSchemaVersion != 2 {
		t.Fatalf("PutSchema() version = %d, want 2", ms.lastSchemaVersion)
	}

	// A conflict that never resolves surfaces as ErrAlreadyExists.
	ms.putSchemaConflicts = schemaPutAttempts
	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v3"}`), 0)
	if !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("UpdateTopicSchema() under persistent conflict error = %v, want %v", err, errs.ErrAlreadyExists)
	}
}

func TestUpdateTopicSchema_PersistFailureDoesNotLoadLocally(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	ms.putSchemaErr = errors.New("persist failed")
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"type":"object"}`), 0)
	if err == nil || !strings.Contains(err.Error(), "persist schema") {
		t.Fatalf("UpdateTopicSchema() error = %v, want persist schema error", err)
	}
	if reg.lastLoadedTopic != "" {
		t.Fatalf("Load() = topic %q after a failed persist, want no load", reg.lastLoadedTopic)
	}
}

func TestUpdateTopicSchema_RejectsEmptyOrInvalidSchema(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName}
	manager := newTestManager(t, ms, &fakeSchemaRegistry{validateDefinitionErr: errors.New("does not compile")})

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, nil, 0)
	if err == nil || !strings.Contains(err.Error(), "schema must not be empty") {
		t.Fatalf("UpdateTopicSchema() empty schema error = %v", err)
	}

	_, err = manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"type":"object"}`), 0)
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("UpdateTopicSchema() invalid schema error = %v, want %v", err, ErrInvalid)
	}
}

func TestDeleteTopic_RemovesTopicAndDirectory(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	topicDir := filepath.Join(manager.dataDir, "topics", testTopicName)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	err := manager.DeleteTopic(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	if ms.lastDeletedTopicName != testTopicName {
		t.Fatalf("DeleteTopic() deleted topic = %q, want %q", ms.lastDeletedTopicName, testTopicName)
	}
	if _, statErr := os.Stat(topicDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("topic dir stat error = %v, want not exists", statErr)
	}
	if reg.lastDroppedTopic != testTopicName {
		t.Fatalf("schema DropTopic() topic = %q, want %q (deleted topic's schemas must not survive in the registry)", reg.lastDroppedTopic, testTopicName)
	}
}

func TestPurgeTopic_RejectsNamesEscapingTopicsRoot(t *testing.T) {
	manager := newTestManager(t, newFakeMetastore(), nil)
	marker := filepath.Join(manager.dataDir, "marker")
	if err := os.WriteFile(marker, []byte("keep"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	topicDir := filepath.Join(manager.dataDir, "topics", testTopicName)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	// ".." resolves to the data dir itself, "." to the topics root, and a
	// separator to a nested path — RemoveAll on any of them would destroy
	// data far beyond a single topic.
	for _, name := range []string{"..", ".", "nested/" + testTopicName} {
		if err := manager.PurgeTopic(context.Background(), name, ""); !errors.Is(err, ErrInvalid) {
			t.Fatalf("PurgeTopic(%q) error = %v, want %v", name, err, ErrInvalid)
		}
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("data dir marker stat error = %v, want data dir untouched", err)
	}
	if _, err := os.Stat(topicDir); err != nil {
		t.Fatalf("topic dir stat error = %v, want topics untouched", err)
	}
}

func TestTopicUpdatesAreSerializedPerName(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3, RetentionMs: 1000}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	ms.updateTopicHook = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	manager := newTestManager(t, ms, nil)

	increaseDone := make(chan error, 1)
	go func() {
		_, err := manager.IncreaseTopicPartitions(context.Background(), testTopicName, 5)
		increaseDone <- err
	}()
	<-entered // partition increase is now mid-write, holding the name lock

	retentionDone := make(chan error, 1)
	go func() {
		_, err := manager.UpdateTopicRetention(context.Background(), testTopicName, 7200000)
		retentionDone <- err
	}()

	// The retention update must block on the per-name lock while the
	// partition increase is mid-write; completing here means it read the
	// pre-increase record and will blindly overwrite the new partition
	// count (lost update).
	select {
	case err := <-retentionDone:
		t.Fatalf("UpdateTopicRetention() finished during concurrent IncreaseTopicPartitions (err = %v), want it serialized", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-increaseDone; err != nil {
		t.Fatalf("IncreaseTopicPartitions() error = %v", err)
	}
	if err := <-retentionDone; err != nil {
		t.Fatalf("UpdateTopicRetention() error = %v", err)
	}
	final := ms.topics[testTopicName]
	if final.Partitions != 5 || final.RetentionMs != 7200000 {
		t.Fatalf("final topic = %+v, want partitions=5 retention_ms=7200000 (one update was lost)", final)
	}
}

func TestDeleteTopic_ReturnsPurgeErrorAfterMetadataDelete(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManager(t, ms, nil)
	if err := os.WriteFile(filepath.Join(manager.dataDir, "topics"), []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err := manager.DeleteTopic(context.Background(), testTopicName)
	var purgeErr PurgeError
	if !errors.As(err, &purgeErr) {
		t.Fatalf("DeleteTopic() error = %v, want PurgeError", err)
	}
	if purgeErr.Topic != testTopicName {
		t.Fatalf("PurgeError topic = %q, want %q", purgeErr.Topic, testTopicName)
	}
	if ms.lastDeletedTopicName != testTopicName {
		t.Fatalf("DeleteTopic() deleted topic = %q, want %q", ms.lastDeletedTopicName, testTopicName)
	}
}
