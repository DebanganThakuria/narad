package topics

import (
	"context"
	"errors"
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
func (f *fakeMetastore) AttachChild(_ context.Context, parent, child string) error {
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
	c.Role, c.Parent = topic.RoleChild, parent
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
	c.Role, c.Parent = topic.RoleStandalone, ""
	f.topics[parent], f.topics[child] = p, c
	return nil
}

func (f *fakeMetastore) PutSchema(_ context.Context, topicName string, version int, raw []byte) error {
	if f.putSchemaErr != nil {
		return f.putSchemaErr
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
	registerVersion       int
	registerErr           error
	loadErr               error
	lastValidatedTopic    string
	lastValidatedSchema   []byte
	lastTopic             string
	lastSchema            []byte
	lastLoadedTopic       string
	lastLoadedVersion     int
	lastLoadedSchema      []byte
	lastUnloadedTopic     string
	lastUnloadedVersion   int
	lastDroppedTopic      string
}

func (f *fakeSchemaRegistry) ValidateDefinition(_ context.Context, topic string, raw []byte) error {
	f.lastValidatedTopic = topic
	f.lastValidatedSchema = append([]byte(nil), raw...)
	return f.validateDefinitionErr
}

func (f *fakeSchemaRegistry) Register(_ context.Context, topic string, raw []byte) (int, error) {
	if f.registerErr != nil {
		return 0, f.registerErr
	}
	f.lastTopic = topic
	f.lastSchema = append([]byte(nil), raw...)
	if f.registerVersion == 0 {
		f.registerVersion = 1
	}
	return f.registerVersion, nil
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

func (f *fakeSchemaRegistry) Unload(_ context.Context, topic string, version int) error {
	f.lastUnloadedTopic = topic
	f.lastUnloadedVersion = version
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

	topicDir := filepath.Join(manager.dataDir, "topics", testTopicName)
	if _, err := os.Stat(topicDir); err != nil {
		t.Fatalf("topic dir stat error = %v", err)
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

func TestUpdateTopicSchema_RegistersAndPersists(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	reg := &fakeSchemaRegistry{registerVersion: 7}
	manager := newTestManager(t, ms, reg)
	rawSchema := []byte(`{"type":"object"}`)

	updated, err := manager.UpdateTopicSchema(context.Background(), testTopicName, rawSchema)
	if err != nil {
		t.Fatalf("UpdateTopicSchema() error = %v", err)
	}
	if updated.Name != testTopicName {
		t.Fatalf("UpdateTopicSchema() topic = %q, want %q", updated.Name, testTopicName)
	}
	if reg.lastTopic != testTopicName {
		t.Fatalf("schema Register() topic = %q, want %q", reg.lastTopic, testTopicName)
	}
	if ms.lastSchemaVersion != 7 {
		t.Fatalf("PutSchema() version = %d, want 7", ms.lastSchemaVersion)
	}
	if string(ms.lastSchemaBytes) != string(rawSchema) {
		t.Fatalf("PutSchema() raw schema = %q, want %q", string(ms.lastSchemaBytes), string(rawSchema))
	}
}

func TestUpdateTopicSchema_RollsBackRegistryWhenPersistFails(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	ms.putSchemaErr = errors.New("persist failed")
	reg := &fakeSchemaRegistry{registerVersion: 7}
	manager := newTestManager(t, ms, reg)

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"type":"object"}`))
	if err == nil || !strings.Contains(err.Error(), "persist schema") {
		t.Fatalf("UpdateTopicSchema() error = %v, want persist schema error", err)
	}
	if reg.lastUnloadedTopic != testTopicName || reg.lastUnloadedVersion != 7 {
		t.Fatalf("Unload() = topic %q version %d, want %q version 7", reg.lastUnloadedTopic, reg.lastUnloadedVersion, testTopicName)
	}
}

func TestUpdateTopicSchema_RejectsEmptyOrInvalidSchema(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName}
	manager := newTestManager(t, ms, &fakeSchemaRegistry{registerErr: schema.ErrIncompatible})

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, nil)
	if err == nil || !strings.Contains(err.Error(), "schema must not be empty") {
		t.Fatalf("UpdateTopicSchema() empty schema error = %v", err)
	}

	_, err = manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"type":"object"}`))
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
		if err := manager.PurgeTopic(context.Background(), name); !errors.Is(err, ErrInvalid) {
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
