package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

type fakeMetastore struct {
	topics map[string]topic.Topic
}

func newFakeMetastore() *fakeMetastore {
	return &fakeMetastore{topics: map[string]topic.Topic{}}
}

func (f *fakeMetastore) CreateTopic(_ context.Context, t topic.Topic) error {
	if _, exists := f.topics[t.Name]; exists {
		return errs.ErrAlreadyExists
	}
	f.topics[t.Name] = t
	return nil
}

func (f *fakeMetastore) UpdateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	return nil
}

func (f *fakeMetastore) DeleteTopic(_ context.Context, name string) error {
	delete(f.topics, name)
	return nil
}

func (f *fakeMetastore) AttachChild(context.Context, string, string) error { return nil }
func (f *fakeMetastore) DetachChild(context.Context, string, string) error { return nil }

func (f *fakeMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	t, ok := f.topics[name]
	if !ok {
		return topic.Topic{}, errs.ErrNotFound
	}
	return t, nil
}

func (f *fakeMetastore) ListTopics(_ context.Context, _ metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}

func (f *fakeMetastore) PutSchema(_ context.Context, _ string, _ int, _ []byte) error { return nil }
func (f *fakeMetastore) GetSchema(_ context.Context, _ string, _ int) ([]byte, error) {
	return nil, errs.ErrNotFound
}
func (f *fakeMetastore) LeaderAddr() string { return "" }
func (f *fakeMetastore) GetMember(string) (metastore.Member, error) {
	return metastore.Member{}, errs.ErrNotFound
}

func (f *fakeMetastore) GetConsumerOffset(_ context.Context, _ string, _ int) (int64, error) {
	return 0, nil
}

func (f *fakeMetastore) SetConsumerOffset(_ context.Context, _ string, _ int, _ int64) error {
	return nil
}
func (f *fakeMetastore) Close() error { return nil }

func validDeps(t *testing.T) Deps {
	t.Helper()
	return Deps{
		DataDir:        t.TempDir(),
		StorageOptions: storage.Options{FlushInterval: time.Millisecond},
		TopicConfig: TopicConfig{
			DefaultPartitions:                1,
			MaxPartitions:                    8,
			DefaultRetentionMs:               1000,
			DefaultVisibilityTimeoutMs:       1000,
			DefaultMaxInFlightPerPartition:   10,
			DefaultMaxAckedAheadPerPartition: 10,
		},
		Metastore:  newFakeMetastore(),
		Partitions: partition.NewHashRoundRobin(),
		Schemas:    schema.NewAlwaysValid(),
		ConsumerOffsets: consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
		}, nil),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestNewRejectsMissingRequiredDeps(t *testing.T) {
	deps := validDeps(t)
	deps.DataDir = ""
	if _, err := New(deps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New() data dir error = %v, want %v", err, ErrInvalidArgument)
	}

	deps = validDeps(t)
	deps.Metastore = nil
	if _, err := New(deps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New() missing dependency error = %v, want %v", err, ErrInvalidArgument)
	}
}

func TestNewRejectsInvalidTopicDefaults(t *testing.T) {
	deps := validDeps(t)
	deps.TopicConfig.DefaultPartitions = 0
	if _, err := New(deps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New() default partitions error = %v, want %v", err, ErrInvalidArgument)
	}

	deps = validDeps(t)
	deps.TopicConfig.DefaultMaxInFlightPerPartition = 0
	if _, err := New(deps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New() max in flight error = %v, want %v", err, ErrInvalidArgument)
	}

	deps = validDeps(t)
	deps.TopicConfig.DefaultMaxAckedAheadPerPartition = 0
	if _, err := New(deps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("New() max acked ahead error = %v, want %v", err, ErrInvalidArgument)
	}
}

func TestNewReturnsWorkingBroker(t *testing.T) {
	br, err := New(validDeps(t))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if br == nil {
		t.Fatal("New() returned nil broker")
	}
	if err := br.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
