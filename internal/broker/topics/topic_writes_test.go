package topics

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// changingStore refuses every compare-and-set topic write as the
// leader's state machine does when the topic changed between the
// Manager's read and its write, until write number succeedAt, which it
// applies as the entries every release applies would.
type changingStore struct {
	*fakeMetastore
	succeedAt int
	writes    int
}

func (s *changingStore) write(apply func() error) error {
	s.writes++
	if s.succeedAt == 0 || s.writes < s.succeedAt {
		return fmt.Errorf("%w: the topic was recreated", errs.ErrTopicChanged)
	}
	return apply()
}

func (s *changingStore) CreateTopicWith(ctx context.Context, t topic.Topic, spec metastore.CreateTopicSpec) error {
	return s.write(func() error {
		if err := s.CreateTopic(ctx, t); err != nil {
			return err
		}
		if spec.Parent != "" {
			return s.AttachChild(ctx, spec.Parent, t.Name, spec.DelayMs)
		}
		return nil
	})
}

func (s *changingStore) UpdateTopicIf(ctx context.Context, t topic.Topic, _ string) error {
	return s.write(func() error { return s.UpdateTopic(ctx, t) })
}

func (s *changingStore) DeleteTopicIf(ctx context.Context, name, _ string) error {
	return s.write(func() error { return s.DeleteTopic(ctx, name) })
}

func (s *changingStore) PutSchemaIf(ctx context.Context, name string, version int, schema []byte, _ string) error {
	return s.write(func() error { return s.PutSchema(ctx, name, version, schema) })
}

func (s *changingStore) AttachChildIf(ctx context.Context, parent, child string, delayMs int64, _, _ string) error {
	return s.write(func() error { return s.AttachChild(ctx, parent, child, delayMs) })
}

func (s *changingStore) DetachChildIf(ctx context.Context, parent, child, _, _ string) error {
	return s.write(func() error { return s.DetachChild(ctx, parent, child) })
}

// A topic write the state machine refused because the topic changed
// since the Manager read it is read, checked and proposed again once;
// a second refusal is answered (409).
func TestTopicChangedIsRetriedOnceThen409(t *testing.T) {
	mutations := map[string]func(context.Context, *Manager) error{
		"retention": func(ctx context.Context, m *Manager) error {
			_, err := m.UpdateTopicRetention(ctx, "orders", 7_200_000)
			return err
		},
		"caps": func(ctx context.Context, m *Manager) error {
			_, err := m.UpdateTopicCaps(ctx, "orders", new(int64(5)), nil)
			return err
		},
		"partitions": func(ctx context.Context, m *Manager) error {
			_, err := m.IncreaseTopicPartitions(ctx, "orders", 6)
			return err
		},
		"schema": func(ctx context.Context, m *Manager) error {
			_, err := m.UpdateTopicSchema(ctx, "orders", []byte(`{"type":"object"}`), 0)
			return err
		},
		"delete": func(ctx context.Context, m *Manager) error {
			return m.DeleteTopic(ctx, "orders")
		},
		"attach": func(ctx context.Context, m *Manager) error {
			return m.AttachChild(ctx, "orders", "audit", 0)
		},
		"detach": func(ctx context.Context, m *Manager) error {
			if err := m.metastore.AttachChild(ctx, "orders", "audit", 0); err != nil {
				return err
			}
			return m.DetachChild(ctx, "orders", "audit")
		},
		"create-as-child": func(ctx context.Context, m *Manager) error {
			_, err := m.CreateTopic(ctx, CreateOpts{Name: "orders-copy", Parent: "orders"})
			return err
		},
	}
	for name, mutate := range mutations {
		for _, succeedAt := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/succeed-at-%d", name, succeedAt), func(t *testing.T) {
				ms := newFakeMetastore()
				ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3, RetentionMs: 3_600_000}
				ms.topics["audit"] = topic.Topic{Name: "audit", ID: "0000000000000002", Partitions: 3, RetentionMs: 3_600_000}
				store := &changingStore{fakeMetastore: ms, succeedAt: succeedAt}
				m := newTestManagerForMetastore(t, store, nil, &fakeSchemaRegistry{}, "")

				err := mutate(context.Background(), m)
				if store.writes != 2 {
					t.Fatalf("%d writes proposed, want 2: one retry after re-reading", store.writes)
				}
				if succeedAt == 0 {
					if !errors.Is(err, errs.ErrTopicChanged) {
						t.Fatalf("answer after two refusals = %v, want ErrTopicChanged (409)", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("answer when the retry applies = %v, want success", err)
				}
			})
		}
	}
}
