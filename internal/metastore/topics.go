package metastore

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/debanganthakuria/narad/internal/topic"
)

func (s *SQLiteStore) CreateTopic(ctx context.Context, t topic.Topic) error {
	if t.Name == "" {
		return errors.New("metastore: topic name required")
	}

	var count int64
	if err := s.db.WithContext(ctx).Model(&TopicRecord{}).Where("name = ?", t.Name).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return ErrAlreadyExists
	}

	if err := s.db.WithContext(ctx).Create(new(TopicRecord{}.FromTopic(t))).Error; err != nil {
		return err
	}
	s.cache.delete(listTopicsKey)
	return nil
}

func (s *SQLiteStore) UpdateTopic(ctx context.Context, t topic.Topic) error {
	if t.Name == "" {
		return errors.New("metastore: topic name required")
	}

	updates := map[string]any{
		"partitions":         t.Partitions,
		"replication_factor": t.ReplicationFactor,
		"max_age_ms":         t.Retention.MaxAgeMs,
		"max_bytes":          t.Retention.MaxBytes,
	}
	result := s.db.WithContext(ctx).Model(&TopicRecord{}).Where("name = ?", t.Name).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	s.cache.delete(topicCacheKey(t.Name))
	s.cache.delete(listTopicsKey)
	return nil
}

func (s *SQLiteStore) DeleteTopic(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("metastore: topic name required")
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Where("name = ?", name).Delete(&TopicRecord{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrNotFound
		}
		tx.Where("topic = ?", name).Delete(&SchemaRecord{})
		tx.Where("topic = ?", name).Delete(&ConsumerOffsetRecord{})
		return nil
	})
	if err != nil {
		return err
	}
	// Surgical: drop only this topic's entries (topic record + every
	// cached schema version), then explicitly invalidate the topics list.
	s.cache.deleteTopicScope(name)
	s.cache.delete(listTopicsKey)
	s.offsets.deleteTopic(name)
	return nil
}

func (s *SQLiteStore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	key := topicCacheKey(name)
	if v, ok := s.cache.get(key); ok {
		return v.(topic.Topic), nil
	}

	var record TopicRecord
	if err := s.db.WithContext(ctx).Where("name = ?", name).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, err
	}
	t := record.ToTopic()
	s.cache.store(key, t, name)
	return t, nil
}

// ListTopics returns topics in lexicographic order by name.
//
// When opts.Limit == 0, returns every topic and caches the full list
// (the cache is the fast path the metrics poller depends on). When
// opts.Limit > 0, paginates by name (keyset) and skips the cache —
// every distinct (limit, token) would be its own cache key, which
// defeats the cache.
//
// The second return is the page token to pass on the next call. It is
// empty when no more rows exist.
func (s *SQLiteStore) ListTopics(ctx context.Context, opts ListOptions) ([]topic.Topic, string, error) {
	if opts.Limit == 0 && opts.PageToken == "" {
		return s.listTopicsAllCached(ctx)
	}
	return s.listTopicsPaginated(ctx, opts)
}

func (s *SQLiteStore) listTopicsAllCached(ctx context.Context) ([]topic.Topic, string, error) {
	if v, ok := s.cache.get(listTopicsKey); ok {
		return v.([]topic.Topic), "", nil
	}

	var records []TopicRecord
	if err := s.db.WithContext(ctx).Order("name").Find(&records).Error; err != nil {
		return nil, "", err
	}

	out := make([]topic.Topic, 0, len(records))
	for _, r := range records {
		out = append(out, r.ToTopic())
	}
	// listTopicsKey is global, not scoped to one topic — we invalidate
	// it explicitly on Create/Update/Delete.
	s.cache.store(listTopicsKey, out, "")
	return out, "", nil
}

func (s *SQLiteStore) listTopicsPaginated(ctx context.Context, opts ListOptions) ([]topic.Topic, string, error) {
	q := s.db.WithContext(ctx).Order("name")
	if opts.PageToken != "" {
		q = q.Where("name > ?", opts.PageToken)
	}
	// Fetch one extra row to detect "more available" without a second query.
	q = q.Limit(opts.Limit + 1)

	var records []TopicRecord
	if err := q.Find(&records).Error; err != nil {
		return nil, "", err
	}

	hasMore := len(records) > opts.Limit
	if hasMore {
		records = records[:opts.Limit]
	}

	out := make([]topic.Topic, 0, len(records))
	for _, r := range records {
		out = append(out, r.ToTopic())
	}

	var nextToken string
	if hasMore && len(out) > 0 {
		nextToken = out[len(out)-1].Name
	}
	return out, nextToken, nil
}
