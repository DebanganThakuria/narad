package metastore

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *SQLiteStore) PutSchema(ctx context.Context, topicName string, version int, schema []byte) error {
	copied := make([]byte, len(schema))
	copy(copied, schema)

	if err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&SchemaRecord{
		Topic:   topicName,
		Version: version,
		Schema:  copied,
	}).Error; err != nil {
		return err
	}
	s.cache.delete(schemaCacheKey(topicName, version))
	return nil
}

func (s *SQLiteStore) GetSchema(ctx context.Context, topicName string, version int) ([]byte, error) {
	key := schemaCacheKey(topicName, version)
	if v, ok := s.cache.get(key); ok {
		// Defensive copy: the cache holds the canonical bytes and must
		// not be mutated by callers. Cheap relative to the SQLite read
		// we just avoided.
		cached := v.([]byte)
		out := make([]byte, len(cached))
		copy(out, cached)
		return out, nil
	}

	var record SchemaRecord
	if err := s.db.WithContext(ctx).Where("topic = ? AND version = ?", topicName, version).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	// Two copies: one stays in the cache, the other goes to the caller.
	cached := make([]byte, len(record.Schema))
	copy(cached, record.Schema)
	s.cache.store(key, cached, topicName)

	out := make([]byte, len(cached))
	copy(out, cached)
	return out, nil
}