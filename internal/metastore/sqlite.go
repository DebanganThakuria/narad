package metastore

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/debanganthakuria/narad/internal/topic"
)

const defaultCacheMaxMB = 128

// TopicRecord is the GORM model for persisted topic metadata.
type TopicRecord struct {
	ID                uint      `gorm:"primaryKey"`
	Name              string    `gorm:"uniqueIndex;size:256;not null"`
	Partitions        int       `gorm:"not null"`
	ReplicationFactor int       `gorm:"not null"`
	MaxAgeMs          int64     `gorm:"not null;default:0"`
	MaxBytes          int64     `gorm:"not null;default:0"`
	CreatedAt         time.Time `gorm:"not null"`
}

func (TopicRecord) FromTopic(t topic.Topic) TopicRecord {
	return TopicRecord{
		Name:              t.Name,
		Partitions:        t.Partitions,
		ReplicationFactor: t.ReplicationFactor,
		MaxAgeMs:          t.Retention.MaxAgeMs,
		MaxBytes:          t.Retention.MaxBytes,
		CreatedAt:         t.CreatedAt,
	}
}

func (r TopicRecord) ToTopic() topic.Topic {
	return topic.Topic{
		Name:              r.Name,
		Partitions:        r.Partitions,
		ReplicationFactor: r.ReplicationFactor,
		Retention: topic.Retention{
			MaxAgeMs: r.MaxAgeMs,
			MaxBytes: r.MaxBytes,
		},
		CreatedAt: r.CreatedAt,
	}
}

// SchemaRecord is the GORM model for JSON Schema versions per topic.
type SchemaRecord struct {
	Topic   string `gorm:"primaryKey;size:256;not null"`
	Version int    `gorm:"primaryKey;not null"`
	Schema  []byte `gorm:"not null"`
}

// ConsumerOffsetRecord is the GORM model for committed consumer offsets.
type ConsumerOffsetRecord struct {
	Topic     string `gorm:"primaryKey;size:256;not null"`
	Partition int    `gorm:"primaryKey;not null"`
	Offset    int64  `gorm:"not null"`
}

// SQLiteStore persists broker metadata in a SQLite database via GORM.
type SQLiteStore struct {
	db      *gorm.DB
	cache   *lruCache
	offsets *offsetStore
}

// NewSQLiteStore opens the SQLite database at path and auto-migrates
// tables. WAL mode is enabled so multiple readers can run concurrently
// with a single writer.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("metastore: mkdir %s: %w", dir, err)
	}

	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("metastore: open db: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("metastore: get sql.DB: %w", err)
	}

	// Enable WAL mode for concurrent reads during writes, then bump
	// the connection pool so multiple readers can be open at once.
	// SQLite serialises writes internally.
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA foreign_keys=ON")
	sqlDB.SetMaxOpenConns(4)

	if err := db.AutoMigrate(&TopicRecord{}, &SchemaRecord{}, &ConsumerOffsetRecord{}); err != nil {
		return nil, fmt.Errorf("metastore: migrate: %w", err)
	}

	return &SQLiteStore{
		db:      db,
		cache:   newLRUCache(defaultCacheMaxMB),
		offsets: newOffsetStore(db),
	}, nil
}

// Close releases the underlying database connection pool.
func (s *SQLiteStore) Close() error {
	s.offsets.close()
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// -- cache key helpers --

func topicCacheKey(name string) string { return "t:" + name }

func schemaCacheKey(topic string, ver int) string {
	return "s:" + topic + ":" + fmt.Sprint(ver)
}

const listTopicsKey = "topics"
