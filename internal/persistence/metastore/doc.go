// Package metastore is the SQLite-backed metadata store. It owns
// every persistent broker fact other than the partition log itself:
// topics, JSON-Schema registrations, and committed consumer offsets.
//
// Architecture:
//
//   - SQLite via glebarez/sqlite (pure-Go modernc.org/sqlite under
//     the hood — no CGO). WAL mode is enabled at open time so reads
//     can run concurrently with the single writer.
//   - GORM is the persistence layer (auto-migrate + tag-driven
//     schema). One *SQLiteStore satisfies the entire Metastore
//     interface.
//   - An in-process LRU cache (see cache.go) sits in front of every
//     read; explicit invalidation on write keeps it consistent.
//   - Consumer offsets get their own write path (see offset_store.go):
//     fast in-memory writes with periodic batched flushes to SQLite,
//     so the consume hot path doesn't touch the database.
//
// File map:
//
//	metastore.go    Metastore interface, ListOptions, sentinel errors.
//	sqlite.go       *SQLiteStore — TopicRecord/SchemaRecord/ConsumerOffsetRecord
//	                GORM models, NewSQLiteStore, Close, cache key helpers.
//	cache.go        *lruCache — byte-bounded LRU read cache.
//	topics.go       CreateTopic, UpdateTopic, DeleteTopic, GetTopic,
//	                ListTopics (cached + paginated paths).
//	schemas.go      PutSchema, LatestSchema, GetSchema.
//	offsets.go      GetConsumerOffset, SetConsumerOffset (delegate to
//	                offset_store).
//	offset_store.go *offsetStore — in-memory consumer-offset writer
//	                with batched SQLite flushes.
//	errors.go       ErrNotFound, ErrAlreadyExists.
package metastore
