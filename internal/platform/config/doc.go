// Package config defines Narad's runtime configuration: the layered
// load order (defaults → JSON file → environment → CLI flags) and the
// per-section structs each layer hydrates.
//
// File map:
//
//	config.go    Top-level Config struct that aggregates every section.
//	defaults.go  Default() — production-shaped values for every field.
//	load.go      Load(path) — driver that reads the JSON file (if any),
//	             merges defaults, then applies env overlays.
//	env.go       applyEnv — explicit os.LookupEnv per documented variable.
//	validate.go  Config.Validate — bounds checks before serve.go uses
//	             a config.
//	duration.go  Custom Duration type for human-friendly JSON ("30s").
//	cluster.go   ClusterConfig (cluster listen address; replication TBD).
//	http.go      HTTPConfig (listen address, timeouts, max-consume-wait).
//	log.go       LogConfig (level, format).
//	storage.go   StorageConfig (data dir plus internal storage defaults).
//	storage_json.go StorageConfig JSON boundary: only data_dir is user-facing.
//	topic.go     TopicConfig (per-topic creation defaults + bounds).
package config
