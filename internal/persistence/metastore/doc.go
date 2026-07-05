// Package metastore is the Raft-backed metadata store for multi-node
// narad. It owns topic configs, JSON schemas, partition assignments, and
// cluster membership. Consumer offsets are managed separately via
// per-partition .offsets log files.
//
// Writes go through Raft consensus (Apply). Reads are served directly
// from the local bbolt replica — stale by at most a few milliseconds,
// which is acceptable for routing decisions.
package metastore
