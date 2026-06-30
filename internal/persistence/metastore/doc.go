// Package metastore is the Raft-backed metadata store for multi-node narad.
// It owns topic configs and JSON schemas. Consumer offsets are managed
// separately via per-partition .offsets log files.
//
// Writes go through Raft consensus (Apply). Reads are served directly
// from the local bbolt replica — stale by at most a few milliseconds,
// which is acceptable for routing decisions.
//
// File map:
//
//	doc.go    this file
//	fsm.go    fsmState (bbolt FSM), command codec, Apply/Snapshot/Restore
//	store.go  Store — public API, Raft setup, topic/schema methods
package metastore
