package metastore

// This file defines the Raft log command encoding. The wire format is
// frozen: changing the opCode values, JSON tags, or payload shapes would
// break replay of existing Raft logs.

type opCode byte

const (
	opCreateTopic opCode = iota + 1
	opUpdateTopic
	opDeleteTopic
	opPutSchema
	opAssignPartition
	opMemberJoin
	opMemberHeartbeat
	opMemberDead
	opCreateUser
	opUpdateUser
	opDeleteUser
	opSeedRootUser
)

// cmd is the envelope written to the Raft log.
type cmd struct {
	Op   opCode `json:"o"`
	Data []byte `json:"d"`
}

// schemaPayload is the body of an opPutSchema command.
type schemaPayload struct {
	Topic   string `json:"t"`
	Version int    `json:"v"`
	Schema  []byte `json:"s"`
}

// heartbeatPayload is the body of an opMemberHeartbeat command.
// At is a Unix timestamp (seconds) — passed in by the caller so Apply
// stays deterministic.
type heartbeatPayload struct {
	ID string `json:"id"`
	At int64  `json:"at"`
}
