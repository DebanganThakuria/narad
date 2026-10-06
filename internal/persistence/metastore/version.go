package metastore

// MetadataVersion advances after every successful Raft-applied metadata
// mutation. Callers can use it to invalidate local read caches without
// coupling themselves to individual metastore write paths.
func (s *Store) MetadataVersion() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.metadataVersion()
}

// TopicVersion advances when the named topic's metadata is created, updated,
// deleted, or replaced by a snapshot restore.
func (s *Store) TopicVersion(name string) uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.topicVersion(name)
}

// AssignmentVersion advances when the named topic's partition assignment set
// changes, the topic is deleted, or a snapshot restore replaces local state.
func (s *Store) AssignmentVersion(topicName string) uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.assignmentVersion(topicName)
}

// SchemaVersion advances when persisted schemas for the named topic change,
// the topic is deleted, or a snapshot restore replaces local state.
func (s *Store) SchemaVersion(topicName string) uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.schemaVersion(topicName)
}

// UsersVersion advances when any user is created, updated, or deleted,
// or a snapshot restore replaces local state. Auth caches key their
// entries by it.
func (s *Store) UsersVersion() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.usersVersion()
}

// RemotesVersion advances when any remote is created, updated, re-encrypted
// or deleted, or a snapshot restore replaces local state. The credential
// cache rebuilds only when it moves.
func (s *Store) RemotesVersion() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.remotesVersion()
}

// RoutingMembersVersion advances when member data used by routing changes:
// membership, API address, cluster address, or alive/dead status. Heartbeat-only
// LastHeartbeat updates do not change this version.
func (s *Store) RoutingMembersVersion() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.routingMembersVersion()
}

// LatestDomainVersion returns the newest version any of the per-domain
// versions above has taken, for any key: it advances whenever a topic,
// assignment, schema, users, remotes or routing-members version does, including
// after a snapshot restore. Unlike MetadataVersion it holds still across
// applies that change none of them (member heartbeats, which every
// member sends every few seconds, and drain flags). Versions advance
// only after the change is committed to the local replica, so a reader
// that takes this value before reading the replica and sees it again
// later knows none of those domains changed in between; background
// reconcilers use that to skip passes over unchanged metadata.
func (s *Store) LatestDomainVersion() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.latest()
}
