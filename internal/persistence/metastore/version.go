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

// RoutingMembersVersion advances when member data used by routing changes:
// membership, API address, cluster address, or alive/dead status. Heartbeat-only
// LastHeartbeat updates do not change this version.
func (s *Store) RoutingMembersVersion() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	return s.fsm.versions.routingMembersVersion()
}
