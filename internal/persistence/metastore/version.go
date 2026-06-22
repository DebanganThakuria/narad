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
