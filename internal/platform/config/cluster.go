package config

// ClusterConfig governs the internal node-to-node listener used for
// replication, follower fetch, membership traffic, and Raft bootstrap.
// Peers lists the static Raft voters used for cluster bootstrap, not a
// dynamic membership registry.
type ClusterConfig struct {
	Addr   string        `json:"addr"`
	NodeID string        `json:"node_id"`
	Peers  []ClusterPeer `json:"peers"`
	// InitialMembers lists the node IDs allowed to BOOTSTRAP a brand-new
	// cluster. Empty means every node may bootstrap (single-node and
	// static-cluster deployments). A node with no prior Raft state whose
	// ID is NOT listed starts join-only: it asks the existing leader for
	// admission instead of bootstrapping a phantom cluster — the
	// scale-out path. Never change this list after the cluster exists.
	InitialMembers []string `json:"initial_members"`
}

// ClusterPeer defines a known cluster voter used during Raft bootstrap.
type ClusterPeer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}
