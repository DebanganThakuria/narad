package config

// ClusterConfig governs the internal node-to-node listener used for
// replication, follower fetch, membership traffic, and Raft bootstrap.
type ClusterConfig struct {
	Addr   string `json:"addr"`
	NodeID string `json:"node_id"`
	// TODO when a new peer joins the cluster do we need to put the peer address here?
	Peers []ClusterPeer `json:"peers"`
}

// ClusterPeer defines a known cluster voter used during Raft bootstrap.
type ClusterPeer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}
