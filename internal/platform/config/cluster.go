package config

// ClusterConfig governs the internal node-to-node listener used for
// replication, follower fetch, and membership traffic.
//
// The wiring pass doesn't actually bind the cluster port; the address
// lives here so operators can pin it now and the worker can pick it up
// unchanged when replication lands.
type ClusterConfig struct {
	Addr string `json:"addr"`
}
