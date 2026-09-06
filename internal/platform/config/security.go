package config

// SecurityConfig governs authentication and authorization on the HTTP
// API and the node-to-node cluster port.
//
// Secrets are deliberately not file-configurable (json:"-"): they are
// supplied via environment variables (NARAD_ADMIN_PASSWORD,
// NARAD_CLUSTER_SECRET) so config files and ConfigMaps never hold
// credentials at rest.
type SecurityConfig struct {
	// Enabled turns on Basic authentication and RBAC for the HTTP API.
	// /healthz, /readyz, and /metrics are always served without
	// credentials. TLS is assumed to terminate in front of Narad; the
	// hop from that terminator to Narad carries Basic credentials in
	// cleartext and must be a trusted network path.
	Enabled bool `json:"enabled"`

	// AdminPassword seeds the root "admin" user the first time a
	// cluster boots with no users. If empty, a random password is
	// generated and logged exactly once by the seeding node.
	// Env: NARAD_ADMIN_PASSWORD.
	AdminPassword string `json:"-"`

	// ClusterSecret authenticates node-to-node cluster RPC. Required
	// when security is enabled and cluster peers are configured. Every
	// stream proves it with a MAC bound to the QUIC connection's TLS
	// session, in both directions. Env: NARAD_CLUSTER_SECRET.
	ClusterSecret string `json:"-"`

	// AllowLegacyClusterAuth is the one-release compatibility path for
	// a rolling upgrade from nodes that proved the cluster secret with
	// a fixed (replayable, one-way) token. While true, this node also
	// offers the legacy protocol to peers that speak nothing else and
	// logs every such connection. Set it on every node for the upgrade,
	// then turn it off once all nodes run the session-bound protocol.
	// Env: NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH.
	AllowLegacyClusterAuth bool `json:"allow_legacy_cluster_auth"`

	// ClusterTLSCertFile, ClusterTLSKeyFile, and ClusterTLSCAFile enable
	// mutual TLS on the Raft metadata transport (which replicates user
	// password hashes and grants). All three must be set together; leaving
	// them empty runs the transport as plain TCP, relying on network
	// isolation. These are file paths (typically a mounted secret), not
	// secret values, so they are file-configurable.
	// Env: NARAD_CLUSTER_TLS_CERT_FILE / _KEY_FILE / _CA_FILE.
	ClusterTLSCertFile string `json:"cluster_tls_cert_file"`
	ClusterTLSKeyFile  string `json:"cluster_tls_key_file"`
	ClusterTLSCAFile   string `json:"cluster_tls_ca_file"`
}
